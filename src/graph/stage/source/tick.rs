use crate::graph::shape::{Shape, SourceShape};
use crate::graph::{stage, GraphResult, Outlet, Port, Stage};
use crate::AppData;
use async_stream::stream;
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use futures_util::stream::StreamExt;
use std::fmt;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::Instrument;

pub type TickApi = mpsc::UnboundedSender<TickMsg>;

#[derive(Debug)]
pub enum TickMsg {
    Stop { tx: oneshot::Sender<GraphResult<()>> },
}

impl TickMsg {
    pub fn stop() -> (Self, oneshot::Receiver<GraphResult<()>>) {
        let (tx, rx) = oneshot::channel();
        (Self::Stop { tx }, rx)
    }
}

pub trait ContinueTicking: Send {
    fn next(&mut self) -> bool;
}

#[derive(fmt::Debug, Clone, Copy, PartialEq)]
pub enum Constraint {
    None,
    ByCount { count: usize, limit: usize },
    ByTime { stop: Option<tokio::time::Instant>, limit: Duration },
}

impl ContinueTicking for Constraint {
    #[inline]
    fn next(&mut self) -> bool {
        match self {
            Constraint::None => true,

            Constraint::ByCount { count, limit } => {
                *count += 1;
                count <= limit
            }

            Constraint::ByTime { stop, limit } => match stop {
                None => {
                    *stop = Some(tokio::time::Instant::now() + *limit);
                    tracing::warn!(?stop, ?limit, "set tick time constraint");
                    true
                }

                Some(stop) => {
                    let now = tokio::time::Instant::now();
                    let do_next = now < *stop;
                    let diff = if do_next { Ok(*stop - now) } else { Err(now - *stop) };
                    tracing::warn!(?diff, %do_next, "eval tick time constraint.");
                    do_next
                }
            },
        }
    }
}

impl Constraint {
    /// By default, Tick has no constraint and will produce ticks ongoing until it is stopped by
    /// either dropping the Tick source or sending it the [TickMsg::Stop] message.
    pub fn none() -> Constraint {
        Constraint::None
    }

    /// Tick can be set to stop after a predefined count of ticks.
    pub fn by_count(limit: usize) -> Constraint {
        Constraint::ByCount { count: 0, limit }
    }

    /// Tick can be set to stop after a predefined duration.
    pub fn by_time(limit: Duration) -> Constraint {
        Constraint::ByTime { stop: None, limit }
    }
}

/// Elements are emitted periodically with the specified interval. The tick element will be
/// sent to downstream consumers via its outlet.
///
/// # Examples
///
/// ```
/// use proctor::graph::stage::tick;
/// use proctor::graph::stage::{self, Stage};
/// use proctor::graph::SourceShape;
/// use std::time::Duration;
/// use tokio::sync::mpsc;
///
/// #[tokio::main]
/// async fn main() {
///     let (tx, mut rx) = mpsc::channel(100);
///     let limit = 5;
///     let mut tick = stage::Tick::with_constraint(
///         "tick17",
///         Duration::from_millis(50),
///         Duration::from_millis(30),
///         17,
///         tick::Constraint::by_count(limit)
///     );
///     tick.outlet().attach(tx).await;
///
///     tokio::spawn(async move {
///         tick.run().await;
///     });
///
///     for _ in (1..=limit) {
///         assert_eq!(Some(17), rx.recv().await);
///     }
///     assert_eq!(None, rx.recv().await);
/// }
/// ```
pub struct Tick<T>
where
    T: AppData + Clone + Unpin,
{
    name: String,
    pub initial_delay: Duration,
    pub interval: Duration,
    tick: T,
    constraint: Constraint,
    outlet: Outlet<T>,
    tx_api: TickApi,
    rx_api: mpsc::UnboundedReceiver<TickMsg>,
}

impl<T> Tick<T>
where
    T: AppData + Clone + Unpin,
{
    pub fn new<S: Into<String>>(name: S, initial_delay: Duration, interval: Duration, tick: T) -> Self {
        Tick::with_constraint(name, initial_delay, interval, tick, Constraint::None)
    }

    pub fn with_constraint<S: Into<String>>(name: S, initial_delay: Duration, interval: Duration, tick: T, constraint: Constraint) -> Self {
        assert!(interval > Duration::new(0, 0), "`interval` must be non-zero.");
        let name = name.into();
        let outlet = Outlet::new(name.clone());
        let (tx_api, rx_api) = mpsc::unbounded_channel();

        Self {
            name,
            initial_delay,
            interval,
            tick,
            constraint,
            outlet,
            tx_api,
            rx_api,
        }
    }
}

impl<T> Shape for Tick<T> where T: AppData + Clone + Unpin {}

impl<T> SourceShape for Tick<T>
where
    T: AppData + Clone + Unpin,
{
    type Out = T;
    fn outlet(&mut self) -> &mut Outlet<Self::Out> {
        &mut self.outlet
    }
}

#[dyn_upcast]
#[async_trait]
impl<T> Stage for Tick<T>
where
    T: AppData + Clone + Unpin + 'static,
{
    fn name(&self) -> &str {
        self.name.as_ref()
    }

    #[tracing::instrument(
        level="info",
        name="run tick source",
        skip(self),
        fields(name=%self.name),
    )]
    async fn run(&mut self) -> GraphResult<()> {
        let start = tokio::time::Instant::now() + self.initial_delay;
        let interval = tokio::time::interval_at(start, self.interval);
        tokio::pin!(interval);
        let guard: Box<dyn ContinueTicking> = Box::new(self.constraint);
        tokio::pin!(guard);
        let tick = &self.tick;
        let tick = || tick.clone();

        let ticks = stream! {
            while guard.next() {
                interval.tick().await;
                yield tick();
            }
        };

        tokio::pin!(ticks);

        let rx_api = &mut self.rx_api;
        let outlet = &self.outlet;

        loop {
            tokio::select! {
                next_tick = ticks.next() => match next_tick {
                    Some(tick) => {
                        tracing::info!(?tick, "tick yielded -- sending...");
                        match outlet.send(tick).instrument(tracing::info_span!("sending tick")).await {
                            Ok(()) => (),
                            Err(err) => {
                                tracing::error!(error=?err, "failed to send tick - completing tick source");
                                break;
                            }
                        }
                    },

                    None => {
                        tracing::info!("ticking stopped -- breaking...");
                        break;
                    }
                },

                Some(msg) = rx_api.recv() => match msg {
                    TickMsg::Stop { tx } => {
                        tracing::info!("handling request to stop ticking.");
                        let _ = tx.send(Ok(()));
                        break;
                    }
                },

                else => {
                    tracing::trace!("done ticking - completing tick source.");
                    break;
                }
            }
        }

        Ok(())
    }

    async fn close(mut self: Box<Self>) -> GraphResult<()> {
        tracing::info!("closing tick source outlet.");
        self.outlet.close().await;
        Ok(())
    }
}

impl<T> stage::WithApi for Tick<T>
where
    T: AppData + Clone + Unpin,
{
    type Sender = TickApi;

    fn tx_api(&self) -> Self::Sender {
        self.tx_api.clone()
    }
}

impl<T> fmt::Debug for Tick<T>
where
    T: AppData + Clone + Unpin,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tick")
            .field("name", &self.name)
            .field("initial_delay", &self.initial_delay)
            .field("interval", &self.interval)
            .field("outlet", &self.outlet)
            .finish()
    }
}

// /////////////////////////////////////////////////////
// // Unit Tests ///////////////////////////////////////
//
#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::stage::WithApi;
    use std::time::Duration;
    use tokio::sync::{mpsc, oneshot};
    use tokio_test::block_on;

    #[test]
    fn test_stop_api() {
        let (tx, mut rx) = mpsc::channel(100);
        let limit = 5;
        let mut tick = Tick::with_constraint(
            "tick17",
            Duration::from_millis(50),
            Duration::from_millis(30),
            17,
            Constraint::by_count(limit),
        );
        let tx_api = tick.tx_api();

        block_on(async {
            tick.outlet().attach(tx).await;

            tokio::spawn(async move {
                tick.run().await.expect("failed to run tick source");
            });

            tokio::time::sleep(Duration::from_millis(95)).await;
            let (stop_tx, stop_rx) = oneshot::channel();
            match tx_api.send(TickMsg::Stop { tx: stop_tx }) {
                Ok(_) => tracing::info!("Stop sent to Tick source."),
                Err(err) => panic!("failed to send Stop: {:?}", err),
            };

            match stop_rx.await.map_err(|err| err.into()).and_then(|res| res) {
                Ok(_) => tracing::info!("Stop Ack received."),
                Err(err) => panic!("Stop failed: {:?}", err),
            };

            for _ in 1..=2 {
                assert_eq!(Some(17), rx.recv().await);
            }
            assert_eq!(None, rx.recv().await);
        });
    }
}
