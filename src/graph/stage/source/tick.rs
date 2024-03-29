use std::fmt::{self, Debug};
use std::time::Duration;

use async_stream::stream;
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use futures_util::stream::StreamExt;
use tokio::sync::{mpsc, oneshot};

use crate::error::StageError;
use crate::graph::shape::SourceShape;
use crate::graph::{stage, Outlet, Port, Stage, PORT_DATA};
use crate::{Ack, AppData, ProctorResult};

pub type TickApi = mpsc::UnboundedSender<TickCmd>;

#[derive(Debug)]
pub enum TickCmd {
    Stop {
        tx: oneshot::Sender<Result<(), StageError>>,
    },
}

impl TickCmd {
    const STAGE_NAME: &'static str = "tick_source";

    pub async fn stop(api: &TickApi) -> Result<Ack, StageError> {
        let (tx, rx) = oneshot::channel();
        api.send(Self::Stop { tx })
            .map_err(|err| StageError::Api(Self::STAGE_NAME.to_string(), err.into()))?;
        rx.await
            .map_err(|err| StageError::Api(Self::STAGE_NAME.to_string(), err.into()))?
    }
}

pub trait ContinueTicking: Send {
    fn next(&mut self) -> bool;
}

#[derive(fmt::Debug, Clone, Copy, PartialEq, Eq)]
pub enum Constraint {
    None,
    ByCount {
        count: usize,
        limit: usize,
    },
    ByTime {
        stop: Option<tokio::time::Instant>,
        limit: Duration,
    },
}

impl ContinueTicking for Constraint {
    fn next(&mut self) -> bool {
        match self {
            Self::None => true,

            Self::ByCount { count, limit } => {
                *count += 1;
                count <= limit
            },

            Self::ByTime { stop, limit } => match stop {
                None => {
                    *stop = Some(tokio::time::Instant::now() + *limit);
                    tracing::debug!(?stop, ?limit, "set tick time constraint");
                    true
                },

                Some(stop) => {
                    let now = tokio::time::Instant::now();
                    let do_next = now < *stop;
                    let diff = if do_next { Ok(*stop - now) } else { Err(now - *stop) };
                    tracing::debug!(?diff, %do_next, "eval tick time constraint.");
                    do_next
                },
            },
        }
    }
}

impl Constraint {
    /// By default, Tick has no constraint and will produce ticks ongoing until it is stopped by
    /// either dropping the Tick source or sending it the [TickMsg::Stop] message.
    #[inline]
    pub const fn none() -> Self {
        Self::None
    }

    /// Tick can be set to stop after a predefined count of ticks.
    #[inline]
    pub const fn by_count(limit: usize) -> Self {
        Self::ByCount { count: 0, limit }
    }

    /// Tick can be set to stop after a predefined duration.
    #[inline]
    pub const fn by_time(limit: Duration) -> Self {
        Self::ByTime { stop: None, limit }
    }
}

/// Elements are emitted periodically with the specified interval. The tick element will be
/// sent to downstream consumers via its outlet.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use proctor::graph::stage::tick;
/// use proctor::graph::stage::{self, Stage};
/// use proctor::graph::SourceShape;
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
///         tick::Constraint::by_count(limit),
///     );
///     tick.outlet().attach("test_channel".into(), tx).await;
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
pub struct Tick<T> {
    name: String,
    pub initial_delay: Duration,
    pub interval: Duration,
    tick: T,
    constraint: Constraint,
    outlet: Outlet<T>,
    tx_api: TickApi,
    rx_api: mpsc::UnboundedReceiver<TickCmd>,
}

impl<T> Tick<T> {
    pub fn new<S: Into<String>>(name: S, initial_delay: Duration, interval: Duration, tick: T) -> Self {
        Self::with_constraint(name, initial_delay, interval, tick, Constraint::None)
    }

    pub fn with_constraint<S: Into<String>>(
        name: S, initial_delay: Duration, interval: Duration, tick: T, constraint: Constraint,
    ) -> Self {
        assert!(interval > Duration::new(0, 0), "`interval` must be non-zero.");
        let name = name.into();
        let outlet = Outlet::new(name.clone(), PORT_DATA);
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

impl<T> SourceShape for Tick<T> {
    type Out = T;

    #[inline]
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<T> Stage for Tick<T>
where
    T: AppData + Clone + Unpin + Sync,
{
    #[inline]
    fn name(&self) -> &str {
        &self.name
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.outlet.check_attachment().await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", name = "run tick source", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
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
            let _timer = stage::start_stage_eval_time(&self.name);

            tokio::select! {
                next_tick = ticks.next() => match next_tick {
                    Some(tick) => {
                        tracing::trace!(?tick, "sending tick...");
                        outlet.send(tick).await?;
                    },

                    None => {
                        tracing::info!(name=%self.name(), "ticking stopped -- breaking...");
                        break;
                    }
                },

                Some(msg) = rx_api.recv() => match msg {
                    TickCmd::Stop { tx } => {
                        tracing::info!(name=%self.name(), "handling request to stop ticking.");
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

    #[tracing::instrument(level = "trace", skip(self))]
    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        tracing::info!(name=%self.name(), "closing tick source outlet.");
        self.outlet.close().await;
        Ok(())
    }
}

impl<T> stage::WithApi for Tick<T> {
    type Sender = TickApi;

    #[inline]
    fn tx_api(&self) -> Self::Sender {
        self.tx_api.clone()
    }
}

impl<T> Debug for Tick<T> {
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
    use std::time::Duration;

    use tokio::sync::{mpsc, oneshot};
    use tokio_test::block_on;

    use super::*;
    use crate::graph::stage::WithApi;

    #[test]
    fn test_stop_api() -> anyhow::Result<()> {
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
            tick.outlet().attach("test_tx".into(), tx).await;

            tokio::spawn(async move {
                tick.run().await.expect("failed to run tick source");
            });

            tokio::time::sleep(Duration::from_millis(100)).await;
            let (stop_tx, stop_rx) = oneshot::channel();
            let foo: anyhow::Result<()> = tx_api.send(TickCmd::Stop { tx: stop_tx }).map_err(|err| err.into());
            let _ = foo?;
            tracing::info!("Stop sent to Tick source.");

            stop_rx.await??;
            tracing::info!("Stop Ack received.");

            for _ in 1..=2 {
                assert_eq!(Some(17), rx.recv().await);
            }
            assert_eq!(None, rx.recv().await);

            Ok(())
        })
    }
}
