use crate::graph::stage::{self, Stage};
use crate::graph::{GraphResult, Outlet, Port, SourceShape};
use crate::{Ack, AppData};
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use std::fmt::{self, Debug};
use tokio::sync::{mpsc, oneshot};

pub type ActorSourceApi<T> = mpsc::UnboundedSender<ActorSourceCmd<T>>;

#[derive(Debug)]
pub enum ActorSourceCmd<T> {
    Push { item: T, tx: oneshot::Sender<Ack> },
    Stop(oneshot::Sender<Ack>),
}

impl<T> ActorSourceCmd<T> {
    #[inline]
    pub fn push(item: T) -> (Self, oneshot::Receiver<Ack>) {
        let (tx, rx) = oneshot::channel();
        (Self::Push { item, tx }, rx)
    }

    #[inline]
    pub fn stop() -> (Self, oneshot::Receiver<Ack>) {
        let (tx, rx) = oneshot::channel();
        (Self::Stop(tx), rx)
    }
}

/// Actor-based protocol to source items into a graph flow.
///
pub struct ActorSource<T> {
    name: String,
    outlet: Outlet<T>,
    tx_api: ActorSourceApi<T>,
    rx_api: mpsc::UnboundedReceiver<ActorSourceCmd<T>>,
}

impl<T> ActorSource<T> {
    pub fn new<S: Into<String>>(name: S) -> Self {
        let name = name.into();
        let outlet = Outlet::new(name.clone());
        let (tx_api, rx_api) = mpsc::unbounded_channel();
        Self {
            name,
            outlet,
            tx_api,
            rx_api,
        }
    }
}

impl<T> SourceShape for ActorSource<T> {
    type Out = T;
    #[inline]
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<T: AppData> Stage for ActorSource<T> {
    #[inline]
    fn name(&self) -> &str {
        self.name.as_ref()
    }

    #[tracing::instrument(level = "info", name = "run actor source", skip(self))]
    async fn run(&mut self) -> GraphResult<()> {
        while let Some(command) = self.rx_api.recv().await {
            tracing::info!(?command, "handling command");
            match command {
                ActorSourceCmd::Push { item, tx } => {
                    let send_span = tracing::info_span!("sending item", ?item);
                    let _ = send_span.enter();
                    match self.outlet().send(item).await {
                        Ok(()) => {
                            let _ignore_failure = tx.send(());
                            ()
                        }

                        Err(err) => {
                            tracing::error!(error=?err, "failed to send item - completing actor source.");
                            break;
                        }
                    }
                }

                ActorSourceCmd::Stop(tx) => {
                    tracing::info!("stopping actor source.");
                    let _ignore_failure = tx.send(());
                    break;
                }
            }
        }

        Ok(())
    }

    async fn close(mut self: Box<Self>) -> GraphResult<()> {
        tracing::info!("closing actor source outlet.");
        self.outlet.close().await;
        Ok(())
    }
}

impl<T> stage::WithApi for ActorSource<T> {
    type Sender = ActorSourceApi<T>;
    #[inline]
    fn tx_api(&self) -> Self::Sender {
        self.tx_api.clone()
    }
}

impl<T> Debug for ActorSource<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorSource")
            .field("name", &self.name)
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
    use tokio::sync::mpsc;
    use tokio_test::block_on;

    #[test]
    fn test_push_stop_api() {
        lazy_static::initialize(&crate::telemetry::TEST_TRACING);
        let main_span = tracing::info_span!("test_push_stop_api");
        let _ = main_span.enter();

        let (tx, mut rx) = mpsc::channel(8);
        let mut src = ActorSource::new("test_source");
        let tx_api = src.tx_api();

        block_on(async move {
            src.outlet().attach(tx).await;

            tokio::spawn(async move {
                src.run().await.expect("failed to run actor source");
            });

            let (cmd, ack) = ActorSourceCmd::push(13_i32);
            tx_api.send(cmd).expect("failed to send cmd");
            ack.await.expect("command rejected");
            let actual = rx.recv().await;
            assert_eq!(actual, Some(13_i32));

            let (cmd, ack) = ActorSourceCmd::push(17_i32);
            tx_api.send(cmd).expect("failed to send cmd");
            ack.await.expect("command rejected");
            let actual = rx.recv().await;
            assert_eq!(actual, Some(17_i32));

            let (cmd, ack) = ActorSourceCmd::stop();
            tx_api.send(cmd).expect("failed to send cmd");
            ack.await.expect("command rejected");
            let actual = rx.recv().await;
            assert_eq!(actual, None);
        })
    }

    #[test]
    fn test_stop_push_api() {
        lazy_static::initialize(&crate::telemetry::TEST_TRACING);
        let main_span = tracing::info_span!("test_stop_push_api");
        let _ = main_span.enter();

        let (tx, mut rx) = mpsc::channel(8);
        let mut src = ActorSource::new("test_source");
        let tx_api = src.tx_api();

        block_on(async move {
            src.outlet().attach(tx).await;

            tokio::spawn(async move {
                src.run().await.expect("failed to run actor source");
            });

            let (cmd, ack) = ActorSourceCmd::stop();
            tx_api.send(cmd).expect("failed to send cmd");
            ack.await.expect("command rejected");
            let actual = rx.recv().await;
            assert_eq!(actual, None);

            let (cmd, _ack) = ActorSourceCmd::push(13_i32);
            let actual = tx_api.send(cmd);
            assert!(actual.is_err());
        })
    }
}
