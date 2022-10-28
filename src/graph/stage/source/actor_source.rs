use std::fmt::{self, Debug};

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use tokio::sync::{mpsc, oneshot};
use tracing::Instrument;

use crate::error::StageError;
use crate::graph::stage::{self, Stage};
use crate::graph::{Outlet, Port, SourceShape, PORT_DATA};
use crate::{Ack, AppData, ProctorResult};

pub type ActorSourceApi<T> = mpsc::UnboundedSender<ActorSourceCmd<T>>;

#[derive(Debug)]
pub enum ActorSourceCmd<T> {
    Push { item: T, tx: oneshot::Sender<Ack> },
    Stop(oneshot::Sender<Ack>),
}

const STAGE_NAME: &str = "actor_source";
impl<T> ActorSourceCmd<T>
where
    T: Debug + Send + Sync + 'static,
{
    pub async fn push(api: &ActorSourceApi<T>, item: T) -> Result<Ack, StageError> {
        let (tx, rx) = oneshot::channel();
        api.send(Self::Push { item, tx })
            .map_err(|err| StageError::Api(STAGE_NAME.to_string(), err.into()))?;

        rx.await.map_err(|err| StageError::Api(STAGE_NAME.to_string(), err.into()))
    }

    #[inline]
    pub async fn stop(api: &ActorSourceApi<T>) -> Result<Ack, StageError> {
        let (tx, rx) = oneshot::channel();
        api.send(Self::Stop(tx))
            .map_err(|err| StageError::Api(STAGE_NAME.to_string(), err.into()))?;

        rx.await.map_err(|err| StageError::Api(STAGE_NAME.to_string(), err.into()))
    }
}

/// Actor-based protocol to source items into a graph flow.
pub struct ActorSource<T> {
    name: String,
    outlet: Outlet<T>,
    tx_api: ActorSourceApi<T>,
    rx_api: mpsc::UnboundedReceiver<ActorSourceCmd<T>>,
}

impl<T> ActorSource<T> {
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        let outlet = Outlet::new(name.clone(), PORT_DATA);
        let (tx_api, rx_api) = mpsc::unbounded_channel();
        Self { name, outlet, tx_api, rx_api }
    }
}

impl<T> SourceShape for ActorSource<T> {
    type Out = T;

    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<T: AppData> Stage for ActorSource<T> {
    fn name(&self) -> &str {
        &self.name
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.outlet.check_attachment().await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", name = "run actor source", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        while let Some(command) = self.rx_api.recv().await {
            let _timer = stage::start_stage_eval_time(&self.name);

            tracing::debug!(?command, "handling command");
            match command {
                ActorSourceCmd::Push { item, tx } => {
                    let span = tracing::trace_span!("actor sourcing item", ?item);
                    self.outlet().send(item).instrument(span).await?;
                    let _ignore_failure = tx.send(());
                },

                ActorSourceCmd::Stop(tx) => {
                    tracing::info!(stage=%self.name(), "stopping actor source.");
                    let _ignore_failure = tx.send(());
                    break;
                },
            }
        }

        Ok(())
    }

    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        tracing::info!(stage=%self.name(), "closing actor source outlet.");
        self.outlet.close().await;
        Ok(())
    }
}

impl<T> stage::WithApi for ActorSource<T> {
    type Sender = ActorSourceApi<T>;

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
    use claim::*;
    use pretty_assertions::assert_eq;
    use tokio::sync::mpsc;
    use tokio_test::block_on;

    use super::*;
    use crate::graph::stage::WithApi;

    #[test]
    fn test_push_stop_api() {
        once_cell::sync::Lazy::force(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_push_stop_api");
        let _ = main_span.enter();

        let (tx, mut rx) = mpsc::channel(8);
        let mut src = ActorSource::new("test_source");
        let tx_api = src.tx_api();

        block_on(async move {
            src.outlet().attach("test_tx".into(), tx).await;

            tokio::spawn(async move {
                src.run().await.expect("failed to run actor source");
            });

            assert_ok!(ActorSourceCmd::push(&tx_api, 13).await);
            let actual = rx.recv().await;
            assert_eq!(actual, Some(13_i32));

            assert_ok!(ActorSourceCmd::push(&tx_api, 17).await);
            let actual = rx.recv().await;
            assert_eq!(actual, Some(17_i32));

            assert_ok!(ActorSourceCmd::stop(&tx_api).await);
            let actual = rx.recv().await;
            assert_eq!(actual, None);
        })
    }

    #[test]
    fn test_stop_push_api() {
        once_cell::sync::Lazy::force(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_stop_push_api");
        let _ = main_span.enter();

        let (tx, mut rx) = mpsc::channel(8);
        let mut src = ActorSource::new("test_source");
        let tx_api = src.tx_api();

        block_on(async move {
            src.outlet().attach("test_tx".into(), tx).await;

            tokio::spawn(async move {
                src.run().await.expect("failed to run actor source");
            });

            assert_ok!(ActorSourceCmd::stop(&tx_api).await);
            let actual = rx.recv().await;
            assert_eq!(actual, None);

            assert_err!(ActorSourceCmd::push(&tx_api, 13).await);
        })
    }

    #[ignore]
    #[test]
    fn test_push_w_fail() {
        todo!()
    }
}
