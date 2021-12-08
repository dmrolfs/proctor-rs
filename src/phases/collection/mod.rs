use std::fmt::{self, Debug};

use async_trait::async_trait;
pub use builder::*;
use cast_trait_object::dyn_upcast;
pub use clearinghouse::*;
use pretty_snowflake::{AlphabetCodec, IdPrettifier, MachineNode};
pub use settings::*;
pub use source::*;
pub use subscription_channel::*;

use crate::elements::Telemetry;
use crate::error::CollectionError;
use crate::graph::stage::{SourceStage, Stage, WithApi};
use crate::graph::{Outlet, Port, SourceShape};
use crate::{AppData, ProctorResult, SharedString};

pub mod builder;
pub mod clearinghouse;
pub mod settings;
pub mod source;
pub mod subscription_channel;

// todo: implement
// pub type CollectApi = mpsc::UnboundedSender<CollectCmd>;
// pub type CollectApiReceiver = mpsc::UnboundedReceiver<CollectCmd>;
// pub type CollectMonitor = broadcast::Receiver<CollectEvent>;
//
// #[derive(Debug)]
// pub enum CollectCmd {
//     Stop { source_pos: usize, tx: oneshot::Sender<Ack> },
// }
//
// impl CollectCmd {
//     pub fn stop(source_pos: usize) -> (CollectCmd, oneshot::Receiver<Ack>) {
//         let (tx, rx) = oneshot::channel();
//         (Self::Stop { source_pos, tx }, rx)
//     }
// }
//
// #[derive(Debug, Clone, PartialEq)]
// pub enum CollectEvent {
//     DataCollected { from_source_pos: usize, },
//     DataPublished,
// }

pub struct Collect<Out> {
    name: SharedString,
    inner: Box<dyn SourceStage<Out>>,
    outlet: Outlet<Out>,
    pub tx_clearinghouse_api: ClearinghouseApi,
    /* todo: tx_api: CollectApi,
     * todo: tx_monitor: CollectMonitor, */
}

impl<Out> Collect<Out> {
    #[tracing::instrument(level = "info", skip(name, sources))]
    pub fn builder(
        name: impl Into<SharedString>, sources: Vec<Box<dyn SourceStage<Telemetry>>>, machine_node: MachineNode,
    ) -> CollectBuilder<Out> {
        let id_generator = CorrelationGenerator::distributed(machine_node, IdPrettifier::<AlphabetCodec>::default());
        CollectBuilder::new(name, sources, id_generator)
    }

    #[tracing::instrument(level = "info", skip(name, sources))]
    pub fn single_node_builder(
        name: impl Into<SharedString>, sources: Vec<Box<dyn SourceStage<Telemetry>>>,
    ) -> CollectBuilder<Out> {
        Self::builder(name, sources, MachineNode::default())
    }
}

impl<Out> Debug for Collect<Out> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Collect")
            .field("name", &self.name)
            .field("inner", &self.inner)
            .field("outlet", &self.outlet)
            .finish()
    }
}

impl<Out> SourceShape for Collect<Out> {
    type Out = Out;

    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

impl<Out> WithApi for Collect<Out> {
    type Sender = ClearinghouseApi;

    fn tx_api(&self) -> Self::Sender {
        self.tx_clearinghouse_api.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<Out: AppData> Stage for Collect<Out> {
    fn name(&self) -> SharedString {
        self.name.clone()
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run collect phase", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        self.do_run().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn close(self: Box<Self>) -> ProctorResult<()> {
        self.do_close().await?;
        Ok(())
    }
}

impl<Out: AppData> Collect<Out> {
    async fn do_check(&self) -> Result<(), CollectionError> {
        self.inner
            .check()
            .await
            .map_err(|err| CollectionError::StageError(err.into()))?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    async fn do_run(&mut self) -> Result<(), CollectionError> {
        self.inner
            .run()
            .await
            .map_err(|err| CollectionError::StageError(err.into()))?;
        Ok(())
    }

    async fn do_close(mut self: Box<Self>) -> Result<(), CollectionError> {
        tracing::trace!("closing collect phase ports.");
        self.inner
            .close()
            .await
            .map_err(|err| CollectionError::StageError(err.into()))?;
        self.outlet.close().await;
        Ok(())
    }
}
