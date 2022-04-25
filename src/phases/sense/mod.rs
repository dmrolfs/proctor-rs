use std::fmt::{self, Debug};

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use pretty_snowflake::{AlphabetCodec, IdPrettifier, MachineNode};

use crate::elements::Telemetry;
use crate::error::SenseError;
use crate::graph::stage::{SourceStage, Stage, WithApi};
use crate::graph::{Outlet, Port, SourceShape};
use crate::{AppData, ProctorResult};

pub mod builder;
pub mod clearinghouse;
pub mod sensor;
pub mod settings;
pub mod subscription_channel;

pub use builder::SenseBuilder;
pub use clearinghouse::{
    Clearinghouse, ClearinghouseApi, ClearinghouseCmd, ClearinghouseSubscriptionAgent, CorrelationGenerator,
    SubscriptionRequirements, TelemetrySubscription, SUBSCRIPTION_CORRELATION, SUBSCRIPTION_TIMESTAMP,
};
pub use sensor::{make_telemetry_cvs_sensor, make_telemetry_rest_api_sensor, TelemetrySensor};
pub use settings::{HttpQuery, SensorSetting};
pub use subscription_channel::SubscriptionChannel;

use crate::phases::sense::clearinghouse::TelemetryCacheSettings;

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

pub struct Sense<Out> {
    name: String,
    inner: Box<dyn SourceStage<Out>>,
    outlet: Outlet<Out>,
    pub tx_clearinghouse_api: ClearinghouseApi,
    // todo: tx_api: CollectApi,
    // todo: tx_monitor: CollectMonitor,
}

impl<Out> Sense<Out> {
    #[tracing::instrument(level = "trace", skip(name, sources))]
    pub fn builder(
        name: impl Into<String>, sources: Vec<Box<dyn SourceStage<Telemetry>>>,
        cache_settings: &TelemetryCacheSettings, machine_node: MachineNode,
    ) -> SenseBuilder<Out> {
        let id_generator = CorrelationGenerator::distributed(machine_node, IdPrettifier::<AlphabetCodec>::default());
        SenseBuilder::new(name, sources, cache_settings, id_generator)
    }

    #[tracing::instrument(level = "trace", skip(name, sources))]
    pub fn single_node_builder(
        name: impl Into<String>, sources: Vec<Box<dyn SourceStage<Telemetry>>>, cache_settings: &TelemetryCacheSettings,
    ) -> SenseBuilder<Out> {
        Self::builder(name, sources, cache_settings, MachineNode::default())
    }
}

impl<Out> Debug for Sense<Out> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Collect")
            .field("name", &self.name)
            .field("inner", &self.inner)
            .field("outlet", &self.outlet)
            .finish()
    }
}

impl<Out> SourceShape for Sense<Out> {
    type Out = Out;

    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

impl<Out> WithApi for Sense<Out> {
    type Sender = ClearinghouseApi;

    fn tx_api(&self) -> Self::Sender {
        self.tx_clearinghouse_api.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<Out: AppData> Stage for Sense<Out> {
    fn name(&self) -> &str {
        &self.name
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", name = "run sense phase", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        self.do_run().await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn close(self: Box<Self>) -> ProctorResult<()> {
        self.do_close().await?;
        Ok(())
    }
}

impl<Out: AppData> Sense<Out> {
    async fn do_check(&self) -> Result<(), SenseError> {
        self.inner.check().await.map_err(|err| SenseError::Stage(err.into()))?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    async fn do_run(&mut self) -> Result<(), SenseError> {
        self.inner.run().await.map_err(|err| SenseError::Stage(err.into()))?;
        Ok(())
    }

    async fn do_close(mut self: Box<Self>) -> Result<(), SenseError> {
        tracing::trace!(stage=%self.name(), "closing sense phase ports.");
        self.inner.close().await.map_err(|err| SenseError::Stage(err.into()))?;
        self.outlet.close().await;
        Ok(())
    }
}
