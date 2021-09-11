use crate::elements::Telemetry;
use crate::error::CollectionError;
use crate::graph::stage::{self, SourceStage, Stage, WithApi};
use crate::graph::{Connect, Graph, Outlet, Port, SinkShape, SourceShape, UniformFanInShape};
use crate::{AppData, ProctorResult};
use async_trait::async_trait;
use cast_trait_object::{dyn_upcast, DynCastExt};
use serde::de::DeserializeOwned;
use std::collections::HashSet;
use std::fmt::{self, Debug};

pub use clearinghouse::*;
pub use settings::*;
pub use source::*;
pub use subscription_channel::*;

pub mod clearinghouse;
pub mod settings;
pub mod source;
pub mod subscription_channel;

//todo: implement
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
    name: String,
    inner: Box<dyn SourceStage<Out>>,
    outlet: Outlet<Out>,
    pub tx_clearinghouse_api: ClearinghouseApi,
    //todo: tx_api: CollectApi,
    //todo: tx_monitor: CollectMonitor,
}

impl<Out: AppData + SubscriptionRequirements + DeserializeOwned> Collect<Out> {
    #[tracing::instrument(level="info", skip(name, sources), fields(nr_sources = %sources.len()))]
    pub async fn new(
        name: impl Into<String>, sources: Vec<Box<dyn SourceStage<Telemetry>>>,
    ) -> Result<Self, CollectionError> {
        Self::for_requirements(
            name,
            sources,
            <Out as SubscriptionRequirements>::required_fields(),
            <Out as SubscriptionRequirements>::optional_fields(),
        )
        .await
    }
}

impl<Out: AppData + DeserializeOwned> Collect<Out> {
    #[tracing::instrument(
        level="info",
        skip(name, sources, required_fields, optional_fields),
        fields(nr_sources = %sources.len())
    )]
    pub async fn for_requirements(
        name: impl Into<String>, sources: Vec<Box<dyn SourceStage<Telemetry>>>,
        required_fields: HashSet<impl Into<String>>, optional_fields: HashSet<impl Into<String>>,
    ) -> Result<Self, CollectionError> {
        let name = name.into();
        let num_sources = sources.len();
        let merge = stage::MergeN::new(format!("{}_source_merge_{}", name, num_sources), num_sources);
        let mut clearinghouse = Clearinghouse::new(format!("{}_clearinghouse", name));
        let tx_clearinghouse_api = clearinghouse.tx_api();
        (merge.outlet(), clearinghouse.inlet()).connect().await;

        let out_channel: SubscriptionChannel<Out> = SubscriptionChannel::connect_channel_with_requirements(
            name.as_str(),
            (&mut clearinghouse).into(),
            required_fields,
            optional_fields,
        )
        .await?;

        clearinghouse
            .check()
            .await
            .map_err(|err| crate::error::PortError::ChannelError(err.into()))?;
        out_channel.subscription_receiver.check_attachment().await?;
        tracing::info!(?clearinghouse, channel_name=%name, "connected subscription channel");

        let outlet = out_channel.outlet();

        let mut g = Graph::default();
        let merge_inlets = merge.inlets();
        let num_merge_inlets = merge_inlets.len().await;
        if num_merge_inlets != num_sources {
            return Err(CollectionError::PortError(crate::error::PortError::Detached(format!(
                "merge inlets({}) does not match number of sources({})",
                num_merge_inlets, num_sources
            ))));
        }

        for (idx, s) in sources.into_iter().enumerate() {
            match merge_inlets.get(idx).await {
                Some(merge_inlet) => {
                    tracing::info!(source=%s.name(), ?merge_inlet, "connecting collection source to clearinghouse.");
                    (s.outlet(), merge_inlet).connect().await;
                    g.push_back(s.dyn_upcast()).await;
                }

                None => {
                    tracing::warn!(source=%s.name(), "no available clearinghouse port for source - skipping source");
                }
            }
        }
        g.push_back(Box::new(merge)).await;
        g.push_back(Box::new(clearinghouse)).await;
        g.push_back(Box::new(out_channel)).await;
        let composite = stage::CompositeSource::new(format!("{}_composite_source", name), g, outlet).await;

        let inner: Box<dyn SourceStage<Out>> = Box::new(composite);
        let outlet = inner.outlet();
        Ok(Self { name, inner, outlet, tx_clearinghouse_api })
    }
}

impl Collect<Telemetry> {
    #[tracing::instrument(
    level="info",
    skip(name, sources, required_fields, optional_fields),
    fields(nr_sources=%sources.len(),)
    )]
    pub async fn telemetry(
        name: impl Into<String>, sources: Vec<Box<dyn SourceStage<Telemetry>>>,
        required_fields: HashSet<impl Into<String>>, optional_fields: HashSet<impl Into<String>>,
    ) -> Result<Self, CollectionError> {
        let name = name.into();
        let num_sources = sources.len();
        let merge = stage::MergeN::new(format!("{}_{}_source_merge", name, num_sources), num_sources);
        let mut clearinghouse = Clearinghouse::new(format!("{}_clearinghouse", name));
        let tx_clearinghouse_api = clearinghouse.tx_api();

        let out_channel = SubscriptionChannel::connect_telemetry_channel(
            name.as_str(),
            (&mut clearinghouse).into(),
            required_fields,
            optional_fields,
        )
        .await?;
        let outlet = out_channel.outlet();

        let mut g = Graph::default();
        let merge_inlets = merge.inlets();
        for (idx, s) in sources.into_iter().enumerate() {
            match merge_inlets.get(idx).await {
                Some(merge_inlet) => {
                    tracing::info!(source=%s.name(), "connecting collection source to clearinghouse.");
                    (s.outlet(), merge_inlet).connect().await;
                    g.push_back(s.dyn_upcast()).await;
                }

                None => {
                    tracing::warn!(source=%s.name(), "no available clearinghouse port for source - skipping");
                }
            }
        }

        // sources pushed into graph above with merge connection
        g.push_back(Box::new(merge)).await;
        g.push_back(Box::new(clearinghouse)).await;
        g.push_back(Box::new(out_channel)).await;
        let composite = stage::CompositeSource::new(format!("{}_composite_source", name), g, outlet).await;

        let inner: Box<dyn SourceStage<Telemetry>> = Box::new(composite);
        let outlet = inner.outlet();
        Ok(Self { name, inner, outlet, tx_clearinghouse_api })
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

#[dyn_upcast]
#[async_trait]
impl<Out: AppData> Stage for Collect<Out> {
    fn name(&self) -> &str {
        self.name.as_str()
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
