use super::{Clearinghouse, Collect, SubscriptionChannel, SubscriptionRequirements};
use crate::elements::Telemetry;
use crate::error::{CollectionError, PortError};
use crate::graph::stage::{self, SourceStage, Stage, WithApi};
use crate::graph::{Connect, Graph, SinkShape, SourceShape, UniformFanInShape};
use crate::phases::collection::{CorrelationGenerator, TelemetrySubscription};
use crate::{AppData, SharedString};
use cast_trait_object::DynCastExt;
use serde::de::DeserializeOwned;
use std::collections::HashSet;
use std::fmt::Debug;
use std::marker::PhantomData;
use pretty_snowflake::Labeling;

#[derive(Debug)]
pub struct CollectBuilder<Out, L: Labeling> {
    name: String,
    sources: Vec<Box<dyn SourceStage<Telemetry>>>,
    merge: stage::MergeN<Telemetry>,
    pub clearinghouse: Clearinghouse<L>,
    marker: PhantomData<Out>,
}

impl<Out, L> CollectBuilder<Out, L>
where
    L: Labeling + Debug,
{
    #[tracing::instrument(level = "info", skip(name, sources))]
    pub fn new(
        name: impl Into<String>, sources: Vec<Box<dyn SourceStage<Telemetry>>>,
        correlation_generator: CorrelationGenerator<L>
    ) -> Self {
        let name = name.into();
        let nr_sources = sources.len();
        let merge = stage::MergeN::new(format!("{}_source_merge_{}", name, nr_sources), nr_sources);
        let clearinghouse = Clearinghouse::new(format!("{}_clearinghouse", name), correlation_generator);

        Self { name, sources, merge, clearinghouse, marker: PhantomData }
    }
}

impl<Out, L> CollectBuilder<Out, L>
where
    Out: AppData + SubscriptionRequirements + DeserializeOwned,
    L: Labeling + Debug + Send + Sync + 'static,
{
    #[tracing::instrument(level = "info", skip(), fields(nr_sources = % self.sources.len()))]
    pub async fn build_for_out(self) -> Result<Collect<Out>, CollectionError> {
        self.build_for_out_requirements(
            <Out as SubscriptionRequirements>::required_fields(),
            <Out as SubscriptionRequirements>::optional_fields(),
        )
        .await
    }

    #[tracing::instrument(level = "info", skip(update_metrics), fields(nr_sources = % self.sources.len()))]
    pub async fn build_for_out_w_metrics(
        self, update_metrics: Box<dyn Fn(&str, &Telemetry) -> () + Send + Sync + 'static>,
    ) -> Result<Collect<Out>, CollectionError> {
        self.build_for_out_requirements_w_metrics(
            <Out as SubscriptionRequirements>::required_fields(),
            <Out as SubscriptionRequirements>::optional_fields(),
            update_metrics,
        )
        .await
    }
}

impl<L> CollectBuilder<Telemetry, L>
where
    L: Labeling + Debug + Send + Sync + 'static,
{
    #[tracing::instrument(level="info", fields(nr_sources = %self.sources.len()))]
    pub async fn build_for_telemetry_out_subscription(
        mut self, out_subscription: TelemetrySubscription,
    ) -> Result<Collect<Telemetry>, CollectionError> {
        let out_channel =
            SubscriptionChannel::connect_telemetry_subscription(out_subscription, (&mut self).into()).await?;
        self.finish(out_channel).await
    }

    #[tracing::instrument(
        level = "info",
        skip(out_required_fields, out_optional_fields, ),
        fields(nr_sources = %self.sources.len())
    )]
    pub async fn build_for_telemetry_out(
        mut self, out_required_fields: HashSet<impl Into<SharedString>>,
        out_optional_fields: HashSet<impl Into<SharedString>>,
    ) -> Result<Collect<Telemetry>, CollectionError> {
        let subscription = TelemetrySubscription::new(self.name.as_str())
            .with_required_fields(out_required_fields)
            .with_optional_fields(out_optional_fields);

        let out_channel = SubscriptionChannel::connect_telemetry_subscription(subscription, (&mut self).into()).await?;

        self.finish(out_channel).await
    }

    #[tracing::instrument(
        level = "info",
        skip(out_required_fields, out_optional_fields, update_metrics),
        fields(nr_sources = %self.sources.len())
    )]
    pub async fn build_for_telemetry_out_w_metrics(
        mut self, out_required_fields: HashSet<impl Into<SharedString>>,
        out_optional_fields: HashSet<impl Into<SharedString>>,
        update_metrics: Box<dyn Fn(&str, &Telemetry) -> () + Send + Sync + 'static>,
    ) -> Result<Collect<Telemetry>, CollectionError> {
        let subscription = TelemetrySubscription::new(self.name.as_str())
            .with_required_fields(out_required_fields)
            .with_optional_fields(out_optional_fields)
            .with_update_metrics_fn(update_metrics);

        let out_channel = SubscriptionChannel::connect_telemetry_subscription(subscription, (&mut self).into()).await?;

        self.finish(out_channel).await
    }
}

impl<Out, L> CollectBuilder<Out, L>
where
    Out: AppData + DeserializeOwned,
    L: Labeling + Debug + Send + Sync + 'static,
{
    #[tracing::instrument(level="info", fields(nr_sources=%self.sources.len()))]
    pub async fn build_for_out_subscription(
        mut self, out_subscription: TelemetrySubscription,
    ) -> Result<Collect<Out>, CollectionError> {
        let out_channel = SubscriptionChannel::connect_subscription(out_subscription, (&mut self).into()).await?;
        self.finish(out_channel).await
    }

    #[tracing::instrument(
        level = "info",
        skip(out_required_fields, out_optional_fields),
        fields(nr_sources = % self.sources.len())
    )]
    pub async fn build_for_out_requirements(
        mut self, out_required_fields: HashSet<impl Into<SharedString>>,
        out_optional_fields: HashSet<impl Into<SharedString>>,
    ) -> Result<Collect<Out>, CollectionError> {
        let subscription = TelemetrySubscription::new(self.name.as_str())
            .with_required_fields(out_required_fields)
            .with_optional_fields(out_optional_fields);

        let out_channel: SubscriptionChannel<Out> =
            SubscriptionChannel::connect_subscription(subscription, (&mut self).into()).await?;

        self.finish(out_channel).await
    }

    #[tracing::instrument(
        level = "info",
        skip(out_required_fields, out_optional_fields, update_metrics),
        fields(nr_sources = % self.sources.len())
    )]
    pub async fn build_for_out_requirements_w_metrics(
        mut self, out_required_fields: HashSet<impl Into<SharedString>>,
        out_optional_fields: HashSet<impl Into<SharedString>>,
        update_metrics: Box<dyn Fn(&str, &Telemetry) -> () + Send + Sync + 'static>,
    ) -> Result<Collect<Out>, CollectionError> {
        let subscription = TelemetrySubscription::new(self.name.as_str())
            .with_required_fields(out_required_fields)
            .with_optional_fields(out_optional_fields)
            .with_update_metrics_fn(update_metrics);

        let out_channel: SubscriptionChannel<Out> =
            SubscriptionChannel::connect_subscription(subscription, (&mut self).into()).await?;

        self.finish(out_channel).await
    }
}

impl<Out, L> CollectBuilder<Out, L>
where
    Out: AppData,
    L: Labeling + Debug + Send + Sync + 'static,
{
    #[tracing::instrument(level = "info")]
    async fn finish(self, out_channel: SubscriptionChannel<Out>) -> Result<Collect<Out>, CollectionError> {
        out_channel.subscription_receiver.check_attachment().await?;

        let tx_clearinghouse_api = self.clearinghouse.tx_api();
        (self.merge.outlet(), self.clearinghouse.inlet()).connect().await;

        self.clearinghouse
            .check()
            .await
            .map_err(|err| PortError::ChannelError(err.into()))?;

        let outlet = out_channel.outlet();
        tracing::info!(clearinghouse=?self.clearinghouse, channel_name=%self.name, "connected subscription channel");

        let merge_inlets = self.merge.inlets();
        let nr_merge_inlets = merge_inlets.len().await;
        if nr_merge_inlets != self.sources.len() {
            return Err(CollectionError::PortError(PortError::Detached(format!(
                "available merge inlets({}) does not match number of sources({})",
                nr_merge_inlets,
                self.sources.len()
            ))));
        }

        let mut g = Graph::default();
        for (idx, s) in self.sources.into_iter().enumerate() {
            match merge_inlets.get(idx).await {
                Some(merge_inlet) => {
                    tracing::info!(source=%s.name(), ?merge_inlet, "connecting collection source to clearinghouse.");
                    (s.outlet(), merge_inlet).connect().await;
                    g.push_back(s.dyn_upcast()).await;
                }

                None => {
                    tracing::warn!(source=%s.name(), "no available clearinghouse port for source - skipping source.");
                }
            }
        }

        g.push_back(Box::new(self.merge)).await;
        g.push_back(Box::new(self.clearinghouse)).await;
        g.push_back(Box::new(out_channel)).await;
        let composite = stage::CompositeSource::new(format!("{}_composite_source", self.name), g, outlet).await;

        let inner: Box<dyn SourceStage<Out>> = Box::new(composite);
        let outlet = inner.outlet();
        Ok(Collect {
            name: self.name,
            inner,
            outlet,
            tx_clearinghouse_api,
        })
    }
}
