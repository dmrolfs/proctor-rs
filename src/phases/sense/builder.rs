use std::collections::HashSet;
use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use cast_trait_object::DynCastExt;
use serde::de::DeserializeOwned;

use super::{Clearinghouse, Sense, SubscriptionChannel, SubscriptionRequirements};
use crate::elements::telemetry::UpdateMetricsFn;
use crate::elements::Telemetry;
use crate::error::{PortError, SenseError};
use crate::graph::stage::{self, SourceStage, Stage, WithApi};
use crate::graph::{Connect, Graph, Inlet, SinkShape, SourceShape, UniformFanInShape};
use crate::phases::sense::{ClearinghouseSubscriptionAgent, CorrelationGenerator, TelemetrySubscription};
use crate::{AppData, SharedString};

#[derive(Debug)]
pub struct SenseBuilder<Out> {
    name: SharedString,
    sources: Vec<Box<dyn SourceStage<Telemetry>>>,
    merge: stage::MergeN<Telemetry>,
    pub clearinghouse: Clearinghouse,
    marker: PhantomData<Out>,
}

#[async_trait]
impl<Out: Send> ClearinghouseSubscriptionAgent for SenseBuilder<Out> {
    async fn subscribe(
        &mut self, subscription: TelemetrySubscription, receiver: Inlet<Telemetry>,
    ) -> Result<(), SenseError> {
        self.clearinghouse.subscribe(subscription, &receiver).await;
        Ok(())
    }
}

impl<Out> SenseBuilder<Out> {
    #[tracing::instrument(level = "trace", skip(name, sources))]
    pub fn new(
        name: impl Into<SharedString>, sources: Vec<Box<dyn SourceStage<Telemetry>>>,
        correlation_generator: CorrelationGenerator,
    ) -> Self {
        let name = name.into();
        let nr_sources = sources.len();
        let merge = stage::MergeN::new(format!("{}_source_merge_{}", name, nr_sources), nr_sources);
        let clearinghouse = Clearinghouse::new(format!("{}_clearinghouse", name), correlation_generator);

        Self {
            name,
            sources,
            merge,
            clearinghouse,
            marker: PhantomData,
        }
    }
}

impl<Out> SenseBuilder<Out>
where
    Out: AppData + SubscriptionRequirements + DeserializeOwned,
{
    #[tracing::instrument(level = "trace", skip(), fields(nr_sources = % self.sources.len()))]
    pub async fn build_for_out(self) -> Result<Sense<Out>, SenseError> {
        self.build_for_out_requirements(
            <Out as SubscriptionRequirements>::required_fields(),
            <Out as SubscriptionRequirements>::optional_fields(),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip(update_metrics), fields(nr_sources = % self.sources.len()))]
    pub async fn build_for_out_w_metrics(
        self, update_metrics: Box<dyn (Fn(&str, &Telemetry)) + Send + Sync + 'static>,
    ) -> Result<Sense<Out>, SenseError> {
        self.build_for_out_requirements_w_metrics(
            <Out as SubscriptionRequirements>::required_fields(),
            <Out as SubscriptionRequirements>::optional_fields(),
            update_metrics,
        )
        .await
    }
}

impl SenseBuilder<Telemetry> {
    #[tracing::instrument(level="trace", fields(nr_sources = %self.sources.len()))]
    pub async fn build_for_telemetry_out_subscription(
        mut self, out_subscription: TelemetrySubscription,
    ) -> Result<Sense<Telemetry>, SenseError> {
        let out_channel = SubscriptionChannel::connect_telemetry_subscription(out_subscription, &mut self).await?;
        self.finish(out_channel).await
    }

    #[tracing::instrument(
        level = "trace",
        skip(out_required_fields, out_optional_fields, ),
        fields(nr_sources = %self.sources.len())
    )]
    pub async fn build_for_telemetry_out(
        mut self, out_required_fields: HashSet<String>, out_optional_fields: HashSet<String>,
    ) -> Result<Sense<Telemetry>, SenseError> {
        let subscription = TelemetrySubscription::new(self.name.as_ref())
            .with_required_fields(out_required_fields)
            .with_optional_fields(out_optional_fields);

        let out_channel = SubscriptionChannel::connect_telemetry_subscription(subscription, &mut self).await?;

        self.finish(out_channel).await
    }

    #[tracing::instrument(
        level = "trace",
        skip(out_required_fields, out_optional_fields, update_metrics),
        fields(nr_sources = %self.sources.len())
    )]
    pub async fn build_for_telemetry_out_w_metrics(
        mut self, out_required_fields: HashSet<String>, out_optional_fields: HashSet<String>,
        update_metrics: Box<dyn (Fn(&str, &Telemetry)) + Send + Sync + 'static>,
    ) -> Result<Sense<Telemetry>, SenseError> {
        let subscription = TelemetrySubscription::new(self.name.as_ref())
            .with_required_fields(out_required_fields)
            .with_optional_fields(out_optional_fields)
            .with_update_metrics_fn(update_metrics);

        let out_channel = SubscriptionChannel::connect_telemetry_subscription(subscription, &mut self).await?;

        self.finish(out_channel).await
    }
}

impl<Out> SenseBuilder<Out>
where
    Out: AppData + DeserializeOwned,
{
    #[tracing::instrument(level="trace", fields(nr_sources=%self.sources.len()))]
    pub async fn build_for_out_subscription(
        mut self, out_subscription: TelemetrySubscription,
    ) -> Result<Sense<Out>, SenseError> {
        let out_channel = SubscriptionChannel::connect_subscription(out_subscription, &mut self).await?;
        self.finish(out_channel).await
    }

    #[tracing::instrument(
        level = "trace",
        skip(out_required_fields, out_optional_fields),
        fields(nr_sources = % self.sources.len())
    )]
    pub async fn build_for_out_requirements(
        mut self, out_required_fields: HashSet<SharedString>, out_optional_fields: HashSet<SharedString>,
    ) -> Result<Sense<Out>, SenseError> {
        let subscription = TelemetrySubscription::new(self.name.as_ref())
            .with_required_fields(out_required_fields)
            .with_optional_fields(out_optional_fields);

        let out_channel: SubscriptionChannel<Out> =
            SubscriptionChannel::connect_subscription(subscription, &mut self).await?;

        self.finish(out_channel).await
    }

    #[tracing::instrument(
        level = "trace",
        skip(out_required_fields, out_optional_fields, update_metrics),
        fields(nr_sources = % self.sources.len())
    )]
    pub async fn build_for_out_requirements_w_metrics(
        mut self, out_required_fields: HashSet<SharedString>, out_optional_fields: HashSet<SharedString>,
        update_metrics: UpdateMetricsFn,
    ) -> Result<Sense<Out>, SenseError> {
        let subscription = TelemetrySubscription::new(self.name.as_ref())
            .with_required_fields(out_required_fields)
            .with_optional_fields(out_optional_fields)
            .with_update_metrics_fn(update_metrics);

        let out_channel: SubscriptionChannel<Out> =
            SubscriptionChannel::connect_subscription(subscription, &mut self).await?;

        self.finish(out_channel).await
    }
}

impl<Out> SenseBuilder<Out>
where
    Out: AppData,
{
    #[tracing::instrument(level = "trace")]
    async fn finish(self, out_channel: SubscriptionChannel<Out>) -> Result<Sense<Out>, SenseError> {
        out_channel.subscription_receiver.check_attachment().await?;

        let tx_clearinghouse_api = self.clearinghouse.tx_api();
        (self.merge.outlet(), self.clearinghouse.inlet()).connect().await;

        self.clearinghouse
            .check()
            .await
            .map_err(|err| PortError::Channel(err.into()))?;

        let outlet = out_channel.outlet();
        tracing::trace!(clearinghouse=?self.clearinghouse, channel_name=%self.name, "connected subscription channel");

        let merge_inlets = self.merge.inlets();
        let nr_merge_inlets = merge_inlets.len().await;
        if nr_merge_inlets != self.sources.len() {
            return Err(SenseError::PortError(PortError::Detached(format!(
                "available merge inlets({}) does not match number of sources({})",
                nr_merge_inlets,
                self.sources.len()
            ))));
        }

        let mut g = Graph::default();
        for (idx, s) in self.sources.into_iter().enumerate() {
            match merge_inlets.get(idx).await {
                Some(merge_inlet) => {
                    tracing::trace!(source=%s.name(), ?merge_inlet, "connecting sense source to clearinghouse.");
                    (s.outlet(), merge_inlet).connect().await;
                    g.push_back(s.dyn_upcast()).await;
                },

                None => {
                    tracing::warn!(source=%s.name(), "no available clearinghouse port for source - skipping source.");
                },
            }
        }

        g.push_back(Box::new(self.merge)).await;
        g.push_back(Box::new(self.clearinghouse)).await;
        g.push_back(Box::new(out_channel)).await;
        let composite = stage::CompositeSource::new(format!("{}_composite_source", self.name).into(), g, outlet).await;

        let inner: Box<dyn SourceStage<Out>> = Box::new(composite);
        let outlet = inner.outlet();
        Ok(Sense {
            name: self.name,
            inner,
            outlet,
            tx_clearinghouse_api,
        })
    }
}
