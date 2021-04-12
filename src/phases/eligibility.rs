use super::collection::{ClearinghouseApi, ClearinghouseCmd};
use crate::elements::{Policy, PolicyFilter, PolicyFilterApi, PolicyFilterEvent, PolicyFilterMonitor, TelemetryData};
use crate::graph::stage::{self, Stage, WithApi, WithMonitor};
use crate::graph::{Connect, Graph, GraphResult, Inlet, Outlet, Port, SinkShape, SourceShape, ThroughShape};
use crate::phases::collection::TelemetrySubscription;
use crate::{ProctorContext, ProctorResult};
use async_trait::async_trait;
use cast_trait_object::{dyn_upcast, DynCastExt};
use std::collections::HashSet;
use std::fmt::{self, Debug};
use tokio::sync::broadcast;

mod context;
mod policy;

pub struct Eligibility<C> {
    name: String,
    inner: Box<dyn InnerStage>,
    context_inlet: Inlet<C>,
    inlet: Inlet<TelemetryData>,
    outlet: Outlet<TelemetryData>,
    tx_policy_api: PolicyFilterApi<C>,
    tx_policy_monitor: broadcast::Sender<PolicyFilterEvent<TelemetryData, C>>,
}

impl<C: ProctorContext> Eligibility<C> {
    pub async fn new<S: Into<String>>(
        name: S, policy: impl Policy<Item = TelemetryData, Context= C> + 'static,
        tx_clearinghouse: ClearinghouseApi,
    ) -> ProctorResult<Self> {
        let name = name.into();
        let (context_source, es_outlet) = Self::subscribe_to_context(
            policy.subscription(name.as_str()),
            tx_clearinghouse,
        )
        .await?;
        let policy_filter = PolicyFilter::new(format!("eligibility_{}", name), Box::new(policy));
        let tx_policy_api = policy_filter.tx_api();
        let tx_policy_monitor = policy_filter.tx_monitor.clone();
        let context_inlet = policy_filter.context_inlet();
        let inner = Self::make_inner(name.as_str(), context_source, es_outlet, policy_filter).await?;
        let inlet = inner.inlet(); //Inlet::new(name.clone());
        let outlet = inner.outlet(); //Outlet::new(name.clone());
        Ok(Self {
            name,
            inner,
            context_inlet,
            inlet,
            outlet,
            tx_policy_api,
            tx_policy_monitor,
        })
    }

    #[inline]
    pub fn context_inlet(&self) -> Inlet<C> {
        self.context_inlet.clone()
    }
}

trait InnerStage: Stage + ThroughShape<In = TelemetryData, Out = TelemetryData> + 'static {}
impl<T: 'static + Stage + ThroughShape<In = TelemetryData, Out = TelemetryData>> InnerStage for T {}

impl<C: ProctorContext> Eligibility<C> {
    async fn make_inner<S: AsRef<str>>(
        name: S, context_source: Box<dyn Stage>, es_outlet: Outlet<C>,
        policy_filter: PolicyFilter<TelemetryData, C>,
    ) -> ProctorResult<Box<dyn InnerStage>> {
        let name = name.as_ref();

        let graph_inlet = policy_filter.inlet();
        (es_outlet, policy_filter.context_inlet()).connect().await;
        let graph_outlet = policy_filter.outlet();

        let mut graph = Graph::default();
        graph.push_back(context_source).await;
        graph.push_back(Box::new(policy_filter)).await;
        let composite = stage::CompositeThrough::new(
            format!("eligibility_composite_{}", name),
            graph,
            graph_inlet,
            graph_outlet,
        )
        .await;
        Ok(Box::new(composite))
    }

    //todo: simplify return to a Stage + SourceShape<C> once upcasting is better support wrt type constraints and/or auto trait support is expanded.
    // but until then settled on this approach to return outlet to be connected with stage.
    async fn subscribe_to_context(
        subscription: TelemetrySubscription,
        tx_clearinghouse: ClearinghouseApi,
    ) -> ProctorResult<(Box<dyn Stage>, Outlet<C>)> {
        let convert_telemetry = crate::elements::make_from_telemetry::<C, _>(&subscription.name, true).await?;

        let (cmd, ack) = ClearinghouseCmd::subscribe(subscription, convert_telemetry.inlet());
        tx_clearinghouse.send(cmd)?;
        ack.await?;

        let outlet = convert_telemetry.outlet();
        Ok((convert_telemetry.dyn_upcast(), outlet))
    }
}

impl<C> SinkShape for Eligibility<C> {
    type In = TelemetryData;
    #[inline]
    fn inlet(&self) -> Inlet<Self::In> {
        self.inner.inlet()
    }
}

impl<C> SourceShape for Eligibility<C> {
    type Out = TelemetryData;
    #[inline]
    fn outlet(&self) -> Outlet<Self::Out> {
        self.inner.outlet()
    }
}

#[dyn_upcast]
#[async_trait]
impl<C: Send + 'static> Stage for Eligibility<C> {
    #[inline]
    fn name(&self) -> &str {
        self.name.as_ref()
    }

    #[tracing::instrument(level = "info", name = "run eligibility through", skip(self))]
    async fn run(&mut self) -> GraphResult<()> {
        self.inner.run().await
    }

    async fn close(mut self: Box<Self>) -> GraphResult<()> {
        tracing::trace!("closing eligibility ports.");
        self.inlet.close().await;
        self.context_inlet.close().await;
        self.inner.close().await?;
        self.outlet.close().await;
        Ok(())
    }
}

impl<C> WithApi for Eligibility<C> {
    type Sender = PolicyFilterApi<C>;
    #[inline]
    fn tx_api(&self) -> Self::Sender {
        self.tx_policy_api.clone()
    }
}

impl<C> WithMonitor for Eligibility<C> {
    type Receiver = PolicyFilterMonitor<TelemetryData, C>;
    #[inline]
    fn rx_monitor(&self) -> Self::Receiver {
        self.tx_policy_monitor.subscribe()
    }
}

impl<C> Debug for Eligibility<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Eligibility")
            .field("name", &self.name)
            .field("inner", &self.inner)
            .field("inlet", &self.inlet)
            .field("outlet", &self.outlet)
            .finish()
    }
}
