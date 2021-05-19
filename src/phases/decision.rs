use crate::elements::{PolicyFilter, PolicyFilterApi, PolicyFilterEvent, PolicyFilterMonitor, QueryPolicy};
use crate::graph::stage::{Stage, ThroughStage, WithApi, WithMonitor};
use crate::graph::{stage, Connect, Graph, GraphResult, Inlet, Outlet, Port, SinkShape, SourceShape};
use crate::{AppData, ProctorContext};
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use oso::ToPolar;
use std::fmt::{self, Debug};
use tokio::sync::broadcast;

pub struct Decision<In, Out, C> {
    name: String,
    inner_stage: Box<dyn ThroughStage<In, Out>>,
    pub context_inlet: Inlet<C>,
    inlet: Inlet<In>,
    outlet: Outlet<Out>,
    tx_policy_api: PolicyFilterApi<C>,
    tx_policy_monitor: broadcast::Sender<PolicyFilterEvent<In, C>>,
}

impl<In: AppData + ToPolar + Clone, Out: AppData, C: ProctorContext> Decision<In, Out, C> {
    #[tracing::instrument(level = "info", skip(name))]
    pub async fn new<S: Into<String>>(
        name: S, policy: impl QueryPolicy<Args = (In, C), QueryResult = bool> + 'static,
        transform: impl ThroughStage<In, Out> + 'static,
    ) -> Self {
        let name = name.into();

        let policy_filter = PolicyFilter::new(format!("{}_decision_policy", name), Box::new(policy));
        let context_inlet = policy_filter.context_inlet();
        let tx_policy_api = policy_filter.tx_api();
        let tx_policy_monitor = policy_filter.tx_monitor.clone();

        let inlet = policy_filter.inlet();
        (policy_filter.outlet(), transform.inlet()).connect().await;
        let outlet = transform.outlet();

        let mut graph = Graph::default();
        graph.push_back(Box::new(policy_filter)).await;
        graph.push_back(Box::new(transform)).await;

        let inner_stage =
            stage::CompositeThrough::new(format!("{}_inner", name), graph, inlet.clone(), outlet.clone()).await;

        Self {
            name,
            inner_stage: Box::new(inner_stage),
            context_inlet,
            inlet,
            outlet,
            tx_policy_api,
            tx_policy_monitor,
        }
    }

    #[inline]
    pub fn context_inlet(&self) -> Inlet<C> {
        self.context_inlet.clone()
    }
}

impl<In, Out, C: Debug> Debug for Decision<In, Out, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Decision")
            .field("name", &self.name)
            .field("inner_stage", &self.inner_stage)
            .field("context_inlet", &self.context_inlet)
            .field("inlet", &self.inlet)
            .field("outlet", &self.outlet)
            .finish()
    }
}

impl<In, Out, C> SinkShape for Decision<In, Out, C> {
    type In = In;

    #[inline]
    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

impl<In, Out, C> SourceShape for Decision<In, Out, C> {
    type Out = Out;

    #[inline]
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<In: AppData, Out: AppData, C: ProctorContext> Stage for Decision<In, Out, C> {
    #[inline]
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> GraphResult<()> {
        self.inlet.check_attachment().await?;
        self.context_inlet.check_attachment().await?;
        self.inner_stage.check().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run decision phase", skip(self))]
    async fn run(&mut self) -> GraphResult<()> {
        self.inner_stage.run().await
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn close(mut self: Box<Self>) -> GraphResult<()> {
        tracing::trace!("closing decision ports.");
        self.inlet.close().await;
        self.context_inlet.close().await;
        self.outlet.close().await;
        self.inner_stage.close().await?;
        Ok(())
    }
}

impl<In, Out, C> WithApi for Decision<In, Out, C> {
    type Sender = PolicyFilterApi<C>;
    #[inline]
    fn tx_api(&self) -> Self::Sender {
        self.tx_policy_api.clone()
    }
}

impl<In, Out, C> WithMonitor for Decision<In, Out, C> {
    type Receiver = PolicyFilterMonitor<In, C>;
    #[inline]
    fn rx_monitor(&self) -> Self::Receiver {
        self.tx_policy_monitor.subscribe()
    }
}
