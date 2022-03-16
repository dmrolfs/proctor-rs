use std::fmt::{self, Debug};
use std::sync::Arc;

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use oso::ToPolar;
use serde::Serialize;
use tokio::sync::broadcast;

use crate::elements::{
    PolicyFilter, PolicyFilterApi, PolicyFilterEvent, PolicyFilterMonitor, PolicyOutcome, QueryPolicy,
};
use crate::error::{PolicyError, ProctorError};
use crate::graph::stage::{Stage, ThroughStage, WithApi, WithMonitor};
use crate::graph::{stage, Connect, Graph, Inlet, Outlet, Port, SinkShape, SourceShape, PORT_DATA};
use crate::{AppData, ProctorContext, ProctorResult, SharedString};

pub struct PolicyPhase<In, Out, C, D> {
    name: SharedString,
    policy_transform: Box<dyn ThroughStage<In, Out>>,
    context_inlet: Inlet<C>,
    inlet: Inlet<In>,
    outlet: Outlet<Out>,
    tx_policy_api: PolicyFilterApi<C, D>,
    tx_policy_monitor: broadcast::Sender<Arc<PolicyFilterEvent<In, C>>>,
}

impl<In, C, D> PolicyPhase<In, PolicyOutcome<In, C>, C, D>
where
    In: AppData + ToPolar,
    C: ProctorContext,
    D: AppData + Serialize,
{
    #[tracing::instrument(level = "trace", skip(name))]
    pub async fn carry_policy_outcome<P>(name: &str, policy: P) -> Result<Self, PolicyError>
    where
        P: 'static + QueryPolicy<Item = In, Context = C, TemplateData = D>,
    {
        let name = SharedString::Owned(format!("{}_carry_policy_outcome", name));
        let identity: stage::Identity<PolicyOutcome<In, C>> = stage::Identity::new(
            name.to_string(),
            Inlet::new(name.clone(), PORT_DATA),
            Outlet::new(name.clone(), PORT_DATA),
        );
        Self::with_transform(name, policy, identity).await
    }
}

impl<T, C, D> PolicyPhase<T, T, C, D>
where
    T: AppData + ToPolar,
    C: ProctorContext,
    D: AppData + Serialize,
{
    #[tracing::instrument(level = "trace", skip(name))]
    pub async fn strip_policy_outcome<P>(name: &str, policy: P) -> Result<Self, PolicyError>
    where
        P: 'static + QueryPolicy<Item = T, Context = C, TemplateData = D>,
    {
        let name = SharedString::Owned(format!("{}_strip_policy_outcome", name));
        let strip = stage::Map::new(name.to_string(), |outcome: PolicyOutcome<T, C>| outcome.item);
        Self::with_transform(name, policy, strip).await
    }
}

impl<In, Out, C, D> PolicyPhase<In, Out, C, D>
where
    In: AppData + ToPolar,
    Out: AppData,
    C: ProctorContext,
    D: AppData + Serialize,
{
    #[tracing::instrument(level = "trace", skip(name))]
    pub async fn with_transform<P, T>(name: SharedString, policy: P, transform: T) -> Result<Self, PolicyError>
    where
        P: 'static + QueryPolicy<Item = In, Context = C, TemplateData = D>,
        T: 'static + ThroughStage<PolicyOutcome<In, C>, Out>,
    {
        let policy_filter = PolicyFilter::new(format!("{}_filter", name), policy)?;
        let context_inlet = policy_filter.context_inlet();
        let tx_policy_api = policy_filter.tx_api();
        let tx_policy_monitor = policy_filter.tx_monitor.clone();

        let graph_inlet = policy_filter.inlet();
        (policy_filter.outlet(), transform.inlet()).connect().await;
        let graph_outlet = transform.outlet();

        let mut graph = Graph::default();
        graph.push_back(Box::new(policy_filter)).await;
        graph.push_back(Box::new(transform)).await;

        let policy_transform = Box::new(
            stage::CompositeThrough::new(format!("{}_composite", name).into(), graph, graph_inlet, graph_outlet).await,
        );

        let inlet = policy_transform.inlet();
        let outlet = policy_transform.outlet();

        Ok(Self {
            name,
            policy_transform,
            context_inlet,
            inlet,
            outlet,
            tx_policy_api,
            tx_policy_monitor,
        })
    }

    pub fn context_inlet(&self) -> Inlet<C> {
        self.context_inlet.clone()
    }
}

impl<In, Out, C, D> Debug for PolicyPhase<In, Out, C, D>
where
    C: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(format!("PolicyPhase[{}]", self.name).as_str())
            .field("policy_transform", &self.policy_transform)
            .field("context_inlet", &self.context_inlet)
            .field("inlet", &self.inlet)
            .field("outlet", &self.outlet)
            .finish()
    }
}

impl<In, Out, C, D> SinkShape for PolicyPhase<In, Out, C, D> {
    type In = In;

    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

impl<In, Out, C, D> SourceShape for PolicyPhase<In, Out, C, D> {
    type Out = Out;

    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<In, Out, C, D> Stage for PolicyPhase<In, Out, C, D>
where
    In: AppData,
    Out: AppData,
    C: ProctorContext,
    D: AppData,
{
    fn name(&self) -> SharedString {
        self.name.clone()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await.map_err(|err| ProctorError::Phase(err.into()))?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", name = "run policy phase", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        self.do_run().await.map_err(|err| ProctorError::Phase(err.into()))?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        self.do_close().await.map_err(|err| ProctorError::Phase(err.into()))?;
        Ok(())
    }
}

// this implementation block provides a convenient means to ground errors to the phase error.
impl<In, Out, C, D> PolicyPhase<In, Out, C, D>
where
    In: AppData,
    Out: AppData,
    C: ProctorContext,
    C::Error: From<anyhow::Error>,
    D: Send,
{
    async fn do_check(&self) -> Result<(), C::Error> {
        self.inlet.check_attachment().await.map_err(|err| err.into())?;
        self.context_inlet.check_attachment().await.map_err(|err| err.into())?;
        self.policy_transform.check().await.map_err(|err| err.into())?;
        self.outlet.check_attachment().await.map_err(|err| err.into())?;
        Ok(())
    }

    async fn do_run(&mut self) -> Result<(), C::Error> {
        self.policy_transform.run().await.map_err(|err| err.into())?;
        Ok(())
    }

    async fn do_close(mut self: Box<Self>) -> Result<(), C::Error> {
        tracing::trace!(stage=%self.name, "closing decision ports.");
        self.inlet.close().await;
        self.context_inlet.close().await;
        self.outlet.close().await;
        self.policy_transform.close().await.map_err(|err| err.into())?;
        Ok(())
    }
}

impl<In, Out, C, D> WithApi for PolicyPhase<In, Out, C, D> {
    type Sender = PolicyFilterApi<C, D>;

    fn tx_api(&self) -> Self::Sender {
        self.tx_policy_api.clone()
    }
}

impl<In, Out, C, D> WithMonitor for PolicyPhase<In, Out, C, D> {
    type Receiver = PolicyFilterMonitor<In, C>;

    fn rx_monitor(&self) -> Self::Receiver {
        self.tx_policy_monitor.subscribe()
    }
}
