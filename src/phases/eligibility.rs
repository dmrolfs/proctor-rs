use crate::elements::{Policy, PolicyFilter, PolicyFilterApi, PolicyFilterEvent, PolicyFilterMonitor};
use crate::error::GraphError;
use crate::graph::stage::{Stage, WithApi, WithMonitor};
use crate::graph::{GraphResult, Inlet, Outlet, Port, SinkShape, SourceShape, ThroughShape};
use crate::{AppData, ProctorContext};
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use std::fmt::{self, Debug};
use tokio::sync::broadcast;

mod context;
mod policy;

pub struct Eligibility<D, C> {
    name: String,
    inner_stage: Option<Box<dyn InnerStage<D>>>,
    pub context_inlet: Inlet<C>,
    inlet: Inlet<D>,
    outlet: Outlet<D>,
    tx_policy_api: PolicyFilterApi<C>,
    tx_policy_monitor: broadcast::Sender<PolicyFilterEvent<D, C>>,
}

impl<D: AppData + Clone, C: ProctorContext> Eligibility<D, C> {
    #[tracing::instrument(level = "info", skip(name))]
    pub fn new<S: Into<String>>(name: S, policy: impl Policy<Item = D, Context = C> + 'static) -> Self {
        let name = name.into();
        let policy_filter: PolicyFilter<D, C> =
            PolicyFilter::new(format!("{}_eligibility_policy", name), Box::new(policy));
        let context_inlet = policy_filter.context_inlet();
        let inlet = policy_filter.inlet();
        let outlet = policy_filter.outlet();
        let tx_policy_api = policy_filter.tx_api();
        let tx_policy_monitor = policy_filter.tx_monitor.clone();

        Self {
            name,
            inner_stage: Some(Box::new(policy_filter)),
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

impl<D, C: Debug> Debug for Eligibility<D, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Eligibility")
            .field("name", &self.name)
            .field("inner_stage", &self.inner_stage)
            .field("context_inlet", &self.context_inlet)
            .field("inlet", &self.inlet)
            .field("outlet", &self.outlet)
            .finish()
    }
}

trait InnerStage<D>: Stage + ThroughShape<In = D, Out = D> + 'static {}
impl<D, T: 'static + Stage + ThroughShape<In = D, Out = D>> InnerStage<D> for T {}

impl<D, C> SinkShape for Eligibility<D, C> {
    type In = D;
    #[inline]
    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

impl<D, C> SourceShape for Eligibility<D, C> {
    type Out = D;
    #[inline]
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<D: AppData, C: ProctorContext> Stage for Eligibility<D, C> {
    #[inline]
    fn name(&self) -> &str {
        self.name.as_ref()
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> GraphResult<()> {
        self.inlet.check_attachment().await?;
        self.context_inlet.check_attachment().await?;
        self.outlet.check_attachment().await?;
        if let Some(ref inner) = self.inner_stage {
            inner.check().await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run eligibility through", skip(self))]
    async fn run(&mut self) -> GraphResult<()> {
        match self.inner_stage.as_mut() {
            Some(inner) => inner.run().await,
            None => Err(GraphError::GraphPrecondition(
                "eligibility already spent - cannot run.".to_string(),
            )),
        }
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn close(mut self: Box<Self>) -> GraphResult<()> {
        tracing::trace!("closing eligibility ports.");
        self.inlet.close().await;
        self.context_inlet.close().await;
        if let Some(inner) = self.inner_stage.take() {
            inner.close().await?;
        }
        self.outlet.close().await;
        Ok(())
    }
}

impl<D, C> WithApi for Eligibility<D, C> {
    type Sender = PolicyFilterApi<C>;
    #[inline]
    fn tx_api(&self) -> Self::Sender {
        self.tx_policy_api.clone()
    }
}

impl<D, C> WithMonitor for Eligibility<D, C> {
    type Receiver = PolicyFilterMonitor<D, C>;
    #[inline]
    fn rx_monitor(&self) -> Self::Receiver {
        self.tx_policy_monitor.subscribe()
    }
}