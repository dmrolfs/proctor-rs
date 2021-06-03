use std::fmt::{self, Debug};

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use tokio::sync::broadcast;

use crate::elements::{
    PolicyFilter, PolicyFilterApi, PolicyFilterEvent, PolicyFilterMonitor, PolicyOutcome, QueryPolicy,
};
use crate::graph::stage::{Stage, WithApi, WithMonitor};
use crate::graph::{Inlet, Outlet, Port, SinkShape, SourceShape};
use crate::{AppData, ProctorContext};
use anyhow::Result;
use oso::ToPolar;

pub struct Eligibility<T, C> {
    name: String,
    policy_filter: Box<dyn Stage>,
    pub context_inlet: Inlet<C>,
    inlet: Inlet<T>,
    outlet: Outlet<PolicyOutcome<T, C>>,
    tx_policy_api: PolicyFilterApi<C>,
    tx_policy_monitor: broadcast::Sender<PolicyFilterEvent<T, C>>,
}

impl<T: AppData + ToPolar + Clone, C: ProctorContext> Eligibility<T, C> {
    #[tracing::instrument(level = "info", skip(name))]
    pub fn new<S: Into<String>>(
        name: S, policy: impl QueryPolicy<Item = T, Context = C, Args = (T, C)> + 'static,
    ) -> Self {
        let name = name.into();
        let policy_filter = PolicyFilter::new(format!("{}_eligibility_policy", name), policy);
        let context_inlet = policy_filter.context_inlet();
        let inlet = policy_filter.inlet();
        let outlet = policy_filter.outlet();
        let tx_policy_api = policy_filter.tx_api();
        let tx_policy_monitor = policy_filter.tx_monitor.clone();

        Self {
            name,
            policy_filter: Box::new(policy_filter),
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

impl<T, C: Debug> Debug for Eligibility<T, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Eligibility")
            .field("name", &self.name)
            .field("policy_filter", &self.policy_filter)
            .field("context_inlet", &self.context_inlet)
            .field("inlet", &self.inlet)
            .field("outlet", &self.outlet)
            .finish()
    }
}

impl<T, C> SinkShape for Eligibility<T, C> {
    type In = T;
    #[inline]
    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

impl<T, C> SourceShape for Eligibility<T, C> {
    type Out = PolicyOutcome<T, C>;
    #[inline]
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<T: AppData, C: ProctorContext> Stage for Eligibility<T, C> {
    #[inline]
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> Result<()> {
        self.inlet.check_attachment().await?;
        self.context_inlet.check_attachment().await?;
        self.outlet.check_attachment().await?;
        self.policy_filter.check().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run eligibility phase", skip(self))]
    async fn run(&mut self) -> Result<()> {
        self.policy_filter.run().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn close(mut self: Box<Self>) -> Result<()> {
        tracing::trace!("closing eligibility ports.");
        self.inlet.close().await;
        self.context_inlet.close().await;
        self.outlet.close().await;
        self.policy_filter.close().await?;
        Ok(())
    }
}

impl<T, C> WithApi for Eligibility<T, C> {
    type Sender = PolicyFilterApi<C>;
    #[inline]
    fn tx_api(&self) -> Self::Sender {
        self.tx_policy_api.clone()
    }
}

impl<T, C> WithMonitor for Eligibility<T, C> {
    type Receiver = PolicyFilterMonitor<T, C>;
    #[inline]
    fn rx_monitor(&self) -> Self::Receiver {
        self.tx_policy_monitor.subscribe()
    }
}
