use std::fmt::{self, Debug};

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use oso::{PolarValue, ToPolar};
use tokio::sync::broadcast;

use crate::elements::{
    PolicyFilter, PolicyFilterApi, PolicyFilterEvent, PolicyFilterMonitor, PolicyOutcome, QueryPolicy,
};
use crate::error::GovernanceError;
use crate::graph::stage::{Stage, ThroughStage, WithApi, WithMonitor};
use crate::graph::{Inlet, Outlet, Port, SinkShape, SourceShape};
use crate::{AppData, ProctorContext, ProctorResult};

pub struct Governance<T, C> {
    name: String,
    policy_stage: Box<dyn ThroughStage<T, PolicyOutcome<T, C>>>,
    pub context_inlet: Inlet<C>,
    inlet: Inlet<T>,
    outlet: Outlet<PolicyOutcome<T, C>>,
    tx_policy_api: PolicyFilterApi<C>,
    tx_policy_monitor: broadcast::Sender<PolicyFilterEvent<T, C>>,
}

impl<T: AppData + ToPolar, C: ProctorContext> Governance<T, C> {
    #[tracing::instrument(level="info", skip(name), fields(stage_name=%name.as_ref()))]
    pub async fn new<P>(name: impl AsRef<str>, policy: P) -> Self
    where
        P: 'static + QueryPolicy<Item = T, Context = C, Args = (T, C, PolarValue)>,
    {
        let policy_stage = Box::new(PolicyFilter::new(
            format!("{}_governance_policy", name.as_ref()),
            policy,
        ));
        let context_inlet = policy_stage.context_inlet();
        let inlet = policy_stage.inlet();
        let outlet = policy_stage.outlet();
        let tx_policy_api = policy_stage.tx_api();
        let tx_policy_monitor = policy_stage.tx_monitor.clone();

        Self {
            name: name.as_ref().to_string(),
            policy_stage,
            context_inlet,
            inlet,
            outlet,
            tx_policy_api,
            tx_policy_monitor,
        }
    }

    pub fn context_inlet(&self) -> Inlet<C> {
        self.context_inlet.clone()
    }
}

impl<T, C> Debug for Governance<T, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Governance")
            .field("name", &self.name)
            .field("policy_stage", &self.policy_stage)
            .field("context_inlet", &self.context_inlet)
            .field("inlet", &self.inlet)
            .field("outlet", &self.outlet)
            .finish()
    }
}


impl<T, C> SinkShape for Governance<T, C> {
    type In = T;

    fn inlet(&self) -> Inlet<Self::In> {
        self.policy_stage.inlet()
    }
}

impl<T, C> SourceShape for Governance<T, C> {
    type Out = PolicyOutcome<T, C>;

    fn outlet(&self) -> Outlet<Self::Out> {
        self.policy_stage.outlet()
    }
}


#[dyn_upcast]
#[async_trait]
impl<T: AppData, C: ProctorContext> Stage for Governance<T, C> {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run governance phase", skip(self))]
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

impl<T: AppData, C: ProctorContext> Governance<T, C> {
    async fn do_check(&self) -> Result<(), GovernanceError> {
        self.policy_stage
            .check()
            .await
            .map_err(|err| GovernanceError::StageError(err.into()))?;
        self.context_inlet.check_attachment().await?;
        Ok(())
    }

    async fn do_run(&mut self) -> Result<(), GovernanceError> {
        self.policy_stage
            .run()
            .await
            .map_err(|err| GovernanceError::StageError(err.into()))?;
        Ok(())
    }

    async fn do_close(mut self: Box<Self>) -> Result<(), GovernanceError> {
        tracing::trace!("closing governance ports.");
        self.policy_stage
            .close()
            .await
            .map_err(|err| GovernanceError::StageError(err.into()))?;
        self.context_inlet.close().await;
        Ok(())
    }
}

impl<T, C> WithApi for Governance<T, C> {
    type Sender = PolicyFilterApi<C>;

    fn tx_api(&self) -> Self::Sender {
        self.tx_policy_api.clone()
    }
}

impl<T, C> WithMonitor for Governance<T, C> {
    type Receiver = PolicyFilterMonitor<T, C>;

    fn rx_monitor(&self) -> Self::Receiver {
        self.tx_policy_monitor.subscribe()
    }
}
