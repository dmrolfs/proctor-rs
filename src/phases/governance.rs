use std::fmt::{self, Debug};

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use oso::{PolarValue, ToPolar};
use tokio::sync::broadcast;

use crate::elements::{
    PolicyFilter, PolicyFilterApi, PolicyFilterEvent, PolicyFilterMonitor, PolicyOutcome, QueryPolicy,
};
use crate::error::GovernanceError;
use crate::graph::stage::{self, Stage, ThroughStage, WithApi, WithMonitor};
use crate::graph::{Connect, Graph, Inlet, Outlet, Port, SinkShape, SourceShape};
use crate::{AppData, ProctorContext, ProctorResult};

pub struct Governance<In, Out, C> {
    name: String,
    inner_policy_transform: Box<dyn ThroughStage<In, Out>>,
    pub context_inlet: Inlet<C>,
    inlet: Inlet<In>,
    outlet: Outlet<Out>,
    tx_policy_api: PolicyFilterApi<C>,
    tx_policy_monitor: broadcast::Sender<PolicyFilterEvent<In, C>>,
}

impl<T: AppData + ToPolar, C: ProctorContext> Governance<T, PolicyOutcome<T, C>, C> {
    #[tracing::instrument(level = "info", skip(name))]
    pub async fn carry_policy_result<P>(name: impl AsRef<str>, policy: P) -> Self
    where
        P: 'static + QueryPolicy<Item = T, Context = C, Args = (T, C, PolarValue)>,
    {
        let name = format!("{}_carry_governance_policy_result", name.as_ref());
        let inlet = Inlet::new(name.as_str());
        let outlet = Outlet::new(name.as_str());
        let identity = stage::Identity::new(name.as_str(), inlet, outlet);
        Self::with_transform(name, policy, identity).await
    }
}

impl<In: AppData + ToPolar, Out: AppData, C: ProctorContext> Governance<In, Out, C> {
    #[tracing::instrument(level="info", skip(name), fields(stage_name=%name.as_ref()))]
    pub async fn with_transform<P, T>(name: impl AsRef<str>, policy: P, transform: T) -> Self
    where
        P: 'static + QueryPolicy<Item = In, Context = C, Args = (In, C, PolarValue)>,
        T: 'static + ThroughStage<PolicyOutcome<In, C>, Out>,
    {
        let policy_filter = PolicyFilter::new(format!("{}_governance_policy", name.as_ref()), policy);
        let context_inlet = policy_filter.context_inlet();
        let tx_policy_api = policy_filter.tx_api();
        let tx_policy_monitor = policy_filter.tx_monitor.clone();

        let graph_inlet = policy_filter.inlet();
        (policy_filter.outlet(), transform.inlet()).connect().await;
        let graph_outlet = transform.outlet();

        let mut graph = Graph::default();
        graph.push_back(Box::new(policy_filter)).await;
        graph.push_back(Box::new(transform)).await;

        let inner_policy_transform = Box::new(
            stage::CompositeThrough::new(format!("{}_composite", name.as_ref()), graph, graph_inlet, graph_outlet)
                .await,
        );

        let inlet = inner_policy_transform.inlet();
        let outlet = inner_policy_transform.outlet();

        Self {
            name: name.as_ref().to_string(),
            inner_policy_transform,
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

impl<In, Out, C> Debug for Governance<In, Out, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Governance")
            .field("name", &self.name)
            .field("inner_policy_transform", &self.inner_policy_transform)
            .field("context_inlet", &self.context_inlet)
            .field("inlet", &self.inlet)
            .field("outlet", &self.outlet)
            .finish()
    }
}

impl<In, Out, C> SinkShape for Governance<In, Out, C> {
    type In = In;

    fn inlet(&self) -> Inlet<Self::In> {
        self.inner_policy_transform.inlet()
    }
}

impl<In, Out, C> SourceShape for Governance<In, Out, C> {
    type Out = Out;

    fn outlet(&self) -> Outlet<Self::Out> {
        self.inner_policy_transform.outlet()
    }
}

#[dyn_upcast]
#[async_trait]
impl<In: AppData, Out: AppData, C: ProctorContext> Stage for Governance<In, Out, C> {
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

impl<In: AppData, Out: AppData, C: ProctorContext> Governance<In, Out, C> {
    async fn do_check(&self) -> Result<(), GovernanceError> {
        self.inlet.check_attachment().await?;
        self.context_inlet.check_attachment().await?;
        self.inner_policy_transform
            .check()
            .await
            .map_err(|err| GovernanceError::StageError(err.into()))?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    async fn do_run(&mut self) -> Result<(), GovernanceError> {
        self.inner_policy_transform
            .run()
            .await
            .map_err(|err| GovernanceError::StageError(err.into()))?;
        Ok(())
    }

    async fn do_close(mut self: Box<Self>) -> Result<(), GovernanceError> {
        tracing::trace!("closing governance ports.");
        self.inlet.close().await;
        self.context_inlet.close().await;
        self.outlet.close().await;
        self.inner_policy_transform
            .close()
            .await
            .map_err(|err| GovernanceError::StageError(err.into()))?;
        Ok(())
    }
}

impl<In, Out, C> WithApi for Governance<In, Out, C> {
    type Sender = PolicyFilterApi<C>;

    fn tx_api(&self) -> Self::Sender {
        self.tx_policy_api.clone()
    }
}

impl<In, Out, C> WithMonitor for Governance<In, Out, C> {
    type Receiver = PolicyFilterMonitor<In, C>;

    fn rx_monitor(&self) -> Self::Receiver {
        self.tx_policy_monitor.subscribe()
    }
}
