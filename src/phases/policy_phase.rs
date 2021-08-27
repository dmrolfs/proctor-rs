use std::fmt::{self, Debug};

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use oso::{PolarValue, ToPolar};
use tokio::sync::broadcast;

use crate::elements::{
    PolicyFilter, PolicyFilterApi, PolicyFilterEvent, PolicyFilterMonitor, PolicyOutcome, QueryPolicy,
};
use crate::error::{PortError, ProctorError};
use crate::graph::stage::{Stage, ThroughStage, WithApi, WithMonitor};
use crate::graph::{stage, Connect, Graph, Inlet, Outlet, Port, SinkShape, SourceShape};
use crate::{AppData, ProctorContext, ProctorResult};

pub struct PolicyPhase<In, Out, C> {
    name: String,
    policy_transform: Box<dyn ThroughStage<In, Out>>,
    pub context_inlet: Inlet<C>,
    inlet: Inlet<In>,
    outlet: Outlet<Out>,
    tx_policy_api: PolicyFilterApi<C>,
    tx_policy_monitor: broadcast::Sender<PolicyFilterEvent<In, C>>,
}

impl<In: AppData + ToPolar, C: ProctorContext> PolicyPhase<In, PolicyOutcome<In, C>, C> {
    #[tracing::instrument(level = "info", skip(name))]
    pub async fn carry_policy_result<P>(name: impl AsRef<str>, policy: P) -> Self
    where
        P: 'static + QueryPolicy<Item = In, Context = C, Args = (In, C, PolarValue)>,
    {
        let name = format!("{}_carry_policy_result", name.as_ref());
        let identity: stage::Identity<PolicyOutcome<In, C>> =
            stage::Identity::new(name.as_str(), Inlet::new(name.as_str()), Outlet::new(name.as_str()));
        Self::with_transform(name.as_str(), policy, identity).await
    }
}

impl<In: AppData + ToPolar, Out: AppData, C: ProctorContext> PolicyPhase<In, Out, C> {
    #[tracing::instrument(level = "info", skip(name))]
    pub async fn with_transform<S, P, T>(name: S, policy: P, transform: T) -> Self
    where
        S: AsRef<str>,
        P: 'static + QueryPolicy<Item = In, Context = C, Args = (In, C, PolarValue)>,
        T: 'static + ThroughStage<PolicyOutcome<In, C>, Out>,
    {
        let policy_filter = PolicyFilter::new(format!("{}_phase", name.as_ref()), policy);
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
            stage::CompositeThrough::new(format!("{}_composite", name.as_ref()), graph, graph_inlet, graph_outlet)
                .await,
        );

        let inlet = policy_transform.inlet();
        let outlet = policy_transform.outlet();

        Self {
            name: name.as_ref().to_string(),
            policy_transform,
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

impl<In, Out, C: Debug> Debug for PolicyPhase<In, Out, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(format!("PolicyPhase[{}]", self.name).as_str())
            .field("policy_transform", &self.policy_transform)
            .field("context_inlet", &self.context_inlet)
            .field("inlet", &self.inlet)
            .field("outlet", &self.outlet)
            .finish()
    }
}

impl<In, Out, C> SinkShape for PolicyPhase<In, Out, C> {
    type In = In;

    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

impl<In, Out, C> SourceShape for PolicyPhase<In, Out, C> {
    type Out = Out;

    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<In, Out, C> Stage for PolicyPhase<In, Out, C>
where
    In: AppData,
    Out: AppData,
    C: ProctorContext,
    C::Error: From<ProctorError> + From<PortError>,
    ProctorError: From<<C as ProctorContext>::Error>,
{
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run policy phase", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        self.do_run().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        self.do_close().await?;
        Ok(())
    }
}

// this implementation block provides a convenient means to ground errors to the phase error.
impl<In: AppData, Out: AppData, C: ProctorContext> PolicyPhase<In, Out, C>
where
    In: AppData,
    Out: AppData,
    C: ProctorContext,
    C::Error: From<ProctorError> + From<PortError>,
{
    async fn do_check(&self) -> Result<(), C::Error> {
        self.inlet.check_attachment().await?;
        self.context_inlet.check_attachment().await?;
        self.policy_transform.check().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    async fn do_run(&mut self) -> Result<(), C::Error> {
        self.policy_transform.run().await?;
        Ok(())
    }

    async fn do_close(mut self: Box<Self>) -> Result<(), C::Error> {
        tracing::trace!("closing decision ports.");
        self.inlet.close().await;
        self.context_inlet.close().await;
        self.outlet.close().await;
        self.policy_transform.close().await?;
        Ok(())
    }
}

impl<In, Out, C> WithApi for PolicyPhase<In, Out, C> {
    type Sender = PolicyFilterApi<C>;

    fn tx_api(&self) -> Self::Sender {
        self.tx_policy_api.clone()
    }
}

impl<In, Out, C> WithMonitor for PolicyPhase<In, Out, C> {
    type Receiver = PolicyFilterMonitor<In, C>;

    fn rx_monitor(&self) -> Self::Receiver {
        self.tx_policy_monitor.subscribe()
    }
}
