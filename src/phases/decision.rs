use crate::elements::{
    PolicyFilter, PolicyFilterApi, PolicyFilterEvent, PolicyFilterMonitor, PolicyOutcome, QueryPolicy, QueryResult,
};
use crate::error::DecisionError;
use crate::error::PolicyError;
use crate::graph::stage::{Stage, ThroughStage, WithApi, WithMonitor};
use crate::graph::{stage, Connect, Graph, Inlet, Outlet, Port, SinkShape, SourceShape};
use crate::{AppData, ProctorContext, ProctorResult};
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use oso::{Oso, PolarValue, ToPolar};
use std::fmt::{self, Debug};
use tokio::sync::broadcast;

pub struct Decision<In, Out, C> {
    name: String,
    inner_policy_transform: Box<dyn ThroughStage<In, Out>>,
    pub context_inlet: Inlet<C>,
    inlet: Inlet<In>,
    outlet: Outlet<Out>,
    tx_policy_api: PolicyFilterApi<C>,
    tx_policy_monitor: broadcast::Sender<PolicyFilterEvent<In, C>>,
}

// impl<T: AppData + ToPolar + Clone, C: ProctorContext> Decision<T, T, C> {
//     #[tracing::instrument(level = "info", skip(name))]
//     pub async fn basic<S: Into<String>>(
//         name: S, policy: impl QueryPolicy<Item = T, Context = C, Args = (T, C, PolarValue)> + 'static,
//     ) -> Self {
//         let strip_policy_result: stage::Map<_, PolicyResult<T>, T> =
//             stage::Map::new("strip_policy_result", |res: PolicyResult<T>| res.item);
//         Self::with_transform(name, policy, strip_policy_result).await
//     }
// }

impl<T: AppData + ToPolar + Clone, C: ProctorContext> Decision<T, PolicyOutcome<T, C>, C> {
    #[tracing::instrument(level = "info", skip(name))]
    pub async fn carry_policy_result<S: AsRef<str>>(
        name: S, policy: impl QueryPolicy<Item = T, Context = C, Args = (T, C, PolarValue)> + 'static,
    ) -> Self {
        let name = format!("{}_carry_policy_result", name.as_ref());
        let identity = stage::Identity::new(&name, Inlet::new(&name), Outlet::new(&name));
        Self::with_transform(name, policy, identity).await
    }
}

impl<In: AppData + ToPolar + Clone, Out: AppData, C: ProctorContext> Decision<In, Out, C> {
    #[tracing::instrument(level = "info", skip(name))]
    pub async fn with_transform<S: Into<String>>(
        name: S, policy: impl QueryPolicy<Item = In, Context = C, Args = (In, C, PolarValue)> + 'static,
        transform: impl ThroughStage<PolicyOutcome<In, C>, Out> + 'static,
    ) -> Self {
        let name = name.into();

        let decision_policy = Self::policy_with_prelude(policy);
        let policy_filter = PolicyFilter::new(format!("{}_decision_policy", name), decision_policy);
        let context_inlet = policy_filter.context_inlet();
        let tx_policy_api = policy_filter.tx_api();
        let tx_policy_monitor = policy_filter.tx_monitor.clone();

        let graph_inlet = policy_filter.inlet();
        (policy_filter.outlet(), transform.inlet()).connect().await;
        let graph_outlet = transform.outlet();

        let mut graph = Graph::default();
        graph.push_back(Box::new(policy_filter)).await;
        graph.push_back(Box::new(transform)).await;

        let inner_stage =
            stage::CompositeThrough::new(format!("{}_inner", name), graph, graph_inlet, graph_outlet).await;

        let inner_inlet = inner_stage.inlet();
        let inner_outlet = inner_stage.outlet();

        Self {
            name,
            inner_policy_transform: Box::new(inner_stage),
            context_inlet,
            inlet: inner_inlet,
            outlet: inner_outlet,
            tx_policy_api,
            tx_policy_monitor,
        }
    }

    #[inline]
    pub fn context_inlet(&self) -> Inlet<C> {
        self.context_inlet.clone()
    }

    fn policy_with_prelude(
        policy: impl QueryPolicy<Item = In, Context = C, Args = (In, C, PolarValue)> + 'static,
    ) -> impl QueryPolicy<Item = In, Context = C, Args = (In, C, PolarValue)> {
        DecisionQueryPolicy(policy)
    }
}

struct DecisionQueryPolicy<T, C, P>(P)
where
    P: QueryPolicy<Item = T, Context = C, Args = (T, C, PolarValue)>;

impl<T, C, P> Debug for DecisionQueryPolicy<T, C, P>
where
    P: QueryPolicy<Item = T, Context = C, Args = (T, C, PolarValue)>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DecisionQueryPolicy").field("inner", &self.0).finish()
    }
}

impl<T, C, P> QueryPolicy for DecisionQueryPolicy<T, C, P>
where
    T: ToPolar + Clone,
    C: ToPolar + Clone,
    P: QueryPolicy<Item = T, Context = C, Args = (T, C, PolarValue)>,
{
    type Item = T;
    type Context = C;
    type Args = (T, C, PolarValue);

    #[inline]
    fn load_policy_engine(&self, engine: &mut Oso) -> Result<(), PolicyError> {
        engine.load_str(
            r#"scale(item, context, direction) if scale_up(item, context, direction) and direction = "up";
            scale(item, context, direction) if scale_down(item, context, direction) and direction = "down";"#,
        )?;
        self.0.load_policy_engine(engine)
    }

    #[inline]
    fn initialize_policy_engine(&mut self, engine: &mut Oso) -> Result<(), PolicyError> {
        self.0.initialize_policy_engine(engine)
    }

    #[inline]
    fn make_query_args(&self, item: &Self::Item, context: &Self::Context) -> Self::Args {
        self.0.make_query_args(item, context)
    }

    #[inline]
    fn query_policy(&self, engine: &Oso, args: Self::Args) -> Result<QueryResult, PolicyError> {
        self.0.query_policy(engine, args)
    }
}

impl<In, Out, C: Debug> Debug for Decision<In, Out, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Decision")
            .field("name", &self.name)
            .field("inner_stage", &self.inner_policy_transform)
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
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run decision phase", skip(self))]
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
impl<In: AppData, Out: AppData, C: ProctorContext> Decision<In, Out, C> {
    #[inline]
    async fn do_check(&self) -> Result<(), DecisionError> {
        self.inlet.check_attachment().await?;
        self.context_inlet.check_attachment().await?;
        self.inner_policy_transform
            .check()
            .await
            .map_err(|err| DecisionError::StageError(err.into()))?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    #[inline]
    async fn do_run(&mut self) -> Result<(), DecisionError> {
        self.inner_policy_transform
            .run()
            .await
            .map_err(|err| DecisionError::StageError(err.into()))?;
        Ok(())
    }

    #[inline]
    async fn do_close(mut self: Box<Self>) -> Result<(), DecisionError> {
        tracing::trace!("closing decision ports.");
        self.inlet.close().await;
        self.context_inlet.close().await;
        self.outlet.close().await;
        self.inner_policy_transform
            .close()
            .await
            .map_err(|err| DecisionError::StageError(err.into()))?;
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
