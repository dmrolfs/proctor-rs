use super::ProctorContext;
use crate::elements::{PolicySettings, PolicySource};
use crate::graph::GraphResult;
use crate::phases::collection::TelemetrySubscription;
use crate::AppData;
use oso::{Oso, Query, ToPolar, ToPolarList};
use std::collections::HashSet;
use std::fmt::{self, Debug};
use std::marker::PhantomData;

pub fn make_item_context_policy<I, C, Q, S, FI, FQ>(
    query_target: S, settings: &impl PolicySettings, initialize_engine: FI, eval_query_result: FQ,
) -> impl Policy<I, C>
where
    I: AppData + ToPolar,
    C: ProctorContext,
    S: Into<String>,
    FI: FnOnce(&mut Oso) -> GraphResult<()> + Send + Sync,
    FQ: Fn(Query) -> GraphResult<Q> + Send + Sync,
{
    AssembledPolicy::new(query_target, settings, initialize_engine, eval_query_result)
}

pub trait Policy<I, C>: PolicySubscription<Context = C> + QueryPolicy<Args = (I, C)> {}
impl<P, I, C> Policy<I, C> for P where P: PolicySubscription<Context = C> + QueryPolicy<Args = (I, C)> {}

pub trait PolicySubscription {
    type Context: ProctorContext;

    fn subscription(&self, name: &str) -> TelemetrySubscription {
        tracing::trace!(
            "context required_fields:{:?}, optional_fields:{:?}",
            Self::Context::required_context_fields(),
            Self::Context::optional_context_fields(),
        );

        let subscription = TelemetrySubscription::new(name)
            .with_required_fields(Self::Context::required_context_fields())
            .with_optional_fields(Self::Context::optional_context_fields());
        let subscription = self.do_extend_subscription(subscription);
        tracing::trace!("subscription after extension: {:?}", subscription);
        subscription
    }

    fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
        subscription
    }
}

pub trait QueryPolicy: Debug + Send + Sync {
    type Args: ToPolarList;
    fn load_policy_engine(&self, engine: &mut oso::Oso) -> GraphResult<()>;
    fn initialize_policy_engine(&mut self, engine: &mut oso::Oso) -> GraphResult<()>;

    type QueryResult;
    fn query_policy(&self, engine: &oso::Oso, args: Self::Args) -> GraphResult<Self::QueryResult>;
}

pub struct AssembledPolicy<I, C, Q, FI, FQ>
where
    FI: FnOnce(&mut Oso) -> GraphResult<()>,
    FQ: Fn(Query) -> GraphResult<Q>,
{
    required_subscription_fields: HashSet<String>,
    optional_subscription_fields: HashSet<String>,
    source: PolicySource,
    query_target: String,
    initialize_engine: Option<FI>,
    eval_query_result: FQ,
    item_marker: PhantomData<I>,
    context_marker: PhantomData<C>,
}

impl<I, C, Q, FI, FQ> AssembledPolicy<I, C, Q, FI, FQ>
where
    FI: FnOnce(&mut Oso) -> GraphResult<()>,
    FQ: Fn(Query) -> GraphResult<Q>,
{
    pub fn new<S: Into<String>>(
        query_target: S, settings: &impl PolicySettings, initialize_engine: FI, eval_query_result: FQ,
    ) -> Self {
        Self {
            required_subscription_fields: settings.required_subscription_fields(),
            optional_subscription_fields: settings.optional_subscription_fields(),
            source: settings.source(),
            query_target: query_target.into(),
            initialize_engine: Some(initialize_engine),
            eval_query_result,
            item_marker: PhantomData,
            context_marker: PhantomData,
        }
    }
}

impl<I, C, Q, FI, FQ> Debug for AssembledPolicy<I, C, Q, FI, FQ>
where
    FI: FnOnce(&mut Oso) -> GraphResult<()>,
    FQ: Fn(Query) -> GraphResult<Q>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AssembledPolicy")
            .field("required", &self.required_subscription_fields)
            .field("optional", &self.optional_subscription_fields)
            .field("source", &self.source)
            .field("query_target", &self.query_target)
            .finish()
    }
}

impl<I, C, Q, FI, FQ> PolicySubscription for AssembledPolicy<I, C, Q, FI, FQ>
where
    C: ProctorContext,
    FI: FnOnce(&mut Oso) -> GraphResult<()>,
    FQ: Fn(Query) -> GraphResult<Q>,
{
    type Context = C;

    fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
        subscription
            .with_required_fields(self.required_subscription_fields.clone())
            .with_optional_fields(self.optional_subscription_fields.clone())
    }
}

impl<I, C, Q, FI, FQ> QueryPolicy for AssembledPolicy<I, C, Q, FI, FQ>
where
    I: AppData + ToPolar,
    C: ProctorContext,
    FI: FnOnce(&mut Oso) -> GraphResult<()> + Send + Sync,
    FQ: Fn(Query) -> GraphResult<Q> + Send + Sync,
{
    type Args = (I, C);

    fn load_policy_engine(&self, engine: &mut Oso) -> GraphResult<()> {
        self.source.load_into(engine)
    }

    #[tracing::instrument(level = "info", skip(engine))]
    fn initialize_policy_engine(&mut self, engine: &mut Oso) -> GraphResult<()> {
        if let Some(init) = self.initialize_engine.take() {
            tracing::info!("initializing policy engine...");
            init(engine)
        } else {
            tracing::info!("skipping - no remaining policy engine initialization required.");
            Ok(())
        }
    }

    type QueryResult = Q;

    fn query_policy(&self, engine: &Oso, args: Self::Args) -> GraphResult<Self::QueryResult> {
        let q = engine.query_rule(self.query_target.as_str(), args)?;
        (self.eval_query_result)(q)
    }
}
