use super::ProctorContext;
use crate::elements::telemetry;
use crate::elements::{PolicySettings, PolicySource, Telemetry, TelemetryValue};
use crate::error::GraphError;
use crate::graph::GraphResult;
use crate::phases::collection::TelemetrySubscription;
use crate::AppData;
use oso::{Oso, PolarClass, Query, ResultSet, ToPolar, ToPolarList};
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::fmt::{self, Debug};
use std::marker::PhantomData;

pub fn make_item_context_policy<I, C, S, FI>(
    query_target: S, settings: &impl PolicySettings, initialize_engine: FI,
) -> impl Policy<I, C>
where
    I: AppData + ToPolar,
    C: ProctorContext,
    S: Into<String>,
    FI: FnOnce(&mut Oso) -> GraphResult<()> + Send + Sync,
{
    AssembledPolicy::new(query_target, settings, initialize_engine)
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

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub bindings: Option<telemetry::Table>,
}

impl QueryResult {
    pub fn from_query(mut query: Query) -> GraphResult<Self> {
        fn fill_results(mut results: telemetry::Table, result_set: &ResultSet) -> GraphResult<telemetry::Table> {
            for key in result_set.keys() {
                match result_set.get_typed(key)? {
                    TelemetryValue::Unit => (),
                    value => {
                        let _ = results.insert(key.to_string(), value);
                    }
                }
            }
            Ok(results)
        }

        let bindings = if let Some(rs) = query.next() {
            let mut result_bindings = fill_results(HashMap::new(), &rs?)?;
            for rs in query {
                result_bindings = fill_results(result_bindings, &rs?)?;
            }
            Some(result_bindings)
        } else {
            None
        };

        Ok(Self { bindings })
    }

    pub fn take_bindings(&mut self) -> Option<telemetry::Table> {
        self.bindings.take()
    }

    pub fn get_typed<T: TryFrom<TelemetryValue>>(&self, key: &str) -> GraphResult<T>
    where
        T: TryFrom<TelemetryValue>,
        <T as TryFrom<TelemetryValue>>::Error: Into<crate::error::GraphError>,
    {
        if let Some(ref inner) = self.bindings {
            let value = inner.get(key).ok_or(GraphError::GraphPrecondition(format!(
                "no policy binding found for key, {}",
                key
            )))?;

            T::try_from(value.clone()).map_err(|err| err.into())
        } else {
            Err(GraphError::GraphPrecondition(
                "no policy bindings for empty result".to_string(),
            ))
        }
    }

    #[inline]
    pub fn is_some(&self) -> bool {
        self.bindings.is_some()
    }

    #[inline]
    pub fn is_none(&self) -> bool {
        self.bindings.is_none()
    }
}

impl std::ops::Deref for QueryResult {
    type Target = telemetry::Table;

    fn deref(&self) -> &Self::Target {
        if let Some(ref inner) = self.bindings {
            &*inner
        } else {
            panic!("no bindings for empty result")
        }
    }
}

pub trait QueryPolicy: Debug + Send + Sync {
    type Args: ToPolarList;
    fn load_policy_engine(&self, engine: &mut oso::Oso) -> GraphResult<()>;
    fn initialize_policy_engine(&mut self, engine: &mut oso::Oso) -> GraphResult<()>;
    fn query_policy(&self, engine: &oso::Oso, args: Self::Args) -> GraphResult<QueryResult>;
}

pub struct AssembledPolicy<I, C, FI>
where
    FI: FnOnce(&mut Oso) -> GraphResult<()>,
{
    required_subscription_fields: HashSet<String>,
    optional_subscription_fields: HashSet<String>,
    source: PolicySource,
    query: String,
    initialize_engine: Option<FI>,
    item_marker: PhantomData<I>,
    context_marker: PhantomData<C>,
}

impl<I, C, FI> AssembledPolicy<I, C, FI>
where
    FI: FnOnce(&mut Oso) -> GraphResult<()>,
{
    pub fn new<S: Into<String>>(query: S, settings: &impl PolicySettings, initialize_engine: FI) -> Self {
        Self {
            required_subscription_fields: settings.required_subscription_fields(),
            optional_subscription_fields: settings.optional_subscription_fields(),
            source: settings.source(),
            query: query.into(),
            initialize_engine: Some(initialize_engine),
            item_marker: PhantomData,
            context_marker: PhantomData,
        }
    }
}

impl<I, C, FI> Debug for AssembledPolicy<I, C, FI>
where
    FI: FnOnce(&mut Oso) -> GraphResult<()>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AssembledPolicy")
            .field("required", &self.required_subscription_fields)
            .field("optional", &self.optional_subscription_fields)
            .field("source", &self.source)
            .field("query_target", &self.query)
            .finish()
    }
}

impl<I, C, FI> PolicySubscription for AssembledPolicy<I, C, FI>
where
    C: ProctorContext,
    FI: FnOnce(&mut Oso) -> GraphResult<()>,
{
    type Context = C;

    fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
        subscription
            .with_required_fields(self.required_subscription_fields.clone())
            .with_optional_fields(self.optional_subscription_fields.clone())
    }
}

impl<I, C, FI> QueryPolicy for AssembledPolicy<I, C, FI>
where
    I: AppData + ToPolar,
    C: ProctorContext,
    FI: FnOnce(&mut Oso) -> GraphResult<()> + Send + Sync,
{
    type Args = (I, C);

    fn load_policy_engine(&self, engine: &mut Oso) -> GraphResult<()> {
        self.source.load_into(engine)
    }

    #[tracing::instrument(level = "info", skip(engine))]
    fn initialize_policy_engine(&mut self, engine: &mut Oso) -> GraphResult<()> {
        if let Some(init) = self.initialize_engine.take() {
            tracing::info!("initializing policy engine...");
            engine.register_class(Telemetry::get_polar_class())?;
            init(engine)
        } else {
            tracing::info!("skipping - no remaining policy engine initialization required.");
            Ok(())
        }
    }

    fn query_policy(&self, engine: &Oso, args: Self::Args) -> GraphResult<QueryResult> {
        let q = engine.query_rule(self.query.as_str(), args)?;
        QueryResult::from_query(q)
    }
}
