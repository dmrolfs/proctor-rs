use super::ProctorContext;
use crate::elements::telemetry;
use crate::elements::{PolicySettings, PolicySource, Telemetry, TelemetryValue};
use crate::error::GraphError;
use crate::graph::GraphResult;
use crate::phases::collection::TelemetrySubscription;
use oso::{Oso, PolarClass, PolarValue, Query, ResultSet, ToPolar, ToPolarList};
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::fmt::{self, Debug};
use std::marker::PhantomData;

pub trait Policy<T, C, A>: PolicySubscription<Context = C> + QueryPolicy<Item = T, Context = C, Args = A> {}

impl<P, T, C, A> Policy<T, C, A> for P where
    P: PolicySubscription<Context = C> + QueryPolicy<Item = T, Context = C, Args = A>
{
}

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
    pub fn is_success(&self) -> bool {
        self.is_some()
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
    type Item: ToPolar + Clone;
    type Context: ToPolar + Clone;
    type Args: ToPolarList;

    fn load_policy_engine(&self, engine: &mut oso::Oso) -> GraphResult<()>;
    fn initialize_policy_engine(&mut self, engine: &mut oso::Oso) -> GraphResult<()>;
    fn make_query_args(&self, item: &Self::Item, context: &Self::Context) -> Self::Args;
    fn query_policy(&self, engine: &oso::Oso, args: Self::Args) -> GraphResult<QueryResult>;
}

//todo: look into supprting a basic implementation; this attempt was side-tracked by the need to create
// create a ToPolarList-supported Args that can support options beyond (Item, Context).
// Vec<PolarValue> does implement ToPolarList? (I can fix that w a pull request), and creating a tuple
// without a macro isn't clear. The limited value may not be enough for the twists and turns.
//
// pub fn make_item_context_policy<T, C, S, F>(
//     query_target: S, settings: &impl PolicySettings, initialize_engine: F,
// ) -> impl Policy<T, C>
// where
//     T: ToPolar + Send + Sync,
//     C: ProctorContext,
//     S: Into<String>,
//     F: FnOnce(&mut Oso) -> GraphResult<()> + Send + Sync,
// {
//     AssembledPolicy::new(query_target, settings, initialize_engine)
// }
//
// pub struct AssembledPolicy<T, C, A, F>
// where
//     F: FnOnce(&mut Oso) -> GraphResult<()>,
// {
//     required_subscription_fields: HashSet<String>,
//     optional_subscription_fields: HashSet<String>,
//     source: PolicySource,
//     query: String,
//     extra_args: Vec<PolarValue>,
//     initialize_engine: Option<F>,
//     item_marker: PhantomData<T>,
//     context_marker: PhantomData<C>,
//     args_marker: PhantomData<A>,
// }
//
// impl<T, C, A, F> AssembledPolicy<T, C, A, F>
// where
//     F: FnOnce(&mut Oso) -> GraphResult<()>,
// {
//     pub fn new<S: Into<String>>(query: S, settings: &impl PolicySettings, initialize_engine: F) -> Self {
//         Self::with_extra_query_args(query, settings, Vec::default(), initialize_engine)
//     }
//
//     pub fn with_extra_query_args<S: Into<String>>(query: S, settings: &impl PolicySettings, extra_args: Vec<PolarValue>, initialize_engine: F) -> Self {
//         Self {
//             required_subscription_fields: settings.required_subscription_fields(),
//             optional_subscription_fields: settings.optional_subscription_fields(),
//             source: settings.source(),
//             query: query.into(),
//             extra_args,
//             initialize_engine: Some(initialize_engine),
//             item_marker: PhantomData,
//             context_marker: PhantomData,
//             args_marker: PhantomData,
//         }
//     }
// }
//
// impl<T, C, A, F> Debug for AssembledPolicy<T, C, A, F>
// where
//     F: FnOnce(&mut Oso) -> GraphResult<()>,
// {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         f.debug_struct("AssembledPolicy")
//             .field("required", &self.required_subscription_fields)
//             .field("optional", &self.optional_subscription_fields)
//             .field("source", &self.source)
//             .field("query", &self.query)
//             .field("extra_args", &self.extra_args)
//             .finish()
//     }
// }
//
// impl<T, C, A, F> PolicySubscription for AssembledPolicy<T, C, A, F>
// where
//     C: ProctorContext,
//     F: FnOnce(&mut Oso) -> GraphResult<()>,
// {
//     type Context = C;
//
//     fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
//         subscription
//             .with_required_fields(self.required_subscription_fields.clone())
//             .with_optional_fields(self.optional_subscription_fields.clone())
//     }
// }
//
// impl<T, C, A, F> QueryPolicy for AssembledPolicy<T, C, A, F>
// where
//     T: ToPolar + Send + Sync,
//     C: ProctorContext,
//     A: ToPolarList,
//     F: FnOnce(&mut Oso) -> GraphResult<()> + Send + Sync,
// {
//     type Item = T;
//     type Context = C;
//     type Args = A;
//
//     fn load_policy_engine(&self, engine: &mut Oso) -> GraphResult<()> {
//         self.source.load_into(engine)
//     }
//
//     #[tracing::instrument(level = "info", skip(engine))]
//     fn initialize_policy_engine(&mut self, engine: &mut Oso) -> GraphResult<()> {
//         if let Some(init) = self.initialize_engine.take() {
//             tracing::info!("initializing policy engine...");
//             engine.register_class(Telemetry::get_polar_class())?;
//             init(engine)
//         } else {
//             tracing::info!("skipping - no remaining policy engine initialization required.");
//             Ok(())
//         }
//     }
//
//     fn make_query_args(&self, item: &Self::Item, context: &Self::Context) -> Self::Args {
//         let mut args = vec![item.to_polar(), context.to_polar()];
//         args.extend(self.extra_args.iter().cloned());
//         args
//     }
//
//     fn query_policy(&self, engine: &Oso, item: &Self::Item, context: &Self::Context) -> GraphResult<QueryResult> {
//         let q = engine.query_rule(self.query.as_str(), self.make_query_args(item, context))?;
//         QueryResult::from_query(q)
//     }
// }
