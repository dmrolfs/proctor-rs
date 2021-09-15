use super::QueryResult;
use crate::elements::PolicySource;
use crate::error::PolicyError;
use crate::phases::collection::{SubscriptionRequirements, TelemetrySubscription};
use oso::{ToPolar, ToPolarList};
use std::fmt::Debug;

pub trait Policy<T, C, A>: PolicySubscription<Requirements = C> + QueryPolicy<Item = T, Context = C, Args = A> {}

impl<P, T, C, A> Policy<T, C, A> for P where
    P: PolicySubscription<Requirements = C> + QueryPolicy<Item = T, Context = C, Args = A>
{
}

pub trait PolicySubscription {
    type Requirements: SubscriptionRequirements;

    fn subscription(&self, name: &str) -> TelemetrySubscription {
        tracing::trace!(
            "policy required_fields:{:?}, optional_fields:{:?}",
            Self::Requirements::required_fields(),
            Self::Requirements::optional_fields(),
        );

        let subscription = TelemetrySubscription::new(name).with_requirements::<Self::Requirements>();
        let subscription = self.do_extend_subscription(subscription);
        tracing::trace!("subscription after extension: {:?}", subscription);
        subscription
    }

    fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
        subscription
    }
}

pub trait QueryPolicy: Debug + Send + Sync {
    type Item: ToPolar + Clone;
    type Context: ToPolar + Clone;
    type Args: ToPolarList;

    #[tracing::instrument(level = "info", skip(engine))]
    fn load_policy_engine(&self, engine: &mut oso::Oso) -> Result<(), PolicyError> {
        engine.clear_rules()?;
        let files = self.policy_sources().iter().map(|ps| ps.source_path()).collect();
        tracing::info!(policy_sources=?files, "loading sources into policy engine...");
        engine.load_files(files)?;
        Ok(())
    }

    fn policy_sources(&self) -> Vec<PolicySource>;
    fn replace_sources(&mut self, sources: Vec<PolicySource>);

    fn initialize_policy_engine(&mut self, engine: &mut oso::Oso) -> Result<(), PolicyError>;
    fn make_query_args(&self, item: &Self::Item, context: &Self::Context) -> Self::Args;
    fn query_policy(&self, engine: &oso::Oso, args: Self::Args) -> Result<QueryResult, PolicyError>;
}

// todo: look into supprting a basic implementation; this attempt was side-tracked by the need to
// create create a ToPolarList-supported Args that can support options beyond (Item, Context).
// Vec<PolarValue> does implement ToPolarList? (I can fix that w a pull request), and creating a
// tuple without a macro isn't clear. The limited value may not be enough for the twists and turns.
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
//     pub fn new<S: Into<String>>(query: S, settings: &impl PolicySettings, initialize_engine: F)
// -> Self {         Self::with_extra_query_args(query, settings, Vec::default(), initialize_engine)
//     }
//
//     pub fn with_extra_query_args<S: Into<String>>(query: S, settings: &impl PolicySettings,
// extra_args: Vec<PolarValue>, initialize_engine: F) -> Self {         Self {
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
//     fn do_extend_subscription(&self, subscription: TelemetrySubscription) ->
// TelemetrySubscription {         subscription
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
//     fn query_policy(&self, engine: &Oso, item: &Self::Item, context: &Self::Context) ->
// GraphResult<QueryResult> {         let q = engine.query_rule(self.query.as_str(),
// self.make_query_args(item, context))?;         QueryResult::from_query(q)
//     }
// }
