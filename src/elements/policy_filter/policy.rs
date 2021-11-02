use super::QueryResult;
use crate::elements::{PolicySource, PolicySourcePath};
use crate::error::PolicyError;
use crate::phases::collection::{SubscriptionRequirements, TelemetrySubscription};
use either::Either;
use either::Either::{Left, Right};
use oso::{ToPolar, ToPolarList};
use serde::Serialize;
use std::fmt::Debug;
use std::io::Write;
use std::path::PathBuf;

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

        let subscription = TelemetrySubscription::new(name).for_requirements::<Self::Requirements>();
        let subscription = self.do_extend_subscription(subscription);
        tracing::trace!("subscription after extension: {:?}", subscription);
        subscription
    }

    fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
        subscription
    }
}

pub type PolicyRegistry<'h> = handlebars::Handlebars<'h>;

pub trait QueryPolicy: Debug + Send + Sync {
    type Item: ToPolar + Clone;
    type Context: ToPolar + Clone;
    type Args: ToPolarList;
    type TemplateData: Serialize + Debug;

    #[tracing::instrument(level = "info", skip(engine))]
    fn load_policy_engine(&mut self, engine: &mut oso::Oso) -> Result<(), PolicyError> {
        engine.clear_rules()?;
        let (templates, complete) = self.sources().iter().partition::<Vec<_>, _>(|s| s.is_template());
        tracing::info!(?complete, ?templates, "loading complete and template sources...");
        let mut source_paths = Vec::with_capacity(complete.len() + 1);
        for s in complete {
            let policy = match s {
                PolicySource::File { path, is_template: _ } => Left(path.clone()),
                PolicySource::String { name: _, polar, is_template: _ } => Right(polar.as_str()),
            };
            source_paths.push(policy_source_path_for(s.name().as_ref(), policy)?);
            tracing::debug!("loaded policy source: {:?}", s);
        }

        if !templates.is_empty() {
            let name = Self::base_template_name();
            let data = self.policy_template_data();
            tracing::info!(template_name=%name, ?data, "rendering template from sources...");
            let template_policy = render_template_policy(templates, name, data)?;
            tracing::debug!(template_name=%name, %template_policy, "rendered template policy.");
            let template_source_path = policy_source_path_for(name, Right(template_policy.as_str()))?;
            source_paths.push(template_source_path);
            tracing::debug!(template_name=%name, "loaded template policy.");
        }

        engine.load_files(source_paths)?;
        Ok(())
    }

    fn base_template_name() -> &'static str;

    fn policy_template_data(&self) -> Option<&Self::TemplateData>;
    fn policy_template_data_mut(&mut self) -> Option<&mut Self::TemplateData>;

    fn sources(&self) -> &[PolicySource];
    fn sources_mut(&mut self) -> &mut Vec<PolicySource>;

    fn initialize_policy_engine(&mut self, engine: &mut oso::Oso) -> Result<(), PolicyError>;
    fn make_query_args(&self, item: &Self::Item, context: &Self::Context) -> Self::Args;
    fn query_policy(&self, engine: &oso::Oso, args: Self::Args) -> Result<QueryResult, PolicyError>;
}

#[tracing::instrument(level = "info")]
fn render_template_policy<T>(templates: Vec<&PolicySource>, name: &str, data: Option<&T>) -> Result<String, PolicyError>
where
    T: Serialize + Debug,
{
    // template_data
    //     .map(|data| {
    tracing::info!("rendering policy string as template with data.");

    // I tried to facilitate registry caching in policy, but handlebars' lifetime parameter
    // (underlying the PolicyRegistry) hampers the ergonomics of policy definition.
    // Not a performance impact since policy loading only happens on bootstrap or during
    // a corresponding, intermittent command.
    let mut registry = PolicyRegistry::new();
    for s in templates {
        let policy_template: String = s.try_into()?;
        registry.register_template_string(s.name().as_ref(), policy_template)?;
    }
    tracing::debug!(?registry, "policy templates registered with handlebars registry");
    let policy = registry.render(name, &data)?;
    tracing::info!(rendered_policy=%policy, "rendered {} policy from template and data.", name);
    Ok(policy)
    // })
    // .unwrap_or_else(|| {
    //     tracing::info!("no template data supplied -- assuming policy string is not a template.");
    //
    //     self.sources()
    //         .iter()
    //         .find(|s| s.name().as_ref() == template_name)
    //         .map(|s| s.try_into())
    //         .unwrap_or(Err(PolicyError::StringPolicyError(format!(
    //             "failed to find policy template: {}",
    //             template_name
    //         ))))
    // })
}

#[tracing::instrument(level = "info", skip())]
fn policy_source_path_for(name: &str, policy: Either<PathBuf, &str>) -> Result<PolicySourcePath, PolicyError> {
    match policy {
        Either::Left(path) => Ok(PolicySourcePath::File(path)),
        Either::Right(rep) => {
            let tempdir = std::env::current_dir()?;

            let mut tmp = tempfile::Builder::new()
                .prefix(format!("policy_{}", name).as_str())
                .rand_bytes(4)
                .suffix(".polar")
                .tempfile_in(tempdir)?;

            tracing::info!("rendered {} policy file for loading: {:?}", name, tmp);

            write!(tmp.as_file_mut(), "{}", rep)?;

            Ok(PolicySourcePath::String(tmp))
        }
    }
}
