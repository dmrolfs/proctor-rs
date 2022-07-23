use std::fmt::Debug;
use std::io::Write;
use std::path::PathBuf;

use either::Either::{self, Left, Right};
use once_cell::sync::Lazy;
use oso::{ToPolar, ToPolarList};
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::{PolicySettings, QueryResult};
use crate::elements::{PolicySource, PolicySourcePath};
use crate::error::PolicyError;
use crate::phases::sense::{SubscriptionRequirements, TelemetrySubscription};

pub trait Policy<T, C, A>: PolicySubscription<Requirements = C> + QueryPolicy<Item = T, Context = C, Args = A> {}

impl<P, T, C, A> Policy<T, C, A> for P where
    P: PolicySubscription<Requirements = C> + QueryPolicy<Item = T, Context = C, Args = A>
{
}

pub trait PolicySubscription: QueryPolicy {
    type Requirements: SubscriptionRequirements;
    // todo: once stable: type TemplateData = <Self as QueryPolicy>::TemplateData;

    fn subscription(
        &self, name: &str, settings: &PolicySettings<<Self as QueryPolicy>::TemplateData>,
    ) -> TelemetrySubscription {
        tracing::trace!(
            "policy required_fields:{:?}, optional_fields:{:?}",
            Self::Requirements::required_fields(),
            Self::Requirements::optional_fields(),
        );

        let subscription = TelemetrySubscription::new(name)
            .for_requirements::<Self::Requirements>()
            .with_required_fields(settings.required_subscription_fields.clone())
            .with_optional_fields(settings.optional_subscription_fields.clone());

        let subscription = self.do_extend_subscription(subscription);

        tracing::trace!("subscription after extension: {:?}", subscription);
        subscription
    }

    fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
        subscription
    }
}

pub type PolicyRegistry<'h> = handlebars::Handlebars<'h>;

pub trait PolicyContributor {
    fn register_with_policy_engine(engine: &mut oso::Oso) -> Result<(), PolicyError>;
}

pub trait QueryPolicy: Debug + Send + Sync {
    type Item: ToPolar + Clone;
    type Context: ToPolar + Clone;
    type Args: ToPolarList;
    type TemplateData: Debug + Serialize + DeserializeOwned;

    fn zero_context(&self) -> Option<Self::Context> {
        None
    }

    #[tracing::instrument(level = "debug", skip(engine))]
    fn load_policy_engine(&self, engine: &mut oso::Oso) -> Result<(), PolicyError> {
        engine.clear_rules()?;
        let source_paths = self.render_policy_sources()?;
        engine.load_files(source_paths)?;
        Ok(())
    }

    #[tracing::instrument(level = "debug")]
    fn render_policy_sources(&self) -> Result<Vec<PolicySourcePath>, PolicyError> {
        let (templates, complete) = self.sources().iter().partition::<Vec<_>, _>(|s| s.is_template());
        tracing::debug!(?complete, ?templates, "loading complete and template sources...");
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
            tracing::debug!(template_name=%name, ?data, "rendering template from sources...");
            let template_policy = render_template_policy(templates, name, data)?;
            tracing::debug!(template_name=%name, %template_policy, "rendered template policy.");
            let template_source_path = policy_source_path_for(name, Right(template_policy.as_str()))?;
            source_paths.push(template_source_path);
            tracing::debug!(template_name=%name, policy=%template_policy, "loaded template policy.");
        }

        Ok(source_paths)
    }

    fn base_template_name() -> &'static str;

    fn policy_template_data(&self) -> Option<&Self::TemplateData>;
    fn policy_template_data_mut(&mut self) -> Option<&mut Self::TemplateData>;

    fn sources(&self) -> &[PolicySource];
    fn sources_mut(&mut self) -> &mut Vec<PolicySource>;

    fn initialize_policy_engine(&self, engine: &mut oso::Oso) -> Result<(), PolicyError>;
    fn make_query_args(&self, item: &Self::Item, context: &Self::Context) -> Self::Args;
    fn query_policy(&self, engine: &oso::Oso, args: Self::Args) -> Result<QueryResult, PolicyError>;
}

#[tracing::instrument(level = "trace", skip(templates, name), fields(template_name=name))]
pub fn render_template_policy<'a, T, D>(templates: T, name: &str, data: Option<&D>) -> Result<String, PolicyError>
where
    T: IntoIterator<Item = &'a PolicySource>,
    D: Serialize + Debug,
{
    tracing::trace!("rendering policy string as template with data.");

    // I tried to facilitate registry caching in policy, but handlebars' lifetime parameter
    // (underlying the PolicyRegistry) hampers the ergonomics of policy definition.
    // Not a performance impact since policy loading only happens on bootstrap or during
    // a corresponding, intermittent command.
    let mut registry = PolicyRegistry::new();
    for s in templates {
        let policy_template: String = s.try_into()?;
        registry.register_template_string(s.name().as_ref(), policy_template)?;
    }
    tracing::trace!(?registry, "policy templates registered with handlebars registry");
    let policy = registry.render(name, &data)?;
    tracing::info!(rendered_policy=%policy, "rendered {} policy from template and data.", name);
    Ok(policy)
}

static APP_TEMPDIR: Lazy<tempfile::TempDir> = Lazy::new(|| {
    let current_exe = std::env::current_exe().unwrap_or_else(|_| "proctor".into());
    let app_name: std::ffi::OsString = current_exe
        .file_stem()
        .map(|name| {
            let mut n: std::ffi::OsString = name.into();
            n.push("_");
            n
        })
        .unwrap_or_else(|| "proctor_".into());

    tempfile::Builder::new()
        .prefix(app_name.as_os_str())
        .tempdir()
        .unwrap_or_else(|err| {
            panic!(
                "failed to create {app_name:?} temp dir under {:?}: {:?}",
                std::env::temp_dir(),
                err
            )
        })
});

#[tracing::instrument(level = "trace", skip(name), fields(policy_name=name))]
fn policy_source_path_for(name: &str, policy: Either<PathBuf, &str>) -> Result<PolicySourcePath, PolicyError> {
    match policy {
        Left(path) => Ok(PolicySourcePath::File(path)),
        Right(rep) => {
            let mut tmp = tempfile::Builder::new()
                .prefix(format!("policy_{}_", name).as_str())
                .rand_bytes(4)
                .suffix(".polar")
                .tempfile_in(APP_TEMPDIR.path())?;

            tracing::trace!("rendered {} policy file for loading at {:?}", name, tmp);

            write!(tmp.as_file_mut(), "{}", rep)?;

            Ok(PolicySourcePath::String(tmp))
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use claim::*;
    use pretty_assertions::assert_eq;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use trim_margin::MarginTrimmable;

    static TEMPLATES: Lazy<Vec<PolicySource>> = Lazy::new(|| {
        let first = r##"|
            |# define eligibile rule in eligibility polar basis file.
            |
            |deploying(_, context) if context.cluster.is_deploying;
            |
            |{{#if cooling_secs}}
            |in_cooling_period(_, context) if context.cluster.last_deployment_within_seconds({{cooling_secs}});
            |{{else}}
            |in_cooling_period(_, _) if false;
            |{{/if}}
            |
            |{{#if stable_secs}}
            |recent_failure(_, context) if context.job.last_failure_within_seconds({{stable_secs}});
            |{{else}}
            |recent_failure(_, _) if false;
            |{{/if}}
            |
            |# Do not scale during multi region failure
            |# Are there job failures
            |# Do not scale while there are current operations; e.g., Cancel, Upgrade, MultiRegion Failover.
            |# license considerations; e.g., Do not autoscale freemium pipelines
            |
            |{{> (lookup this "basis")}}
            |"##;

        let second = r##"|
            |eligible(item, context, reason) if not ineligible(item, context, root_reason) and reason = root_reason;
            |
            |ineligible(item, _context, reason) if item.cluster.nr_active_jobs == 0 and reason = "no_active_jobs";
            |ineligible(_item, context, reason) if context.cluster.is_rescaling and reason = "rescaling";
            |ineligible(item, context, reason) if deploying(item, context) and reason = "deploying";
            |ineligible(item, context, reason) if recent_failure(item, context) and reason = "recent_failure";
            |ineligible(item, context, reason) if in_cooling_period(item, context) and reason = "cooling_period";
            |"##;

        vec![
            assert_ok!(PolicySource::from_template_string("common", first)),
            assert_ok!(PolicySource::from_template_string("common_basis", second)),
        ]
    });

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(default)]
    pub struct TemplateData {
        pub basis: String,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub cooling_secs: Option<u32>,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub stable_secs: Option<u32>,

        #[serde(flatten, skip_serializing_if = "HashMap::is_empty")]
        pub custom: HashMap<String, String>,
    }

    impl Default for TemplateData {
        fn default() -> Self {
            Self {
                basis: "common_basis".to_string(),
                cooling_secs: None,
                stable_secs: None,
                custom: HashMap::default(),
            }
        }
    }

    #[test]
    fn test_render_with_template_data() {
        Lazy::force(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_render_with_template_data");
        let _main_span_guard = main_span.enter();

        let data = TemplateData {
            cooling_secs: Some(300),
            stable_secs: Some(900),
            ..TemplateData::default()
        };

        let templates = TEMPLATES.clone();
        let actual = assert_ok!(render_template_policy(&templates, "common", Some(data).as_ref()));
        assert_eq!(
            actual,
            r##"|
            |# define eligibile rule in eligibility polar basis file.
            |
            |deploying(_, context) if context.cluster.is_deploying;
            |
            |in_cooling_period(_, context) if context.cluster.last_deployment_within_seconds(300);
            |
            |recent_failure(_, context) if context.job.last_failure_within_seconds(900);
            |
            |# Do not scale during multi region failure
            |# Are there job failures
            |# Do not scale while there are current operations; e.g., Cancel, Upgrade, MultiRegion Failover.
            |# license considerations; e.g., Do not autoscale freemium pipelines
            |
            |
            |eligible(item, context, reason) if not ineligible(item, context, root_reason) and reason = root_reason;
            |
            |ineligible(item, _context, reason) if item.cluster.nr_active_jobs == 0 and reason = "no_active_jobs";
            |ineligible(_item, context, reason) if context.cluster.is_rescaling and reason = "rescaling";
            |ineligible(item, context, reason) if deploying(item, context) and reason = "deploying";
            |ineligible(item, context, reason) if recent_failure(item, context) and reason = "recent_failure";
            |ineligible(item, context, reason) if in_cooling_period(item, context) and reason = "cooling_period";
            |"##
            .trim_margin()
            .unwrap()
        );
    }

    #[test]
    fn test_render_without_template_data() {
        Lazy::force(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_render_without_template_data");
        let _main_span_guard = main_span.enter();

        let templates = TEMPLATES.clone();
        let actual = assert_ok!(render_template_policy(
            &templates,
            "common",
            Option::<&TemplateData>::None
        ));
        assert_eq!(
            actual,
            r##"|
            |# define eligibile rule in eligibility polar basis file.
            |
            |deploying(_, context) if context.cluster.is_deploying;
            |
            |in_cooling_period(_, _) if false;
            |
            |recent_failure(_, _) if false;
            |
            |# Do not scale during multi region failure
            |# Are there job failures
            |# Do not scale while there are current operations; e.g., Cancel, Upgrade, MultiRegion Failover.
            |# license considerations; e.g., Do not autoscale freemium pipelines
            |
            |"##
            .trim_margin()
            .unwrap()
        );
    }
}
