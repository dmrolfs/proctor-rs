use super::QueryResult;
use crate::elements::{PolicySource, PolicySourcePath};
use crate::error::PolicyError;
use crate::phases::collection::{SubscriptionRequirements, TelemetrySubscription};
use oso::{ToPolar, ToPolarList};
use serde::Serialize;
use std::fmt::Debug;
use std::io::Write;

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
        let name = Self::base_template_name();
        let data = self.policy_template_data();
        let policy = self.render_policy(name, data)?;
        let policy_file = render_policy_source_tempfile(name, &policy)?;
        engine.load_files(vec![policy_file])?;
        Ok(())
    }

    #[tracing::instrument(level = "info")]
    fn render_policy(
        &self, template_name: &str, template_data: Option<&Self::TemplateData>,
    ) -> Result<String, PolicyError> {
        template_data
            .map(|data| {
                tracing::info!("rendering policy string as template with data.");

                // I tried to facilitate registry caching in policy, but handlebars' lifetime parameter
                // (underlying the PolicyRegistry) hampers the ergonomics of policy definition.
                // Not a performance impact since policy loading only happens on bootstrap or during
                // a corresponding, intermittent command.
                let mut registry = PolicyRegistry::new();
                for s in self.sources() {
                    let policy_template: String = s.try_into()?;
                    registry.register_template_string(s.name().as_ref(), policy_template)?;
                }
                tracing::debug!(?registry, "policy templates registered with handlebars registry");
                let policy = registry.render(template_name, data)?;
                tracing::info!(rendered_policy=%policy, "rendered {} policy from template and data.", template_name);
                Ok(policy)
            })
            .unwrap_or_else(|| {
                tracing::info!("no template data supplied -- assuming policy string is not a template.");

                self.sources()
                    .iter()
                    .find(|s| s.name().as_ref() == template_name)
                    .map(|s| s.try_into())
                    .unwrap_or(Err(PolicyError::StringPolicyError(format!(
                        "failed to find policy template: {}",
                        template_name
                    ))))
            })
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

#[tracing::instrument(level = "info", skip())]
fn render_policy_source_tempfile(name: &str, policy: &str) -> Result<PolicySourcePath, PolicyError> {
    let tempdir = std::env::current_dir()?;

    let mut tmp = tempfile::Builder::new()
        .prefix(format!("policy_{}", name).as_str())
        .rand_bytes(4)
        .suffix(".polar")
        .tempfile_in(tempdir)?;

    tracing::info!("rendered {} policy file for loading: {:?}", name, tmp);

    write!(tmp.as_file_mut(), "{}", policy)?;

    Ok(PolicySourcePath::String(tmp))
}
