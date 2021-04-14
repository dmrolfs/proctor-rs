use super::context::*;
use crate::elements::{Policy, PolicySettings, PolicySource, TelemetryData};
use crate::graph::GraphResult;
use crate::phases::collection::TelemetrySubscription;
use crate::settings::EligibilitySettings;
use oso::{Oso, PolarClass};
use std::collections::HashSet;

#[derive(Debug)]
pub struct EligibilityPolicy {
    required_subscription_fields: HashSet<String>,
    optional_subscription_fields: HashSet<String>,
    policy_source: PolicySource,
}

impl EligibilityPolicy {
    pub fn new(settings: &EligibilitySettings) -> Self {
        let policy_source = PolicySource::File(settings.policy_path.clone());
        Self {
            required_subscription_fields: settings.required_subscription_fields(),
            optional_subscription_fields: settings.optional_subscription_fields(),
            policy_source,
        }
    }
}

impl Policy for EligibilityPolicy {
    type Item = TelemetryData;
    type Context = FlinkEligibilityContext;

    fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
        subscription
            .with_required_fields(self.required_subscription_fields.clone())
            .with_optional_fields(self.optional_subscription_fields.clone())
    }

    fn load_knowledge_base(&self, oso: &mut Oso) -> GraphResult<()> {
        self.policy_source.load_into(oso)
    }

    fn initialize_knowledge_base(&self, oso: &mut Oso) -> GraphResult<()> {
        oso.register_class(TelemetryData::get_polar_class())?;
        oso.register_class(FlinkEligibilityContext::get_polar_class())?;
        oso.register_class(
            TaskStatus::get_polar_class_builder()
                .name("TaskStatus")
                .add_method("last_failure_within_seconds", TaskStatus::last_failure_within_seconds)
                .build(),
        )?;
        oso.register_class(
            ClusterStatus::get_polar_class_builder()
                .name("ClusterStatus")
                .add_method(
                    "last_deployment_within_seconds",
                    ClusterStatus::last_deployment_within_seconds,
                )
                .build(),
        )?;

        Ok(())
    }

    fn query_knowledge_base(&self, oso: &Oso, item_env: (Self::Item, Self::Context)) -> GraphResult<oso::Query> {
        oso.query_rule("eligible", item_env).map_err(|err| err.into())
    }
}
