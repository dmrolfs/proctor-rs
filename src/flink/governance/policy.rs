use std::collections::HashSet;

use oso::{Oso, PolarClass, PolarValue};
use serde::{Deserialize, Serialize};

use super::context::FlinkGovernanceContext;
use crate::elements::{PolicySettings, PolicySource, PolicySubscription, QueryPolicy, QueryResult, Telemetry};
use crate::error::PolicyError;
use crate::flink::plan::FlinkScalePlan;
use crate::phases::collection::TelemetrySubscription;
use crate::ProctorContext;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GovernancePolicy {
    required_subscription_fields: HashSet<String>,
    optional_subscription_fields: HashSet<String>,
    policy_source: PolicySource,
}

impl GovernancePolicy {
    pub const ADJUSTED_TARGET: &'static str = "adjusted_nr_task_managers";
    pub const DEFAULT_POLICY: &'static str = r#"
        accept(plan, context, adjusted_target) if acceot_step(plan, context, adjusted_target) and
            adjusted_target < context.min_cluster_size and
            adjusted_target = context.min_cluster_size;

        accept(plan, context, adjusted_target) if acceot_step(plan, context, adjusted_target) and
            context.max_cluster_size < adjusted_target and
            adjusted_target = context.max_cluster_size;

        accept_step(plan, context, adjusted_target)
            if custom.max_scaling_step < (plan.target_nr_task_managers - plan.current_nr_task_managers) and
            adjusted_target = plan.current_nr_task_managers + custom.max_scaling_step;

        accept_step(plan, context, adjusted_target)
            if custom.max_scaling_step < (plan.current_nr_task_managers - plan.target_nr_task_managers) and
            adjusted_target = plan.current_nr_task_managers - custom.max_scaling_step;
        "#;

    pub fn new(settings: &impl PolicySettings) -> Self {
        Self {
            required_subscription_fields: settings.required_subscription_fields(),
            optional_subscription_fields: settings.optional_subscription_fields(),
            policy_source: settings.source(),
        }
    }
}

impl PolicySubscription for GovernancePolicy {
    type Context = FlinkGovernanceContext;

    fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
        subscription
            .with_required_fields(self.required_subscription_fields.clone())
            .with_optional_fields(self.optional_subscription_fields.clone())
    }
}

impl QueryPolicy for GovernancePolicy {
    type Args = (Self::Item, Self::Context, PolarValue);
    type Context = FlinkGovernanceContext;
    type Item = FlinkScalePlan;

    fn load_policy_engine(&self, engine: &mut Oso) -> Result<(), PolicyError> {
        engine.load_str(Self::DEFAULT_POLICY)?;
        self.policy_source.load_into(engine)
    }

    fn initialize_policy_engine(&mut self, engine: &mut Oso) -> Result<(), PolicyError> {
        Telemetry::initialize_policy_engine(engine)?;

        engine.register_class(
            FlinkGovernanceContext::get_polar_class_builder()
                .add_method("custom", ProctorContext::custom)
                .build(),
        )?;

        Ok(())
    }

    fn make_query_args(&self, item: &Self::Item, context: &Self::Context) -> Self::Args {
        (
            item.clone(),
            context.clone(),
            PolarValue::Variable("adjusted_nr_task_managers".to_string()),
        )
    }

    fn query_policy(&self, engine: &Oso, args: Self::Args) -> Result<QueryResult, PolicyError> {
        QueryResult::from_query(engine.query_rule("accept", args)?)
    }
}
