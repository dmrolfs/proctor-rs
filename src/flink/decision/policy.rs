use super::context::FlinkDecisionContext;
use crate::elements::{PolicySettings, PolicySource, PolicySubscription, QueryPolicy, QueryResult, Telemetry};
use crate::flink::MetricCatalog;
use crate::graph::GraphResult;
use crate::phases::collection::TelemetrySubscription;
use crate::ProctorContext;
use oso::{Oso, PolarClass, PolarValue};
use std::collections::HashSet;

#[derive(Debug)]
pub struct DecisionPolicy {
    required_subscription_fields: HashSet<String>,
    optional_subscription_fields: HashSet<String>,
    policy_source: PolicySource,
}

impl DecisionPolicy {
    pub fn new(settings: &impl PolicySettings) -> Self {
        Self {
            required_subscription_fields: settings.required_subscription_fields(),
            optional_subscription_fields: settings.optional_subscription_fields(),
            policy_source: settings.source(),
        }
    }
}

impl PolicySubscription for DecisionPolicy {
    type Context = FlinkDecisionContext;

    fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
        subscription
            .with_required_fields(self.required_subscription_fields.clone())
            .with_optional_fields(self.optional_subscription_fields.clone())
    }
}

impl QueryPolicy for DecisionPolicy {
    type Item = MetricCatalog;
    type Context = FlinkDecisionContext;
    type Args = (Self::Item, Self::Context, PolarValue);

    fn load_policy_engine(&self, oso: &mut Oso) -> GraphResult<()> {
        self.policy_source.load_into(oso)
    }

    fn initialize_policy_engine(&mut self, oso: &mut Oso) -> GraphResult<()> {
        oso.register_class(Telemetry::get_polar_class())?;

        oso.register_class(
            FlinkDecisionContext::get_polar_class_builder()
                .add_method("custom", ProctorContext::custom)
                .build(),
        )?;

        Ok(())
    }

    fn make_query_args(&self, item: &Self::Item, context: &Self::Context) -> Self::Args {
        (
            item.clone(),
            context.clone(),
            PolarValue::Variable("direction".to_string()),
        )
    }

    fn query_policy(&self, engine: &Oso, args: Self::Args) -> GraphResult<QueryResult> {
        QueryResult::from_query(engine.query_rule("scale", args)?)
    }
}