// use crate::elements::{PolicySettings, Telemetry, Policy, make_item_context_policy};
// use crate::flink::eligibility::context::*;
// use crate::flink::MetricCatalog;
// use crate::graph::GraphResult;
// use crate::phases::collection::TelemetrySubscription;
// use oso::{Oso, PolarClass};
// use std::collections::HashSet;
//
//
//todo: leave this until SimplePolicy and corresponding maker fns prove themselves.
// #[derive(Debug)]
// pub struct EligibilityPolicy {
//     required_subscription_fields: HashSet<String>,
//     optional_subscription_fields: HashSet<String>,
//     policy_source: PolicySource,
// }
//
// impl EligibilityPolicy {
//     pub fn new(settings: &impl PolicySettings) -> Self {
//         let policy_source = settings.source();
//         Self {
//             required_subscription_fields: settings.required_subscription_fields(),
//             optional_subscription_fields: settings.optional_subscription_fields(),
//             policy_source,
//         }
//     }
// }
//
// impl PolicySubscription for EligibilityPolicy {
//     type Context = FlinkEligibilityContext;
//
//     fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
//         subscription
//             .with_required_fields(self.required_subscription_fields.clone())
//             .with_optional_fields(self.optional_subscription_fields.clone())
//     }
// }
//
// impl QueryPolicy for EligibilityPolicy {
//     type Args = (MetricCatalog, FlinkEligibilityContext);
//
//     fn load_policy_engine(&self, oso: &mut Oso) -> GraphResult<()> {
//         self.policy_source.load_into(oso)
//     }
//
//     fn initialize_policy_engine(&mut self, oso: &mut Oso) -> GraphResult<()> {
//         oso.register_class(Telemetry::get_polar_class())?;
//         oso.register_class(FlinkEligibilityContext::get_polar_class())?;
//         oso.register_class(
//             TaskStatus::get_polar_class_builder()
//                 .name("TaskStatus")
//                 .add_method("last_failure_within_seconds", TaskStatus::last_failure_within_seconds)
//                 .build(),
//         )?;
//         oso.register_class(
//             ClusterStatus::get_polar_class_builder()
//                 .name("ClusterStatus")
//                 .add_method(
//                     "last_deployment_within_seconds",
//                     ClusterStatus::last_deployment_within_seconds,
//                 )
//                 .build(),
//         )?;
//
//         Ok(())
//     }
//
//     fn query_policy(&self, oso: &Oso, args: Self::Args) -> GraphResult<oso::Query> {
//         oso.query_rule("eligible", args).map_err(|err| err.into())
//     }
// }
