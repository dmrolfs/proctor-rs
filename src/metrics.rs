use crate::elements::policy_filter;
use crate::error::ProctorError;
use crate::graph;
use crate::phases::collection::clearinghouse;
use prometheus::Registry;

#[tracing::instrument(level = "info")]
pub fn register_proctor_metrics(registry: &Registry) -> Result<(), ProctorError> {
    registry.register(Box::new(graph::GRAPH_ERRORS.clone()))?;
    registry.register(Box::new(graph::STAGE_INGRESS_COUNTS.clone()))?;
    registry.register(Box::new(graph::STAGE_EGRESS_COUNTS.clone()))?;
    registry.register(Box::new(policy_filter::POLICY_FILTER_EVAL_COUNTS.clone()))?;
    registry.register(Box::new(policy_filter::POLICY_FILTER_EVAL_TIME.clone()))?;
    registry.register(Box::new(clearinghouse::SUBSCRIPTIONS_GAUGE.clone()))?;
    registry.register(Box::new(clearinghouse::PUBLICATIONS.clone()))?;
    Ok(())
}
