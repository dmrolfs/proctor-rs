use prometheus::Registry;

use crate::elements::policy_filter;
use crate::error::ProctorError;
use crate::graph;
use crate::phases::sense::clearinghouse;

#[tracing::instrument(level = "trace")]
pub fn register_proctor_metrics(registry: &Registry) -> Result<(), ProctorError> {
    registry.register(Box::new(graph::GRAPH_ERRORS.clone()))?;
    registry.register(Box::new(graph::STAGE_INGRESS_COUNTS.clone()))?;
    registry.register(Box::new(graph::STAGE_EGRESS_COUNTS.clone()))?;
    registry.register(Box::new(graph::stage::STAGE_EVAL_TIME.clone()))?;
    registry.register(Box::new(policy_filter::POLICY_FILTER_EVAL_TIME.clone()))?;
    registry.register(Box::new(clearinghouse::SUBSCRIPTIONS_GAUGE.clone()))?;
    registry.register(Box::new(clearinghouse::PUBLICATIONS.clone()))?;
    Ok(())
}
