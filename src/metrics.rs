use once_cell::sync::Lazy;
use path_absolutize::*;
use prometheus::Registry;
use std::collections::HashMap;
use std::path::Path;

use crate::elements::policy_filter;
use crate::error::{MetricsError, ProctorError};
use crate::graph;
use crate::phases::sense::clearinghouse;

pub const ENV_VAR_CONST_LABELS_PATH: &str = "METRICS_CONST_LABELS";

pub static CONST_LABELS: Lazy<HashMap<String, String>> = Lazy::new(|| {
    std::env::var(ENV_VAR_CONST_LABELS_PATH)
        .map(|labels_path| {
            let absolute_labels_path = Path::new(&labels_path)
                .absolutize()
                .unwrap_or_else(|err| panic!("Failed to absolutize path {}: {:?}", labels_path, err));
            load_const_labels(absolute_labels_path.as_ref())
                .unwrap_or_else(|_| panic!("failed proctor metrics const_labels loading from {absolute_labels_path:?}"))
        })
        .unwrap_or_else(|_| HashMap::default())
});

pub fn load_const_labels(labels: impl AsRef<Path>) -> Result<HashMap<String, String>, MetricsError> {
    let labels_path = labels.as_ref();
    let labels_file = std::fs::File::open(labels_path)?;
    let extension = labels_path.extension().map(|ext| ext.to_string_lossy());

    let const_labels = match extension.as_deref() {
        None | Some("yaml") => serde_yaml::from_reader(labels_file)?,
        Some("json") => serde_json::from_reader(labels_file)?,
        Some("ron") => ron::de::from_reader(labels_file).map_err(|err| MetricsError::Ron(err))?,
        Some(unknown) => Err(MetricsError::UnknownFormat(unknown.to_string()))?,
    };

    Ok(const_labels)
}

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
