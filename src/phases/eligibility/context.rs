use chrono::{DateTime, Utc};
use oso::PolarClass;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use crate::ProctorContext;

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlinkEligibilityContext {
    #[polar(attribute)]
    #[serde(flatten)]
    pub task_status: TaskStatus,
    #[polar(attribute)]
    pub cluster_status: ClusterStatus,

    #[polar(attribute)]
    #[serde(flatten)]
    pub custom: HashMap<String, String>,
}

impl ProctorContext for FlinkEligibilityContext {
    fn custom(&self) -> HashMap<String, String> { self.custom.clone() }
}

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskStatus {
    #[serde(default)]
    #[serde(
        serialize_with = "crate::serde::serialize_optional_datetime",
        deserialize_with = "crate::serde::deserialize_optional_datetime"
    )]
    pub last_failure: Option<DateTime<Utc>>,
}

impl TaskStatus {
    pub fn last_failure_within_seconds(&self, seconds: i64) -> bool {
        self.last_failure.map_or(false, |last_failure| {
            let boundary = Utc::now() - chrono::Duration::seconds(seconds);
            boundary < last_failure
        })
    }
}

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterStatus {
    #[polar(attribute)]
    pub is_deploying: bool,
    #[serde(with = "crate::serde")]
    pub last_deployment: DateTime<Utc>,
}

impl ClusterStatus {
    pub fn last_deployment_within_seconds(&self, seconds: i64) -> bool {
        let boundary = Utc::now() - chrono::Duration::seconds(seconds);
        boundary < self.last_deployment
    }
}
