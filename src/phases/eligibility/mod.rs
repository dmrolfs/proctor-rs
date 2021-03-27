mod gather;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EligibilityContext {
    pub task_status: TaskStatus,
    pub cluster_status: ClusterStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskStatus {
    #[serde(default)]
    #[serde(
        serialize_with = "crate::serde::serialize_optional_datetime",
        deserialize_with = "crate::serde::deserialize_optional_datetime",
    )]
    pub last_failure: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterStatus {
    pub is_deploying: bool,
    #[serde(with = "crate::serde")]
    pub last_deployment: DateTime<Utc>,
}
