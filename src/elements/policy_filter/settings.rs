use super::PolicySource;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct PolicySettings {
    #[serde(skip_serializing_if = "HashSet::is_empty")]
    pub required_subscription_fields: HashSet<String>,
    #[serde(skip_serializing_if = "HashSet::is_empty")]
    pub optional_subscription_fields: HashSet<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub policies: Vec<PolicySource>,
}

impl PolicySettings {
    pub fn new(required_fields: HashSet<String>, optional_fields: HashSet<String>) -> Self {
        Self {
            required_subscription_fields: required_fields,
            optional_subscription_fields: optional_fields,
            policies: vec![],
        }
    }

    pub fn with_source(mut self, source: PolicySource) -> Self {
        self.policies.push(source);
        self
    }
}
