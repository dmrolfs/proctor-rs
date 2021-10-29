use super::PolicySource;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt::Debug;

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct PolicySettings<T> {
    #[serde(skip_serializing_if = "HashSet::is_empty")]
    pub required_subscription_fields: HashSet<String>,

    #[serde(skip_serializing_if = "HashSet::is_empty")]
    pub optional_subscription_fields: HashSet<String>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub policies: Vec<PolicySource>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_attributes: Option<T>,
}

impl<T> PolicySettings<T>
where
    T: Default,
{
    pub fn new(required_fields: HashSet<String>, optional_fields: HashSet<String>) -> Self {
        Self {
            required_subscription_fields: required_fields,
            optional_subscription_fields: optional_fields,
            policies: vec![],
            policy_attributes: None,
        }
    }

    pub fn with_source(mut self, source: PolicySource) -> Self {
        self.policies.push(source);
        self
    }

    pub fn with_attributes(mut self, policy_attributes: T) -> Self {
        self.policy_attributes = Some(policy_attributes);
        self
    }
}
