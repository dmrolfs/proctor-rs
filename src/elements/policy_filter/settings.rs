use super::PolicySource;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt::Debug;

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct PolicySettings<T>
where
    T: Debug + Serialize + DeserializeOwned,
{
    #[serde(skip_serializing_if = "HashSet::is_empty")]
    pub required_subscription_fields: HashSet<String>,

    #[serde(skip_serializing_if = "HashSet::is_empty")]
    pub optional_subscription_fields: HashSet<String>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub policies: Vec<PolicySource>,

    #[serde(default, skip_serializing_if = "Option::is_none", bound = "")]
    pub template_data: Option<T>,
}

impl<T> PolicySettings<T>
where
    T: Debug + Serialize + DeserializeOwned,
{
    pub fn new(required_fields: HashSet<String>, optional_fields: HashSet<String>) -> Self {
        Self {
            required_subscription_fields: required_fields,
            optional_subscription_fields: optional_fields,
            policies: vec![],
            template_data: None,
        }
    }

    pub fn with_source(mut self, source: PolicySource) -> Self {
        self.policies.push(source);
        self
    }

    pub fn with_template_data(mut self, data: T) -> Self {
        self.template_data = Some(data);
        self
    }
}
