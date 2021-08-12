use crate::elements::{FromTelemetry, QueryResult};
use crate::error::PolicyError;

#[derive(Debug, Clone, PartialEq)]
pub struct PolicyOutcome<T, C> {
    pub item: T,
    pub context: C,
    pub policy_results: QueryResult,
}

impl<T, C> PolicyOutcome<T, C> {
    pub fn new(item: T, context: C, results: QueryResult) -> Self {
        Self { item, context, policy_results: results }
    }

    pub fn passed(&self) -> bool {
        !self.policy_results.is_empty()
    }

    pub fn binding<B: FromTelemetry>(&self, var: impl AsRef<str>) -> Result<Vec<B>, PolicyError> {
        self.policy_results.binding(var)
    }
}
