use std::collections::HashMap;

use oso::Query;

use crate::elements::{FromTelemetry, TelemetryValue};
use crate::error::PolicyError;

pub type Bindings = HashMap<String, Vec<TelemetryValue>>;

#[derive(Debug, Default, Clone, PartialEq)]
pub struct QueryResult {
    pub passed: bool,
    pub bindings: Bindings,
}

impl QueryResult {
    pub fn passed_without_bindings() -> Self {
        Self { passed: true, bindings: Bindings::default() }
    }

    #[tracing::instrument(level = "debug", skip(query))]
    pub fn from_query(query: Query) -> Result<Self, PolicyError> {
        let mut bindings = Bindings::default();
        let mut passed = None;

        for result_set in query {
            let result_set = result_set?;

            if passed.is_none() {
                tracing::info!(?result_set, "DMR: item passes policy review!");
                passed = Some(true);
            }

            for key in result_set.keys() {
                let value = result_set.get_typed(key);
                tracing::info!(?result_set, "DMR: pulling binding: binding[{}]={:?}", key, value);
                match value? {
                    TelemetryValue::Unit => {
                        tracing::debug!("Unit value bound to key[{}] - skipping.", key);
                    },
                    val => {
                        if let Some(values) = bindings.get_mut(key) {
                            values.push(val);
                            tracing::info!("DMR: push binding[{}]: {:?}", key, values);
                        } else {
                            tracing::info!("DMR: started binding[{}]: [{:?}]", key, val);
                            bindings.insert(key.to_string(), vec![val]);
                        }
                    },
                }
            }
        }

        Ok(Self { passed: passed.unwrap_or(false), bindings })
    }

    pub fn binding<T: FromTelemetry>(&self, var: impl AsRef<str>) -> Result<Vec<T>, PolicyError> {
        if let Some(bindings) = self.bindings.get(var.as_ref()) {
            let mut result = vec![];

            for b in bindings {
                result.push(T::from_telemetry(b.clone())?)
            }

            Ok(result)
        } else {
            Ok(vec![])
        }
    }
}

impl std::ops::Deref for QueryResult {
    type Target = Bindings;

    fn deref(&self) -> &Self::Target {
        &self.bindings
    }
}
