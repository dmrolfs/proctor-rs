use super::Str;
use crate::elements::Telemetry;
use crate::error::CollectionError;
use crate::graph::{Connect, Inlet, Outlet, Port};
use std::collections::HashSet;

//todo: refactor to based on something like Json Schema
pub trait SubscriptionRequirements {
    fn required_fields() -> HashSet<Str>;

    fn optional_fields() -> HashSet<Str> {
        HashSet::default()
    }
}

#[derive(Debug, Clone)]
pub enum TelemetrySubscription {
    All {
        name: String,
        outlet_to_subscription: Outlet<Telemetry>,
    },
    Explicit {
        name: String,
        required_fields: HashSet<String>,
        optional_fields: HashSet<String>,
        outlet_to_subscription: Outlet<Telemetry>,
    },
    /* Remainder {
     *     name: String,
     *     outlet_to_subscription: Outlet<TelemetryData>,
     * } */
}

impl TelemetrySubscription {
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        let outlet_to_subscription = Outlet::new(format!("outlet_for_subscription_{}", name));
        Self::All { name, outlet_to_subscription }
    }

    pub fn with_requirements<T: SubscriptionRequirements>(self) -> Self {
        self.with_required_fields(T::required_fields())
            .with_optional_fields(T::optional_fields())
    }

    pub fn with_required_fields<S: Into<String>>(self, required_fields: HashSet<S>) -> Self {
        let required_fields = required_fields.into_iter().map(|s| s.into()).collect();

        match self {
            Self::All { name, outlet_to_subscription } => Self::Explicit {
                name,
                required_fields,
                optional_fields: HashSet::default(),
                outlet_to_subscription,
            },
            Self::Explicit {
                name,
                required_fields: mut my_required_fields,
                optional_fields,
                outlet_to_subscription,
            } => {
                my_required_fields.extend(required_fields);
                Self::Explicit {
                    name,
                    required_fields: my_required_fields,
                    optional_fields,
                    outlet_to_subscription,
                }
            }
        }
    }

    pub fn with_optional_fields<S: Into<String>>(self, optional_fields: HashSet<S>) -> Self {
        let optional_fields = optional_fields.into_iter().map(|s| s.into()).collect();
        match self {
            Self::All { name, outlet_to_subscription } => Self::Explicit {
                name,
                required_fields: HashSet::default(),
                optional_fields,
                outlet_to_subscription,
            },
            Self::Explicit {
                name,
                required_fields,
                optional_fields: mut my_optional_fields,
                outlet_to_subscription,
            } => {
                my_optional_fields.extend(optional_fields);
                Self::Explicit {
                    name,
                    required_fields,
                    optional_fields: my_optional_fields,
                    outlet_to_subscription,
                }
            }
        }
    }

    // pub fn remainder<S: Into<String>>(name: S) -> Self {
    //     let name = name.into();
    //     let outlet_to_subscription = Outlet::new(format!("outlet_for_subscription_{}", name));
    //     Self::Remainder { name, outlet_to_subscription }
    // }

    pub fn name(&self) -> &str {
        match self {
            Self::All { name, outlet_to_subscription: _ } => name.as_str(),
            Self::Explicit {
                name,
                required_fields: _,
                optional_fields: _,
                outlet_to_subscription: _,
            } => name.as_str(),
        }
    }

    pub fn outlet_to_subscription(&self) -> Outlet<Telemetry> {
        match self {
            Self::All { name: _, outlet_to_subscription } => outlet_to_subscription.clone(),
            Self::Explicit {
                name: _,
                required_fields: _,
                optional_fields: _,
                outlet_to_subscription,
            } => outlet_to_subscription.clone(),
        }
    }

    pub fn any_interest(&self, _available_fields: &HashSet<&String>, changed_fields: &HashSet<String>) -> bool {
        match self {
            Self::All { .. } => true,
            Self::Explicit {
                name: _,
                required_fields,
                optional_fields,
                outlet_to_subscription: _,
            } => {
                let mut interested = false;
                for changed in changed_fields {
                    if required_fields.contains(changed) || optional_fields.contains(changed) {
                        interested = true;
                        break;
                    }
                }
                interested
            }
        }
    }

    pub fn trim_to_subscription(&self, database: &Telemetry) -> Result<(Telemetry, HashSet<String>), CollectionError> {
        let mut db = database.clone();

        match self {
            Self::All { .. } => Ok((db, HashSet::default())),
            Self::Explicit {
                name: _,
                required_fields,
                optional_fields,
                outlet_to_subscription: _,
            } => {
                let mut missing = HashSet::default();
                for (key, _value) in database.iter() {
                    if !required_fields.contains(key) && !optional_fields.contains(key) {
                        let _ = db.remove(key);
                    }
                }

                for req in required_fields {
                    if !db.contains_key(req) {
                        missing.insert(req.into());
                    }
                }

                for opt in optional_fields {
                    if !db.contains_key(opt) {
                        missing.insert(opt.into());
                    }
                }

                Ok((db, missing))
            }
        }
    }

    #[tracing::instrument(level = "info")]
    pub fn fulfill(&self, database: &Telemetry) -> Option<Telemetry> {
        match self {
            Self::All { .. } => Some(database.clone()),
            Self::Explicit {
                name: _,
                required_fields,
                optional_fields,
                outlet_to_subscription: _,
            } => {
                let mut ready = Vec::new();
                let mut unfilled = Vec::new();

                for required in required_fields.iter() {
                    match database.get(required) {
                        Some(value) => ready.push((required.clone(), value)),
                        None => unfilled.push(required),
                    }
                }

                if unfilled.is_empty() {
                    for optional in optional_fields.iter() {
                        tracing::trace!(?optional, "looking for optional.");
                        if let Some(value) = database.get(optional) {
                            ready.push((optional.clone(), value))
                        }
                    }
                }

                tracing::trace!(?ready, ?unfilled, subscription=?self, "fulfilling required and optional fields.");
                if ready.is_empty() || !unfilled.is_empty() {
                    tracing::info!(
                        subscription=?self,
                        unfilled_fields=?unfilled,
                        "unsatisfided subscription - not publishing."
                    );

                    None
                } else {
                    let ready = ready.into_iter().map(|(k, v)| (k, v.clone())).collect();
                    Some(ready)
                }
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn connect_to_receiver(&self, receiver: &Inlet<Telemetry>) {
        let outlet = self.outlet_to_subscription();
        (&outlet, receiver).connect().await;
    }

    pub async fn send(&self, telemetry: Telemetry) -> Result<(), CollectionError> {
        self.outlet_to_subscription().send(telemetry).await?;
        Ok(())
    }
}

impl TelemetrySubscription {
    #[tracing::instrument()]
    pub async fn close(self) {
        self.outlet_to_subscription().close().await;
    }
}

impl PartialEq for TelemetrySubscription {
    fn eq(&self, other: &Self) -> bool {
        use TelemetrySubscription::*;

        match (self, other) {
            (All { name: lhs_name, outlet_to_subscription: _ }, All { name: rhs_name, outlet_to_subscription: _ }) => {
                lhs_name == rhs_name
            }
            (
                Explicit {
                    name: lhs_name,
                    required_fields: lhs_required,
                    optional_fields: lhs_optional,
                    outlet_to_subscription: _,
                },
                Explicit {
                    name: rhs_name,
                    required_fields: rhs_required,
                    optional_fields: rhs_optional,
                    outlet_to_subscription: _,
                },
            ) => (lhs_name == rhs_name) && (lhs_required == rhs_required) && (lhs_optional == rhs_optional),
            _ => false,
        }
    }
}
