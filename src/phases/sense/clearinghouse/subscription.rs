use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use serde::Serialize;

use crate::elements::telemetry::UpdateMetricsFn;
use crate::elements::Telemetry;
use crate::error::SenseError;
use crate::graph::{Connect, Inlet, Outlet, Port, PORT_DATA};
use crate::SharedString;

// todo: refactor to based on something like Json Schema
pub trait SubscriptionRequirements {
    fn required_fields() -> HashSet<SharedString>;

    fn optional_fields() -> HashSet<SharedString> {
        HashSet::default()
    }
}

#[derive(Clone, Serialize)]
pub enum TelemetrySubscription {
    All {
        name: SharedString,
        #[serde(skip)]
        outlet_to_subscription: Outlet<Telemetry>,
        #[serde(skip)]
        update_metrics: Option<Arc<UpdateMetricsFn>>,
    },
    Explicit {
        name: SharedString,
        required_fields: HashSet<SharedString>,
        optional_fields: HashSet<SharedString>,
        #[serde(skip)]
        outlet_to_subscription: Outlet<Telemetry>,
        #[serde(skip)]
        update_metrics: Option<Arc<UpdateMetricsFn>>,
    },
}

impl fmt::Debug for TelemetrySubscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::All { name, outlet_to_subscription, update_metrics: _ } => f
                .debug_struct("TelemetrySubscription")
                .field("name", &name)
                .field("outlet_to_subscription", &outlet_to_subscription)
                .field("required", &"[]")
                .field("optional", &"[<all>]")
                .finish(),
            Self::Explicit {
                name,
                required_fields,
                optional_fields,
                outlet_to_subscription,
                update_metrics: _,
            } => f
                .debug_struct("TelemetrySubscription")
                .field("name", &name)
                .field("outlet_to_subscription", &outlet_to_subscription)
                .field("required", &required_fields)
                .field("optional", &optional_fields)
                .finish(),
        }
    }
}

impl TelemetrySubscription {
    pub fn new(name: impl AsRef<str>) -> Self {
        let name: SharedString = SharedString::Owned(format!("{}_subscription", name.as_ref()));
        let outlet_to_subscription = Outlet::new(name.clone(), PORT_DATA);
        Self::All { name, outlet_to_subscription, update_metrics: None }
    }

    pub fn for_requirements<T: SubscriptionRequirements>(self) -> Self {
        self.with_required_fields(T::required_fields())
            .with_optional_fields(T::optional_fields())
    }

    pub fn with_required_fields<S: Into<SharedString>>(self, required_fields: HashSet<S>) -> Self {
        let required_fields: HashSet<SharedString> = required_fields.into_iter().map(|s| s.into()).collect();

        match self {
            Self::All { name, outlet_to_subscription, update_metrics } => Self::Explicit {
                name,
                optional_fields: HashSet::default(),
                required_fields,
                outlet_to_subscription,
                update_metrics,
            },
            Self::Explicit {
                name,
                required_fields: mut my_required_fields,
                optional_fields,
                outlet_to_subscription,
                update_metrics,
            } => {
                my_required_fields.extend(required_fields);

                Self::Explicit {
                    name,
                    required_fields: my_required_fields,
                    optional_fields,
                    outlet_to_subscription,
                    update_metrics,
                }
            },
        }
    }

    pub fn with_optional_fields<S: Into<SharedString>>(self, optional_fields: HashSet<S>) -> Self {
        let optional_fields = optional_fields.into_iter().map(|s| s.into()).collect();
        match self {
            Self::All { name, outlet_to_subscription, update_metrics } => Self::Explicit {
                name,
                required_fields: HashSet::default(),
                optional_fields,
                outlet_to_subscription,
                update_metrics,
            },
            Self::Explicit {
                name,
                required_fields,
                optional_fields: mut my_optional_fields,
                outlet_to_subscription,
                update_metrics,
            } => {
                my_optional_fields.extend(optional_fields);

                Self::Explicit {
                    name,
                    required_fields,
                    optional_fields: my_optional_fields,
                    outlet_to_subscription,
                    update_metrics,
                }
            },
        }
    }

    pub fn contains(&self, field: &str) -> bool {
        match self {
            Self::All { .. } => true,
            Self::Explicit { required_fields, optional_fields, .. } => {
                required_fields.contains(field) || optional_fields.contains(field)
            },
        }
    }

    pub fn with_update_metrics_fn(
        self, update_metrics: Box<dyn (Fn(&str, &Telemetry)) + Send + Sync + 'static>,
    ) -> Self {
        match self {
            Self::All { name, outlet_to_subscription, .. } => Self::All {
                name,
                outlet_to_subscription,
                update_metrics: Some(Arc::new(update_metrics)),
            },
            Self::Explicit {
                name,
                required_fields,
                optional_fields,
                outlet_to_subscription,
                ..
            } => Self::Explicit {
                name,
                required_fields,
                optional_fields,
                outlet_to_subscription,
                update_metrics: Some(Arc::new(update_metrics)),
            },
        }
    }

    pub fn name(&self) -> SharedString {
        match self {
            Self::All { name, .. } => name.clone(),
            Self::Explicit { name, .. } => name.clone(),
        }
    }

    pub fn outlet_to_subscription(&self) -> Outlet<Telemetry> {
        match self {
            Self::All { outlet_to_subscription, .. } => outlet_to_subscription.clone(),
            Self::Explicit { outlet_to_subscription, .. } => outlet_to_subscription.clone(),
        }
    }

    pub fn any_interest(&self, pushed_fields: &HashSet<String>) -> bool {
        match self {
            Self::All { .. } => true,
            Self::Explicit { required_fields, optional_fields, .. } => pushed_fields
                .iter()
                .map(|p| p.as_str())
                .any(|p| required_fields.contains(p) || optional_fields.contains(p)),
        }
    }

    pub fn trim_to_subscription(&self, database: &Telemetry) -> Result<(Telemetry, HashSet<String>), SenseError> {
        let mut db = database.clone();

        match self {
            Self::All { .. } => Ok((db, HashSet::default())),
            Self::Explicit { required_fields, optional_fields, .. } => {
                let mut missing = HashSet::default();
                for (key, _value) in database.iter() {
                    let key = SharedString::Owned(key.clone());
                    if !required_fields.contains(&key) && !optional_fields.contains(&key) {
                        let _ = db.remove(key.as_ref());
                    }
                }

                for req in required_fields {
                    if !db.contains_key(req.as_ref()) {
                        missing.insert(req.to_string());
                    }
                }

                for opt in optional_fields {
                    if !db.contains_key(opt.as_ref()) {
                        missing.insert(opt.to_string());
                    }
                }

                Ok((db, missing))
            },
        }
    }

    #[tracing::instrument(level = "trace")]
    pub fn fulfill(&self, database: &Telemetry) -> Option<Telemetry> {
        match self {
            Self::All { .. } => Some(database.clone()),
            Self::Explicit { required_fields, optional_fields, .. } => {
                let mut ready = Vec::new();
                let mut unfilled = Vec::new();

                for required in required_fields.iter() {
                    match database.get(required.as_ref()) {
                        Some(value) => ready.push((required.to_string(), value)),
                        None => unfilled.push(required),
                    }
                }

                if unfilled.is_empty() {
                    for optional in optional_fields.iter() {
                        tracing::trace!(?optional, "looking for optional.");
                        if let Some(value) = database.get(optional.as_ref()) {
                            ready.push((optional.to_string(), value))
                        }
                    }
                }

                tracing::trace!(?ready, ?unfilled, subscription=?self, "fulfilling required and optional fields.");
                if ready.is_empty() || !unfilled.is_empty() {
                    tracing::debug!(
                        subscription=?self,
                        unfilled_fields=?unfilled,
                        "unsatisfied subscription - not publishing."
                    );

                    None
                } else {
                    let ready = ready.into_iter().map(|(k, v)| (k, v.clone())).collect();
                    Some(ready)
                }
            },
        }
    }

    pub fn update_metrics(&self, telemetry: &Telemetry) {
        let update_fn = match self {
            Self::All { update_metrics, .. } => update_metrics,
            Self::Explicit { update_metrics, .. } => update_metrics,
        };

        if let Some(update_metrics) = update_fn {
            update_metrics(self.name().as_ref(), telemetry)
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn connect_to_receiver(&self, receiver: &Inlet<Telemetry>) {
        let outlet = self.outlet_to_subscription();
        (&outlet, receiver).connect().await;
    }

    pub async fn send(&self, telemetry: Telemetry) -> Result<(), SenseError> {
        self.outlet_to_subscription().send(telemetry).await?;
        Ok(())
    }
}

impl TelemetrySubscription {
    #[tracing::instrument(level = "trace")]
    pub async fn close(self) {
        self.outlet_to_subscription().close().await;
    }
}

impl PartialEq for TelemetrySubscription {
    fn eq(&self, other: &Self) -> bool {
        use TelemetrySubscription::*;

        match (self, other) {
            (All { name: lhs_name, .. }, All { name: rhs_name, .. }) => lhs_name == rhs_name,
            (
                Explicit {
                    name: lhs_name,
                    required_fields: lhs_required,
                    optional_fields: lhs_optional,
                    ..
                },
                Explicit {
                    name: rhs_name,
                    required_fields: rhs_required,
                    optional_fields: rhs_optional,
                    ..
                },
            ) => (lhs_name == rhs_name) && (lhs_required == rhs_required) && (lhs_optional == rhs_optional),
            _ => false,
        }
    }
}
