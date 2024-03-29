use pretty_snowflake::Label;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use crate::Env;
use serde::Serialize;

use super::cache::TelemetryCache;
use crate::elements::telemetry::UpdateMetricsFn;
use crate::elements::{Telemetry, TelemetryValue};
use crate::error::SenseError;
use crate::graph::{Connect, Inlet, Outlet, Port, PORT_DATA};

// todo: refactor to based on something like Json Schema
pub trait SubscriptionRequirements {
    fn required_fields() -> HashSet<String>;

    fn optional_fields() -> HashSet<String> {
        HashSet::default()
    }
}

impl<C> SubscriptionRequirements for Env<C>
where
    C: Label + SubscriptionRequirements,
{
    fn required_fields() -> HashSet<String> {
        <C as SubscriptionRequirements>::required_fields()
    }

    fn optional_fields() -> HashSet<String> {
        <C as SubscriptionRequirements>::optional_fields()
    }
}

#[derive(Clone, Serialize)]
pub enum TelemetrySubscription {
    All {
        name: String,
        #[serde(skip)]
        outlet_to_subscription: Outlet<Env<Telemetry>>,
        #[serde(skip)]
        update_metrics: Option<Arc<UpdateMetricsFn>>,
    },
    Explicit {
        name: String,
        required_fields: HashSet<String>,
        optional_fields: HashSet<String>,
        #[serde(skip)]
        outlet_to_subscription: Outlet<Env<Telemetry>>,
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
        let name = format!("{}_subscription", name.as_ref());
        let outlet_to_subscription = Outlet::new(&name, PORT_DATA);
        Self::All { name, outlet_to_subscription, update_metrics: None }
    }

    pub fn for_requirements<T: SubscriptionRequirements>(self) -> Self {
        self.with_required_fields(T::required_fields())
            .with_optional_fields(T::optional_fields())
    }

    pub fn with_required_fields<S: Into<String>>(self, required_fields: HashSet<S>) -> Self {
        let required_fields = required_fields.into_iter().map(|s| s.into()).collect();

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

    pub fn with_optional_fields<S: Into<String>>(self, optional_fields: HashSet<S>) -> Self {
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

    pub fn name(&self) -> &str {
        match self {
            Self::All { name, .. } => name,
            Self::Explicit { name, .. } => name,
        }
    }

    pub fn outlet_to_subscription(&self) -> Outlet<Env<Telemetry>> {
        match self {
            Self::All { outlet_to_subscription, .. } => outlet_to_subscription.clone(),
            Self::Explicit { outlet_to_subscription, .. } => outlet_to_subscription.clone(),
        }
    }

    pub fn any_interest<'f, I>(&self, fields: I) -> bool
    where
        I: IntoIterator<Item = &'f String>,
    {
        match self {
            Self::All { .. } => true,
            Self::Explicit { required_fields, optional_fields, .. } => fields
                .into_iter()
                // .map(|p| p.as_str())
                .any(|field| {
                    required_fields.contains(field) || optional_fields.contains(field)
                }),
        }
    }

    pub fn trim_to_subscription<I>(&self, database: I) -> Result<(Telemetry, HashSet<String>), SenseError>
    where
        I: Iterator<Item = (String, TelemetryValue)>,
    {
        match self {
            Self::All { .. } => Ok((database.collect(), HashSet::default())),
            Self::Explicit { required_fields, optional_fields, .. } => {
                let mut db = Telemetry::new();
                let mut missing = HashSet::default();
                for (key, value) in database {
                    if required_fields.contains(&key) || optional_fields.contains(&key) {
                        let _ = db.insert(key, value);
                    }
                }

                for req in required_fields {
                    if !db.contains_key(req) {
                        missing.insert(req.clone());
                    }
                }

                for opt in optional_fields {
                    if !db.contains_key(opt) {
                        missing.insert(opt.clone());
                    }
                }

                Ok((db, missing))
            },
        }
    }

    #[tracing::instrument(level = "trace", skip(self, cache))]
    pub(super) fn fulfill(&self, cache: &TelemetryCache) -> Option<Telemetry> {
        match self {
            Self::All { .. } => {
                // let all = data.iter().cloned().collect::<std::collections::HashMap<_, _>>();
                // let all_telemetry = Telemetry::from_iter(all.into_iter());
                Some(cache.get_telemetry())
            },
            Self::Explicit { name, required_fields, optional_fields, .. } => {
                let mut ready = Vec::new();
                let mut unfilled = Vec::new();

                for required in required_fields.iter() {
                    match cache.get(required) {
                        Some(value) => ready.push((required.clone(), value.value().clone())),
                        None => unfilled.push(required),
                    }
                }

                if unfilled.is_empty() {
                    if !optional_fields.is_empty() {
                        tracing::trace!("Subscription({name}) looking for optional fields: {optional_fields:?}.");
                    }
                    for optional in optional_fields.iter() {
                        if let Some(value) = cache.get(optional) {
                            ready.push((optional.clone(), value.value().clone()))
                        }
                    }
                }

                if ready.is_empty() || !unfilled.is_empty() {
                    tracing::debug!(
                        subscription=?self, seen=?cache.seen(), unfilled_fields=?unfilled, ready_fields=?ready,
                        "unsatisfied Subscription({name}) - not publishing."
                    );

                    None
                } else {
                    tracing::debug!(?ready, ?unfilled, subscription=?self, "Subscription({name}) found required and optional fields.");
                    Some(Telemetry::from_iter(ready.into_iter()))
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
            update_metrics(self.name(), telemetry)
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn connect_to_receiver(&self, receiver: &Inlet<Env<Telemetry>>) {
        let outlet = self.outlet_to_subscription();
        (&outlet, receiver).connect().await;
    }

    pub async fn send(&self, telemetry: Env<Telemetry>) -> Result<(), SenseError> {
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
