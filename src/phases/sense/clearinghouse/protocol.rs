use std::collections::HashSet;

use tokio::sync::{mpsc, oneshot};

use super::{Telemetry, TelemetrySubscription};
use crate::error::SenseError;
use crate::graph::Inlet;
use crate::{Ack, SharedString};

pub type ClearinghouseApi = mpsc::UnboundedSender<ClearinghouseCmd>;

#[derive(Debug)]
pub enum ClearinghouseCmd {
    Subscribe {
        subscription: Box<TelemetrySubscription>,
        receiver: Inlet<Telemetry>,
        tx: oneshot::Sender<Ack>,
    },
    Unsubscribe {
        name: String,
        tx: oneshot::Sender<Ack>,
    },
    GetSnapshot {
        name: Option<String>,
        tx: oneshot::Sender<ClearinghouseSnapshot>,
    },
}

impl ClearinghouseCmd {
    const STAGE_NAME: &'static str = "clearinghouse";

    pub async fn subscribe(
        api: &ClearinghouseApi, subscription: TelemetrySubscription, receiver: Inlet<Telemetry>,
    ) -> Result<Ack, SenseError> {
        let (tx, rx) = oneshot::channel();
        api.send(Self::Subscribe { subscription: Box::new(subscription), receiver, tx })
            .map_err(|err| SenseError::Api(Self::STAGE_NAME.to_string(), err.into()))?;

        rx.await
            .map_err(|err| SenseError::Api(Self::STAGE_NAME.to_string(), err.into()))
    }

    pub async fn unsubscribe(api: &ClearinghouseApi, name: &str) -> Result<Ack, SenseError> {
        let (tx, rx) = oneshot::channel();
        api.send(Self::Unsubscribe { name: name.to_string(), tx })
            .map_err(|err| SenseError::Api(Self::STAGE_NAME.to_string(), err.into()))?;
        rx.await
            .map_err(|err| SenseError::Api(Self::STAGE_NAME.to_string(), err.into()))
    }

    pub async fn get_clearinghouse_snapshot(api: &ClearinghouseApi) -> Result<ClearinghouseSnapshot, SenseError> {
        let (tx, rx) = oneshot::channel();
        api.send(Self::GetSnapshot { name: None, tx })
            .map_err(|err| SenseError::Api(Self::STAGE_NAME.to_string(), err.into()))?;
        rx.await
            .map_err(|err| SenseError::Api(Self::STAGE_NAME.to_string(), err.into()))
    }

    pub async fn get_subscription_snapshot(
        api: &ClearinghouseApi, name: &str,
    ) -> Result<ClearinghouseSnapshot, SenseError> {
        let (tx, rx) = oneshot::channel();
        api.send(Self::GetSnapshot { name: Some(name.to_string()), tx })
            .map_err(|err| SenseError::Api(Self::STAGE_NAME.to_string(), err.into()))?;
        rx.await
            .map_err(|err| SenseError::Api(Self::STAGE_NAME.to_string(), err.into()))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClearinghouseSnapshot {
    pub database: Telemetry,
    pub missing: HashSet<String>,
    pub subscriptions: Vec<TelemetrySubscription>,
}

impl serde::Serialize for ClearinghouseSnapshot {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(serde::Serialize)]
        struct Subscription<'n> {
            name: &'n str,
            required: Option<HashSet<SharedString>>,
            optional: Option<HashSet<SharedString>>,
        }

        use serde::ser::SerializeStruct;

        let mut state = serializer.serialize_struct("ClearinghouseSnapshot", 3)?;

        let telemetry = (&self.database).iter().collect::<std::collections::HashMap<_, _>>();
        state.serialize_field("telemetry", &telemetry)?;

        state.serialize_field("missing", &self.missing)?;

        let subscriptions = self
            .subscriptions
            .iter()
            .map(|subscription| match subscription {
                TelemetrySubscription::All { name, .. } => Subscription {
                    name: name.as_ref(),
                    required: None,
                    optional: None,
                }, // (name, None, None),
                TelemetrySubscription::Explicit { name, required_fields, optional_fields, .. } => {
                    Subscription {
                        name: name.as_ref(),
                        required: Some(required_fields.clone()),
                        optional: Some(optional_fields.clone()),
                    }
                    //(name, Some(required_fields), Some(optional_fields))
                },
            })
            .collect::<Vec<_>>();
        state.serialize_field("subscriptions", &subscriptions)?;

        state.end()
    }
}