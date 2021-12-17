use std::collections::HashSet;

use tokio::sync::{mpsc, oneshot};

use super::{Telemetry, TelemetrySubscription};
use crate::graph::Inlet;
use crate::Ack;

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
    #[inline]
    pub fn subscribe(
        subscription: TelemetrySubscription, receiver: Inlet<Telemetry>,
    ) -> (Self, oneshot::Receiver<Ack>) {
        let (tx, rx) = oneshot::channel();
        (
            Self::Subscribe { subscription: Box::new(subscription), receiver, tx },
            rx,
        )
    }

    #[inline]
    pub fn unsubscribe<S: Into<String>>(name: S) -> (Self, oneshot::Receiver<Ack>) {
        let (tx, rx) = oneshot::channel();
        (Self::Unsubscribe { name: name.into(), tx }, rx)
    }

    #[inline]
    pub fn get_clearinghouse_snapshot() -> (Self, oneshot::Receiver<ClearinghouseSnapshot>) {
        let (tx, rx) = oneshot::channel();
        (Self::GetSnapshot { name: None, tx }, rx)
    }

    #[inline]
    pub fn get_subscription_snapshot<S: Into<String>>(name: S) -> (Self, oneshot::Receiver<ClearinghouseSnapshot>) {
        let (tx, rx) = oneshot::channel();
        (Self::GetSnapshot { name: Some(name.into()), tx }, rx)
    }
}

#[derive(Debug)]
pub struct ClearinghouseSnapshot {
    pub database: Telemetry,
    pub missing: HashSet<String>,
    pub subscriptions: Vec<TelemetrySubscription>,
}
