use async_trait::async_trait;

use super::subscription::TelemetrySubscription;
use crate::elements::Telemetry;
use crate::error::SenseError;
use crate::graph::Inlet;

#[async_trait]
pub trait ClearinghouseSubscriptionAgent {
    async fn subscribe(&mut self, subscription: TelemetrySubscription, receiver: Inlet<Telemetry>) -> Result<(), SenseError>;
}
