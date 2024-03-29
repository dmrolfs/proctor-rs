use async_trait::async_trait;

use super::subscription::TelemetrySubscription;
use crate::elements::Telemetry;
use crate::error::SenseError;
use crate::graph::Inlet;
use crate::Env;

#[async_trait]
pub trait ClearinghouseSubscriptionAgent: Send {
    async fn subscribe(
        &mut self, subscription: TelemetrySubscription, receiver: Inlet<Env<Telemetry>>,
    ) -> Result<(), SenseError>;
}
