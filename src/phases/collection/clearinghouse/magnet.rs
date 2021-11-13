use super::protocol::{ClearinghouseApi, ClearinghouseCmd};
use super::subscription::TelemetrySubscription;
use super::Clearinghouse;
use crate::elements::Telemetry;
use crate::error::CollectionError;
use crate::graph::Inlet;
use crate::phases::collection::CollectBuilder;
use std::fmt::Debug;

#[derive(Debug)]
pub enum ClearinghouseSubscriptionMagnet<'c> {
    Direct(&'c mut Clearinghouse),
    Api(&'c ClearinghouseApi),
}

use ClearinghouseSubscriptionMagnet as Magnet;

impl<'c> ClearinghouseSubscriptionMagnet<'c> {
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn subscribe(
        &mut self, subscription: TelemetrySubscription, receiver: Inlet<Telemetry>,
    ) -> Result<(), CollectionError> {
        match self {
            Magnet::Direct(clearinghouse) => {
                clearinghouse.subscribe(subscription, &receiver).await;
                Ok(())
            }

            Magnet::Api(tx_clearinghouse_api) => {
                let (cmd, rx_ack) = ClearinghouseCmd::subscribe(subscription.clone(), receiver.clone());
                tracing::info!(?subscription, ?receiver, "requesting clearinghouse subscription...");
                tx_clearinghouse_api
                    .send(cmd)
                    .map_err(|err| CollectionError::StageError(err.into()))?;

                let ack = rx_ack.await.map_err(|err| CollectionError::StageError(err.into()));
                tracing::warn!(
                    ?ack,
                    ?subscription,
                    "registered clearinghouse {} subscription: {}",
                    subscription.name(),
                    ack.is_ok()
                );
                ack
            }
        }
    }
}

impl<'c> From<&'c mut Clearinghouse> for ClearinghouseSubscriptionMagnet<'c> {
    fn from(that: &'c mut Clearinghouse) -> Self {
        ClearinghouseSubscriptionMagnet::Direct(that)
    }
}

impl<'a, 'c> From<&'a ClearinghouseApi> for ClearinghouseSubscriptionMagnet<'c>
where
    'a: 'c,
{
    fn from(that: &'a ClearinghouseApi) -> Self {
        ClearinghouseSubscriptionMagnet::Api(that)
    }
}

impl<'c, Out> From<&'c mut CollectBuilder<Out>> for ClearinghouseSubscriptionMagnet<'c> {
    fn from(that: &'c mut CollectBuilder<Out>) -> Self {
        ClearinghouseSubscriptionMagnet::Direct(&mut that.clearinghouse)
    }
}
