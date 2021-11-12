use std::fmt::Debug;
use pretty_snowflake::Labeling;
use super::protocol::{ClearinghouseApi, ClearinghouseCmd};
use super::subscription::TelemetrySubscription;
use super::Clearinghouse;
use crate::elements::Telemetry;
use crate::error::CollectionError;
use crate::graph::Inlet;
use crate::phases::collection::CollectBuilder;

#[derive(Debug)]
pub enum ClearinghouseSubscriptionMagnet<'c, L: Labeling> {
    Direct(&'c mut Clearinghouse<L>),
    Api(&'c ClearinghouseApi),
}

use ClearinghouseSubscriptionMagnet as Magnet;

impl<'c, L> ClearinghouseSubscriptionMagnet<'c, L>
where
    L: Labeling + Debug,
{
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

impl<'c, L> From<&'c mut Clearinghouse<L>> for ClearinghouseSubscriptionMagnet<'c, L>
where
    L: Labeling,
{
    fn from(that: &'c mut Clearinghouse<L>) -> Self {
        ClearinghouseSubscriptionMagnet::Direct(that)
    }
}

impl<'a, 'c, L> From<&'a ClearinghouseApi> for ClearinghouseSubscriptionMagnet<'c, L>
where
    'a: 'c,
    L: Labeling,
{
    fn from(that: &'a ClearinghouseApi) -> Self {
        ClearinghouseSubscriptionMagnet::Api(that)
    }
}

impl<'c, Out, L: Labeling> From<&'c mut CollectBuilder<Out, L>> for ClearinghouseSubscriptionMagnet<'c, L> {
    fn from(that: &'c mut CollectBuilder<Out, L>) -> Self {
        ClearinghouseSubscriptionMagnet::Direct(&mut that.clearinghouse)
    }
}
