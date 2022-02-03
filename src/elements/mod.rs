use async_trait::async_trait;
pub use collection::Collect;
pub use from_telemetry::*;
use oso::PolarClass;
use serde::{de::DeserializeOwned, Serialize};

use crate::phases::sense::{
    ClearinghouseApi, ClearinghouseCmd, SubscriptionChannel, SubscriptionRequirements, TelemetrySubscription,
};
use crate::AppData;

mod collection;
mod from_telemetry;
pub mod policy_filter;
pub mod records_per_second;
pub mod signal;
pub mod telemetry;
pub mod timestamp;

pub use policy_filter::*;
pub use records_per_second::RecordsPerSecond;
pub use signal::*;
pub use telemetry::{FromTelemetry, Telemetry, TelemetryType, TelemetryValue, ToTelemetry};
pub use timestamp::*;

pub type Point = (f64, f64);

#[async_trait]
pub trait ProctorContext: AppData + SubscriptionRequirements + PolarClass + Serialize + DeserializeOwned {
    type Error: std::error::Error + From<anyhow::Error> + Send + Sync;

    fn custom(&self) -> telemetry::TableType;

    #[tracing::instrument(level = "info", skip(tx_clearinghouse_api))]
    async fn connect_context(
        subscription: TelemetrySubscription, tx_clearinghouse_api: &ClearinghouseApi,
    ) -> Result<SubscriptionChannel<Self>, Self::Error> {
        let channel = SubscriptionChannel::new(subscription.name())
            .await
            .map_err(|err| err.into())?;

        let (cmd, rx_ack) = ClearinghouseCmd::subscribe(subscription, channel.subscription_receiver.clone());

        tx_clearinghouse_api.send(cmd).map_err(|err| err.into())?;
        rx_ack.await.map_err(|err| err.into())?;

        Ok(channel)
    }
}
