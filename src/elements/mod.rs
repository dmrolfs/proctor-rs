use async_trait::async_trait;
pub use collection::Collect;
pub use from_telemetry::*;
use oso::ToPolar;
use pretty_snowflake::Label;
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
pub trait ProctorContext: AppData + Label + SubscriptionRequirements + ToPolar + Serialize + DeserializeOwned {
    type ContextData: AppData + Label + DeserializeOwned;
    type Error: std::error::Error + From<anyhow::Error> + Send + Sync;

    fn custom(&self) -> telemetry::TableType;

    #[tracing::instrument(level = "trace", skip(tx_clearinghouse_api))]
    async fn connect_context(
        subscription: TelemetrySubscription, tx_clearinghouse_api: &ClearinghouseApi,
    ) -> Result<SubscriptionChannel<Self::ContextData>, Self::Error> {
        let channel = SubscriptionChannel::new(subscription.name())
            .await
            .map_err(|err| err.into())?;

        ClearinghouseCmd::subscribe(
            tx_clearinghouse_api,
            subscription,
            channel.subscription_receiver.clone(),
        )
        .await
        .map_err(|err| err.into())?;

        Ok(channel)
    }
}
