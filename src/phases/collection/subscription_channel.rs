use std::fmt::{self, Debug};

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use serde::de::DeserializeOwned;

use crate::elements::{make_from_telemetry, FromTelemetryShape, Telemetry};
use crate::error::CollectionError;
use crate::graph::stage::Stage;
use crate::graph::{Inlet, Outlet, Port, SourceShape};
use crate::phases::collection::{ClearinghouseSubscriptionMagnet, SubscriptionRequirements, TelemetrySubscription};
use crate::{AppData, ProctorResult};
use std::collections::HashSet;

//todo: consider refactor all of these builder functions into a typed subscription channel builder.

/// Subscription Source stage that can be used to adapt subscribed telemetry data into a
/// typed inlet.
pub struct SubscriptionChannel<T> {
    name: String,
    pub subscription_receiver: Inlet<Telemetry>,
    inner_stage: Option<FromTelemetryShape<T>>,
    outlet: Outlet<T>,
}

impl<T: SubscriptionRequirements + AppData + DeserializeOwned> SubscriptionChannel<T> {
    #[tracing::instrument(level = "info")]
    pub async fn connect_channel(
        channel_name: &str, magnet: ClearinghouseSubscriptionMagnet<'_>,
    ) -> Result<SubscriptionChannel<T>, CollectionError> {
        Self::connect_channel_with_requirements(
            channel_name,
            magnet,
            <T as SubscriptionRequirements>::required_fields(),
            <T as SubscriptionRequirements>::optional_fields(),
        )
        .await
    }
}

impl<T: AppData + DeserializeOwned> SubscriptionChannel<T> {
    #[tracing::instrument(level = "info")]
    pub async fn connect_subscription(
        subscription: TelemetrySubscription, mut magnet: ClearinghouseSubscriptionMagnet<'_>,
    ) -> Result<SubscriptionChannel<T>, CollectionError> {
        let channel = Self::new(format!("{}_channel", subscription.name())).await?;
        magnet
            .subscribe(subscription, channel.subscription_receiver.clone())
            .await?;
        Ok(channel)
    }

    #[tracing::instrument(level = "info", skip(required_fields, optional_fields))]
    pub async fn connect_channel_with_requirements(
        channel_name: &str, magnet: ClearinghouseSubscriptionMagnet<'_>, required_fields: HashSet<impl Into<String>>,
        optional_fields: HashSet<impl Into<String>>,
    ) -> Result<SubscriptionChannel<T>, CollectionError> {
        let subscription = TelemetrySubscription::new(channel_name)
            .with_required_fields(required_fields)
            .with_optional_fields(optional_fields);
        Self::connect_subscription(subscription, magnet).await
    }
}

impl SubscriptionChannel<Telemetry> {
    #[tracing::instrument(level = "info")]
    pub async fn connect_telemetry_subscription(
        subscription: TelemetrySubscription, mut magnet: ClearinghouseSubscriptionMagnet<'_>,
    ) -> Result<SubscriptionChannel<Telemetry>, CollectionError> {
        let channel = Self::telemetry(format!("{}_telemetry_channel", subscription.name())).await?;
        magnet
            .subscribe(subscription, channel.subscription_receiver.clone())
            .await?;
        Ok(channel)
    }

    #[tracing::instrument(level = "info", skip(required_fields, optional_fields))]
    pub async fn connect_telemetry_channel(
        channel_name: &str, magnet: ClearinghouseSubscriptionMagnet<'_>, required_fields: HashSet<impl Into<String>>,
        optional_fields: HashSet<impl Into<String>>,
    ) -> Result<SubscriptionChannel<Telemetry>, CollectionError> {
        let subscription = TelemetrySubscription::new(channel_name)
            .with_required_fields(required_fields)
            .with_optional_fields(optional_fields);
        Self::connect_telemetry_subscription(subscription, magnet).await
    }
}

impl<T: AppData + DeserializeOwned> SubscriptionChannel<T> {
    #[tracing::instrument(level="info", name="subscription_channel_new", skip(name), fields(channel_name=%name.as_ref()))]
    pub async fn new(name: impl AsRef<str>) -> Result<Self, CollectionError> {
        let inner_stage = make_from_telemetry(name.as_ref(), true).await;
        let subscription_receiver = inner_stage.inlet();
        let outlet = inner_stage.outlet();

        Ok(Self {
            name: format!("{}_typed_subscription", name.as_ref()),
            subscription_receiver,
            inner_stage: Some(inner_stage),
            outlet,
        })
    }
}

impl SubscriptionChannel<Telemetry> {
    #[tracing::instrument(level="info", name="subscription_channel_telemetry", skip(name), fields(channel_name=%name.as_ref()))]
    pub async fn telemetry(name: impl AsRef<str>) -> Result<Self, CollectionError> {
        let identity =
            crate::graph::stage::Identity::new(name.as_ref(), Inlet::new(name.as_ref()), Outlet::new(name.as_ref()));
        let inner_stage: FromTelemetryShape<Telemetry> = Box::new(identity);
        let subscription_receiver = inner_stage.inlet();
        let outlet = inner_stage.outlet();

        Ok(Self {
            name: format!("{}_telemetry_subscription", name.as_ref()),
            subscription_receiver,
            inner_stage: Some(inner_stage),
            outlet,
        })
    }
}

impl<T: Debug> Debug for SubscriptionChannel<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscriptionChannel")
            .field("name", &self.name)
            .field("subscription_receiver", &self.subscription_receiver)
            .field("inner", &self.inner_stage)
            .field("outlet", &self.outlet)
            .finish()
    }
}

impl<T> SourceShape for SubscriptionChannel<T> {
    type Out = T;
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<T: AppData> Stage for SubscriptionChannel<T> {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run subscription channel", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        self.do_run().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        self.do_close().await?;
        Ok(())
    }
}

impl<T: AppData> SubscriptionChannel<T> {
    async fn do_check(&self) -> Result<(), CollectionError> {
        self.subscription_receiver.check_attachment().await?;
        self.outlet.check_attachment().await?;
        if let Some(ref inner) = self.inner_stage {
            inner
                .check()
                .await
                .map_err(|err| CollectionError::StageError(err.into()))?;
        }
        Ok(())
    }

    async fn do_run(&mut self) -> Result<(), CollectionError> {
        match self.inner_stage.as_mut() {
            Some(inner) => {
                inner.run().await.map_err(|err| CollectionError::StageError(err.into()))?;
                Ok(())
            }

            None => Err(CollectionError::ClosedSubscription(self.name.clone())),
        }
    }

    async fn do_close(mut self: Box<Self>) -> Result<(), CollectionError> {
        tracing::info!("closing subscription_channel.");
        self.subscription_receiver.close().await;
        if let Some(inner) = self.inner_stage.take() {
            inner
                .close()
                .await
                .map_err(|err| CollectionError::StageError(err.into()))?;
        }
        self.outlet.close().await;
        Ok(())
    }
}
