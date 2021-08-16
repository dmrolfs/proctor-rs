use std::fmt::{self, Debug};

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use serde::de::DeserializeOwned;

use crate::elements::{make_from_telemetry, FromTelemetryShape, Telemetry};
use crate::error::CollectionError;
use crate::graph::stage::Stage;
use crate::graph::{Inlet, Outlet, Port, SourceShape};
use crate::{AppData, ProctorResult};

/// Subscription Source stage that can be used to adapt subscribed telemetry data into a
/// typed inlet.
pub struct SubscriptionChannel<T> {
    name: String,
    pub subscription_receiver: Inlet<Telemetry>,
    inner_stage: Option<FromTelemetryShape<T>>,
    outlet: Outlet<T>,
}

impl<T: AppData + DeserializeOwned> SubscriptionChannel<T> {
    pub async fn new<S: Into<String>>(name: S) -> Result<Self, CollectionError> {
        let name = name.into();
        let inner_stage = make_from_telemetry(name.as_str(), true).await;
        let subscription_receiver = inner_stage.inlet();
        let outlet = inner_stage.outlet();

        Ok(Self {
            name,
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

    #[inline]
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<T: AppData> Stage for SubscriptionChannel<T> {
    #[inline]
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await?;
        Ok(())
    }

    async fn run(&mut self) -> ProctorResult<()> {
        self.do_run().await?;
        Ok(())
    }

    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        self.do_close().await?;
        Ok(())
    }
}

impl<T: AppData> SubscriptionChannel<T> {
    #[inline]
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

    #[inline]
    async fn do_run(&mut self) -> Result<(), CollectionError> {
        match self.inner_stage.as_mut() {
            Some(inner) => {
                inner.run().await.map_err(|err| CollectionError::StageError(err.into()))?;
                Ok(())
            }

            None => Err(CollectionError::ClosedSubscription(self.name.clone())),
        }
    }

    #[inline]
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
