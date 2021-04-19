use crate::elements::{make_from_telemetry, FromTelemetryShape, TelemetryData};
use crate::error::GraphError;
use crate::graph::stage::Stage;
use crate::graph::{GraphResult, Inlet, Outlet, Port, SourceShape};
use crate::AppData;
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use serde::de::DeserializeOwned;
use std::fmt::{self, Debug};

/// Subscription Source stage that can be used to adapt subscribed telemetry data into a
/// typed inlet.
///
pub struct SubscriptionChannel<T> {
    name: String,
    pub subscription_receiver: Inlet<TelemetryData>,
    inner_stage: Option<FromTelemetryShape<T>>,
    outlet: Outlet<T>,
}

impl<T: AppData + Sync + DeserializeOwned> SubscriptionChannel<T> {
    pub async fn new<S: Into<String>>(name: S) -> GraphResult<Self> {
        let name = name.into();
        let inner_stage = make_from_telemetry(name.as_str(), true).await?;
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
    async fn check(&self) -> GraphResult<()> {
        self.subscription_receiver.check_attachment().await?;
        self.outlet.check_attachment().await?;
        if let Some(ref inner) = self.inner_stage {
            inner.check().await?;
        }
        Ok(())
    }

    async fn run(&mut self) -> GraphResult<()> {
        match self.inner_stage.as_mut() {
            Some(inner) => inner.run().await,
            None => Err(GraphError::GraphPrecondition(format!(
                "subscription_channel, {}, already spent - cannot run.",
                self.name
            ))),
        }
    }

    async fn close(mut self: Box<Self>) -> GraphResult<()> {
        tracing::info!("closing subscription_channel.");
        self.subscription_receiver.close().await;
        if let Some(inner) = self.inner_stage.take() {
            inner.close().await?;
        }
        self.outlet.close().await;
        Ok(())
    }
}
