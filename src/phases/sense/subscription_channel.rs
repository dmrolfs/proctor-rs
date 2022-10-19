use std::fmt::{self, Debug};

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use pretty_snowflake::Label;
use serde::de::DeserializeOwned;

use crate::elements::{self, FromTelemetryShape, Telemetry};
use crate::error::SenseError;
use crate::graph::stage::Stage;
use crate::graph::{Inlet, Outlet, Port, SourceShape, PORT_DATA};
use crate::phases::sense::{ClearinghouseSubscriptionAgent, TelemetrySubscription};
use crate::{AppData, DataSet, ProctorResult};

// todo: consider refactor all of these builder functions into a typed subscription channel builder.

/// Subscription Source stage that can be used to adapt subscribed telemetry data into a
/// typed inlet.
pub struct SubscriptionChannel<T>
where
    T: Label,
{
    name: String,
    pub subscription_receiver: Inlet<DataSet<Telemetry>>,
    inner_stage: Option<FromTelemetryShape<T>>,
    outlet: Outlet<DataSet<T>>,
}

impl<T> SubscriptionChannel<T>
where
    T: AppData + Label + DeserializeOwned,
{
    #[tracing::instrument(level = "trace", skip(agent))]
    pub async fn connect_subscription<A>(subscription: TelemetrySubscription, agent: &mut A) -> Result<Self, SenseError>
    where
        A: ClearinghouseSubscriptionAgent,
    {
        let channel = Self::new(format!("{}_channel", subscription.name()).as_str()).await?;
        agent
            .subscribe(subscription, channel.subscription_receiver.clone())
            .await?;
        Ok(channel)
    }
}

impl SubscriptionChannel<Telemetry> {
    #[tracing::instrument(level = "trace", skip(agent))]
    pub async fn connect_telemetry_subscription<A>(
        subscription: TelemetrySubscription, agent: &mut A,
    ) -> Result<Self, SenseError>
    where
        A: ClearinghouseSubscriptionAgent,
    {
        let channel_name = format!("{}_channel", subscription.name());
        let channel = Self::telemetry(&channel_name).await?;
        agent
            .subscribe(subscription, channel.subscription_receiver.clone())
            .await?;
        Ok(channel)
    }
}

impl<T> SubscriptionChannel<T>
where
    T: AppData + Label + DeserializeOwned,
{
    #[tracing::instrument(level = "trace", name = "subscription_channel_new", skip(name))]
    pub async fn new(name: &str) -> Result<Self, SenseError> {
        let inner_stage = elements::make_from_telemetry(name, true).await;
        let subscription_receiver = inner_stage.inlet();
        let outlet = inner_stage.outlet();

        Ok(Self {
            name: name.to_string(),
            subscription_receiver,
            inner_stage: Some(inner_stage),
            outlet,
        })
    }
}

impl SubscriptionChannel<Telemetry> {
    /// Create a subscription channel for direct telemetry data; i.e., no schema conversion.
    #[tracing::instrument(level = "trace", name = "subscription_channel_telemetry", skip(name))]
    pub async fn telemetry(name: &str) -> Result<Self, SenseError> {
        let identity =
            crate::graph::stage::Identity::new(name, Inlet::new(name, PORT_DATA), Outlet::new(name, PORT_DATA));
        let inner_stage: FromTelemetryShape<Telemetry> = Box::new(identity);
        let subscription_receiver = inner_stage.inlet();
        let outlet = inner_stage.outlet();

        Ok(Self {
            name: format!("{name}_subscription"),
            subscription_receiver,
            inner_stage: Some(inner_stage),
            outlet,
        })
    }
}

impl<T> Debug for SubscriptionChannel<T>
where
    T: Debug + Label,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscriptionChannel")
            .field("name", &self.name)
            .field("subscription_receiver", &self.subscription_receiver)
            .field("inner", &self.inner_stage)
            .field("outlet", &self.outlet)
            .finish()
    }
}

impl<T> SourceShape for SubscriptionChannel<T>
where
    T: Label,
{
    type Out = DataSet<T>;

    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<T> Stage for SubscriptionChannel<T>
where
    T: AppData + Label,
{
    fn name(&self) -> &str {
        &self.name
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", name = "run subscription channel", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        self.do_run().await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        self.do_close().await?;
        Ok(())
    }
}

impl<T> SubscriptionChannel<T>
where
    T: AppData + Label,
{
    async fn do_check(&self) -> Result<(), SenseError> {
        self.subscription_receiver.check_attachment().await?;
        self.outlet.check_attachment().await?;
        if let Some(ref inner) = self.inner_stage {
            inner.check().await.map_err(|err| SenseError::Stage(err.into()))?;
        }
        Ok(())
    }

    async fn do_run(&mut self) -> Result<(), SenseError> {
        match self.inner_stage.as_mut() {
            Some(inner) => {
                inner.run().await.map_err(|err| SenseError::Stage(err.into()))?;
                Ok(())
            },

            None => Err(SenseError::ClosedSubscription(self.name.to_string())),
        }
    }

    async fn do_close(mut self: Box<Self>) -> Result<(), SenseError> {
        tracing::trace!(stage=%self.name(), "closing subscription_channel.");
        self.subscription_receiver.close().await;
        if let Some(inner) = self.inner_stage.take() {
            inner.close().await.map_err(|err| SenseError::Stage(err.into()))?;
        }
        self.outlet.close().await;
        Ok(())
    }
}
