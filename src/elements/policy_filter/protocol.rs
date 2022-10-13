use pretty_snowflake::{Label, MakeLabeling};
use std::fmt::Debug;
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc, oneshot};

use crate::elements::{PolicySource, QueryResult};
use crate::error::PolicyError;
use crate::Ack;

pub type PolicyFilterApi<C, D> = mpsc::UnboundedSender<PolicyFilterCmd<C, D>>;
pub type PolicyFilterApiReceiver<C, D> = mpsc::UnboundedReceiver<PolicyFilterCmd<C, D>>;
pub type PolicyFilterMonitor<T, C> = broadcast::Receiver<Arc<PolicyFilterEvent<T, C>>>;

#[derive(Debug)]
pub enum PolicyFilterCmd<C, D> {
    ReplacePolicies {
        new_policies: Vec<PolicySource>,
        new_template_data: Option<D>,
        tx: oneshot::Sender<Ack>,
    },
    AppendPolicy {
        additional_policy: PolicySource,
        new_template_data: Option<D>,
        tx: oneshot::Sender<Ack>,
    },
    Inspect(oneshot::Sender<PolicyFilterDetail<C, D>>),
}

impl<C, D> PolicyFilterCmd<C, D> {
    const STAGE_NAME: &'static str = "policy_filter";

    pub async fn replace_policies(
        api: &PolicyFilterApi<C, D>, new_policies: Vec<PolicySource>, new_template_data: Option<D>,
    ) -> Result<Ack, PolicyError>
    where
        C: Debug + Send + Sync + 'static,
        D: Debug + Send + Sync + 'static,
    {
        let (tx, rx) = oneshot::channel();
        api.send(Self::ReplacePolicies { new_policies, new_template_data, tx })
            .map_err(|err| PolicyError::Api(Self::STAGE_NAME.to_string(), err.into()))?;

        rx.await
            .map_err(|err| PolicyError::Api(Self::STAGE_NAME.to_string(), err.into()))
    }

    pub async fn append_policy(
        api: &PolicyFilterApi<C, D>, additional_policy: PolicySource, new_template_data: Option<D>,
    ) -> Result<Ack, PolicyError>
    where
        C: Debug + Send + Sync + 'static,
        D: Debug + Send + Sync + 'static,
    {
        let (tx, rx) = oneshot::channel();
        api.send(Self::AppendPolicy { additional_policy, new_template_data, tx })
            .map_err(|err| PolicyError::Api(Self::STAGE_NAME.to_string(), err.into()))?;

        rx.await
            .map_err(|err| PolicyError::Api(Self::STAGE_NAME.to_string(), err.into()))
    }

    pub async fn inspect(api: &PolicyFilterApi<C, D>) -> Result<PolicyFilterDetail<C, D>, PolicyError>
    where
        C: Debug + Send + Sync + 'static,
        D: Debug + Send + Sync + 'static,
    {
        let (tx, rx) = oneshot::channel();
        api.send(Self::Inspect(tx))
            .map_err(|err| PolicyError::Api(Self::STAGE_NAME.to_string(), err.into()))?;
        rx.await
            .map_err(|err| PolicyError::Api(Self::STAGE_NAME.to_string(), err.into()))
    }
}

#[derive(Debug)]
pub struct PolicyFilterDetail<C, D> {
    pub name: String,
    pub context: Option<C>,
    pub policy_template_data: Option<D>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PolicyFilterEvent<T, C> {
    ContextChanged(Option<C>),
    ItemPassed(T, QueryResult),
    ItemBlocked(T, Option<QueryResult>),
}

impl<T, C> Label for PolicyFilterEvent<T, C> {
    type Labeler = MakeLabeling<Self>;

    fn labeler() -> Self::Labeler {
        MakeLabeling::default()
    }
}
