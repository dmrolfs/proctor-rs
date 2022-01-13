use std::fmt::Debug;
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc, oneshot};

use crate::elements::{PolicySource, QueryResult};
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
    pub fn replace_policies(
        new_policies: impl IntoIterator<Item = PolicySource>, new_template_data: Option<D>,
    ) -> (PolicyFilterCmd<C, D>, oneshot::Receiver<Ack>) {
        let (tx, rx) = oneshot::channel();
        let new_policies = new_policies.into_iter().collect();
        (Self::ReplacePolicies { new_policies, new_template_data, tx }, rx)
    }

    pub fn append_policy(
        additional_policy: PolicySource, new_template_data: Option<D>,
    ) -> (PolicyFilterCmd<C, D>, oneshot::Receiver<Ack>) {
        let (tx, rx) = oneshot::channel();
        (Self::AppendPolicy { additional_policy, new_template_data, tx }, rx)
    }

    pub fn inspect() -> (PolicyFilterCmd<C, D>, oneshot::Receiver<PolicyFilterDetail<C, D>>) {
        let (tx, rx) = oneshot::channel();
        (Self::Inspect(tx), rx)
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
