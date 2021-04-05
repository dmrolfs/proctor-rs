use crate::{Ack, AppData};
use tokio::sync::{broadcast, mpsc, oneshot};

pub type PolicyFilterApi<E> = mpsc::UnboundedSender<PolicyFilterCmd<E>>;
pub type PolicyFilterMonitor<T, E> = broadcast::Receiver<PolicyFilterEvent<T, E>>;

#[derive(Debug)]
pub enum PolicyFilterCmd<E> {
    ReplacePolicy { new_policy: PolicySource, tx: oneshot::Sender<Ack> },
    AppendPolicy { policy: PolicySource, tx: oneshot::Sender<Ack> },
    ResetPolicy(oneshot::Sender<Ack>),
    Inspect(oneshot::Sender<PolicyFilterDetail<E>>),
}

impl<E> PolicyFilterCmd<E> {
    pub fn replace_policy(new_policy: PolicySource) -> (PolicyFilterCmd<E>, oneshot::Receiver<Ack>) {
        let (tx, rx) = oneshot::channel();
        (Self::ReplacePolicy { new_policy, tx }, rx)
    }

    pub fn append_policy(policy: PolicySource) -> (PolicyFilterCmd<E>, oneshot::Receiver<Ack>) {
        let (tx, rx) = oneshot::channel();
        (Self::AppendPolicy { policy, tx }, rx)
    }

    pub fn reset_policy() -> (PolicyFilterCmd<E>, oneshot::Receiver<Ack>) {
        let (tx, rx) = oneshot::channel();
        (Self::ResetPolicy(tx), rx)
    }

    pub fn inspect() -> (PolicyFilterCmd<E>, oneshot::Receiver<PolicyFilterDetail<E>>) {
        let (tx, rx) = oneshot::channel();
        (Self::Inspect(tx), rx)
    }
}

#[derive(Debug)]
pub struct PolicyFilterDetail<E> {
    pub name: String,
    pub environment: Option<E>,
}

#[derive(Debug, Clone)]
pub enum PolicyFilterEvent<T: AppData, E: AppData> {
    EnvironmentChanged(Option<E>),
    ItemBlocked(T),
}

#[derive(Debug)]
pub enum PolicySource {
    String(String),
    File(std::path::PathBuf),
}
