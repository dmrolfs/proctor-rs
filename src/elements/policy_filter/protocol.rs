use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::error::PolicyError;
use crate::Ack;

pub type PolicyFilterApi<C> = mpsc::UnboundedSender<PolicyFilterCmd<C>>;
pub type PolicyFilterApiReceiver<C> = mpsc::UnboundedReceiver<PolicyFilterCmd<C>>;
pub type PolicyFilterMonitor<T, C> = broadcast::Receiver<PolicyFilterEvent<T, C>>;

#[derive(Debug)]
pub enum PolicyFilterCmd<C> {
    ReplacePolicy {
        new_policy: PolicySource,
        tx: oneshot::Sender<Ack>,
    },
    AppendPolicy {
        additional_policy: PolicySource,
        tx: oneshot::Sender<Ack>,
    },
    ResetPolicy(oneshot::Sender<Ack>),
    Inspect(oneshot::Sender<PolicyFilterDetail<C>>),
}

impl<C> PolicyFilterCmd<C> {
    pub fn replace_policy(new_policy: PolicySource) -> (PolicyFilterCmd<C>, oneshot::Receiver<Ack>) {
        let (tx, rx) = oneshot::channel();
        (Self::ReplacePolicy { new_policy, tx }, rx)
    }

    pub fn append_policy(additional_policy: PolicySource) -> (PolicyFilterCmd<C>, oneshot::Receiver<Ack>) {
        let (tx, rx) = oneshot::channel();
        (Self::AppendPolicy { additional_policy, tx }, rx)
    }

    pub fn reset_policy() -> (PolicyFilterCmd<C>, oneshot::Receiver<Ack>) {
        let (tx, rx) = oneshot::channel();
        (Self::ResetPolicy(tx), rx)
    }

    pub fn inspect() -> (PolicyFilterCmd<C>, oneshot::Receiver<PolicyFilterDetail<C>>) {
        let (tx, rx) = oneshot::channel();
        (Self::Inspect(tx), rx)
    }
}

#[derive(Debug)]
pub struct PolicyFilterDetail<C> {
    pub name: String,
    pub context: Option<C>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PolicyFilterEvent<T, C> {
    ContextChanged(Option<C>),
    ItemPassed,
    ItemBlocked(T),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicySource {
    String(String),
    File(PathBuf),
    NoPolicy,
}

impl PolicySource {
    pub fn from_string<S: Into<String>>(policy: S) -> Self {
        Self::String(policy.into())
    }

    pub fn from_path(policy_path: PathBuf) -> Self {
        Self::File(policy_path)
    }

    pub fn load_into(&self, oso: &mut oso::Oso) -> Result<(), PolicyError> {
        match self {
            PolicySource::String(policy) => oso.load_str(policy.as_str())?,
            PolicySource::File(policy) => oso.load_file(policy)?,
            PolicySource::NoPolicy => (),
        };

        Ok(())
    }

    pub fn validate(&self) -> Result<(), PolicyError> {
        let polar = polar_core::polar::Polar::new();

        match self {
            Self::String(policy) => polar.load_str(policy.as_str())?,
            Self::File(policy) => {
                let file = policy.as_path();
                if !file.extension().map(|ext| ext == "polar").unwrap_or(false) {
                    return Err(oso::OsoError::IncorrectFileType { filename: file.to_string_lossy().into_owned() })
                        .map_err(|err| err.into());
                }

                use std::io::Read;
                let mut f = std::fs::File::open(file)?;
                let mut p = String::new();
                f.read_to_string(&mut p)?;
                polar.load_str(p.as_str())?;
            }
            Self::NoPolicy => (),
        };

        Ok(())
    }
}
