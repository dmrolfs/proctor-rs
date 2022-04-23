use std::fmt::Debug;

use either::{Either, Left};
use thiserror::Error;

use super::MetricLabel;
use crate::SharedString;

#[derive(Debug, Error)]
pub enum PortError {
    #[error("cannot use detached port, {0}.")]
    Detached(String),

    /// error occurred while attempting to send across a sync channel.
    #[error("could not send data across sync channel: {0:?}")]
    Channel(#[source] anyhow::Error),
}

impl MetricLabel for PortError {
    fn slug(&self) -> SharedString {
        "port".into()
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            Self::Detached(_) => Left("detached".into()),
            Self::Channel(_) => Left("channel".into()),
        }
    }
}

impl<T: 'static + Debug + Send + Sync> From<tokio::sync::mpsc::error::SendError<T>> for PortError {
    fn from(that: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Self::Channel(that.into())
    }
}
