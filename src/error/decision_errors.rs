use either::{Either, Left, Right};
use thiserror::Error;

use super::{MetricLabel, PortError, TelemetryError};
use crate::SharedString;

#[derive(Debug, Error)]
pub enum DecisionError {
    #[error("data not found at key, {0}")]
    DataNotFound(String),

    #[error("failed to handle policy binding: {key} = {value}")]
    Binding { key: String, value: String },

    #[error("{0}")]
    Telemetry(#[from] TelemetryError),

    #[error("{0}")]
    Port(#[from] PortError),

    #[error("{0}")]
    Stage(#[from] anyhow::Error),
}

impl MetricLabel for DecisionError {
    fn slug(&self) -> SharedString {
        "decision".into()
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            Self::DataNotFound(_) => Left("data_not_found".into()),
            Self::Binding { .. } => Left("binding".into()),
            Self::Telemetry(e) => Right(Box::new(e)),
            Self::Port(e) => Right(Box::new(e)),
            Self::Stage(_) => Left("stage".into()),
        }
    }
}
