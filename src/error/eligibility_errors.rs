use either::{Either, Left, Right};
use thiserror::Error;

use super::{MetricLabel, PortError, TelemetryError};

#[derive(Debug, Error)]
pub enum EligibilityError {
    #[error("data not found at key, {0}")]
    DataNotFound(String),

    #[error("failed to handle policy binding: {key} = {value}")]
    Binding { key: String, value: String },

    #[error("{0}")]
    Telemetry(#[from] TelemetryError),

    #[error("{0}")]
    Port(#[from] PortError),

    // #[error("sense error during eligibility phase: {0}")]
    // CollectionError(#[from] SenseError),
    #[error("{0}")]
    Stage(#[from] anyhow::Error),
}

impl MetricLabel for EligibilityError {
    fn slug(&self) -> String {
        "eligibility".into()
    }

    fn next(&self) -> Either<String, Box<&dyn MetricLabel>> {
        match self {
            Self::DataNotFound(_) => Left("data_not_found".into()),
            Self::Binding { .. } => Left("binding".into()),
            Self::Telemetry(e) => Right(Box::new(e)),
            Self::Port(e) => Right(Box::new(e)),
            // Self::CollectionError(e) => Right(Box::new(e)),
            Self::Stage(_) => Left("stage".into()),
        }
    }
}
