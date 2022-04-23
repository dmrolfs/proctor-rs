use chrono::{DateTime, Utc};
use either::{Either, Left, Right};
use thiserror::Error;

use super::{MetricLabel, PortError, TelemetryError};
use crate::SharedString;

#[derive(Debug, Error)]
pub enum PlanError {
    #[error("data not found at key, {0}")]
    DataNotFound(String),

    #[error("unknown performance repository type requested: {0}")]
    UnknownRepositoryType(String),

    #[error("{0}")]
    Telemetry(#[from] TelemetryError),

    #[error("{0}")]
    Port(#[from] PortError),

    #[error("0")]
    Forecast(anyhow::Error),

    #[error("not enough data to build forecast model - supplied:{supplied} need:{need}")]
    NotEnoughData { supplied: usize, need: usize },

    #[error("duration, {0}ms, exceeds supported limit")]
    DurationLimitExceeded(u128),

    #[error("zero duration for {0} not supported")]
    ZeroDuration(String),

    #[error("failed to solve regression for timestamp {0:?}")]
    RegressionFailed(DateTime<Utc>),

    #[error("{0}")]
    Stage(#[from] anyhow::Error),

    #[error("IO failure during planning phase: {0}")]
    IO(#[from] std::io::Error),

    #[error("failed to read performance history JSON: {0}")]
    Serde(#[from] serde_json::Error),
}

impl MetricLabel for PlanError {
    fn slug(&self) -> SharedString {
        "plan".into()
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            Self::DataNotFound(_) => Left("data_not_found".into()),
            Self::UnknownRepositoryType(_) => Left("settings".into()),
            Self::Telemetry(e) => Right(Box::new(e)),
            Self::Port(e) => Right(Box::new(e)),
            Self::Forecast(_) => Left("forecast".into()),
            _e @ Self::NotEnoughData { .. } => Left("not_enough_data".into()),
            Self::DurationLimitExceeded(_) => Left("duration_limit_exceeded".into()),
            Self::ZeroDuration(_) => Left("zero_duration".into()),
            Self::RegressionFailed(_) => Left("regression_failed".into()),
            Self::Stage(_) => Left("stage".into()),
            Self::IO(_) => Left("io".into()),
            Self::Serde(_) => Left("serde".into()),
        }
    }
}
