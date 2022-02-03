use super::{MetricLabel, PortError};
use crate::SharedString;
use either::{Either, Left, Right};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StageError {
    #[error("failure while materializing graph stage value: {0}")]
    Materialization(String),

    #[error("{0}")]
    Port(#[from] PortError),
}

impl MetricLabel for StageError {
    fn slug(&self) -> SharedString {
        "stage".into()
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            Self::Materialization(_) => Left("materialization".into()),
            Self::Port(e) => Right(Box::new(e)),
        }
    }
}
