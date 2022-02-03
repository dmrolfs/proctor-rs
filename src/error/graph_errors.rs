use super::StageError;
use super::{MetricLabel, PortError};
use crate::SharedString;
use either::{Either, Left, Right};
use thiserror::Error;

#[derive(Debug, Error)]
#[allow(clippy::large_enum_variant)]
pub enum GraphError {
    // #[error("{0}")]
    // Policy(#[from] PolicyError),
    #[error("{0}")]
    Stage(#[from] StageError),

    #[error("Could not join task handle: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("{0}")]
    Port(#[from] PortError),
}

impl MetricLabel for GraphError {
    fn slug(&self) -> SharedString {
        "graph".into()
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            // Self::Policy(e) => Right(Box::new(e)),
            Self::Stage(e) => Right(Box::new(e)),
            Self::Join(_) => Left("join".into()),
            Self::Port(e) => Right(Box::new(e)),
        }
    }
}
