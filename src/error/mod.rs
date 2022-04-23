use std::fmt::Debug;

use either::{Either, Left, Right};
use thiserror::Error;

use crate::SharedString;

mod decision_errors;
mod eligibility_errors;
mod governance_errors;
mod graph_errors;
mod plan_errors;
mod policy_errors;
mod port_errors;
mod sense_errors;
mod stage_errors;
mod telemetry_errors;

pub use decision_errors::DecisionError;
pub use eligibility_errors::EligibilityError;
pub use governance_errors::GovernanceError;
pub use graph_errors::GraphError;
pub use plan_errors::PlanError;
pub use policy_errors::PolicyError;
pub use port_errors::PortError;
pub use sense_errors::{IncompatibleSensorSettings, SenseError};
pub use stage_errors::StageError;
pub use telemetry_errors::{TelemetryError, UnexpectedType};

pub trait MetricLabel {
    fn label(&self) -> SharedString {
        match self.next() {
            Either::Right(n) => format!("{}::{}", self.slug(), n.label()).into(),
            Either::Left(ls) => format!("{}::{}", self.slug(), ls).into(),
        }
    }

    fn slug(&self) -> SharedString;
    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>>;
}

#[derive(Debug, Error)]
pub enum ProctorError {
    #[error("{0}")]
    SensePhase(#[from] SenseError),

    #[error("{0}")]
    EligibilityPhase(#[from] EligibilityError),

    #[error("{0}")]
    DecisionPhase(#[from] DecisionError),

    #[error("{0}")]
    GovernancePhase(#[from] GovernanceError),

    #[error("{0}")]
    PlanPhase(#[from] PlanError),

    #[error("{0}")]
    Phase(#[from] anyhow::Error),

    #[error("{0}")]
    Graph(#[from] GraphError),

    #[error("{0}")]
    Policy(#[from] PolicyError),

    #[error("{0}")]
    Metrics(#[from] prometheus::Error),
}

impl MetricLabel for ProctorError {
    fn slug(&self) -> SharedString {
        "proctor".into()
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            Self::SensePhase(e) => Right(Box::new(e)),
            Self::EligibilityPhase(e) => Right(Box::new(e)),
            Self::DecisionPhase(e) => Right(Box::new(e)),
            Self::GovernancePhase(e) => Right(Box::new(e)),
            Self::PlanPhase(e) => Right(Box::new(e)),
            Self::Phase(_) => Left("phase".into()),
            Self::Graph(e) => Right(Box::new(e)),
            Self::Policy(e) => Right(Box::new(e)),
            Self::Metrics(_) => Left("prometheus".into()),
        }
    }
}

impl From<PortError> for ProctorError {
    fn from(that: PortError) -> Self {
        Self::Graph(that.into())
    }
}

impl From<StageError> for ProctorError {
    fn from(that: StageError) -> Self {
        Self::Graph(that.into())
    }
}

#[derive(Debug, Error)]
pub enum UrlError {
    #[error("failed to parse sesnor url: {0}")]
    UrlParse(#[from] url::ParseError),

    #[error("sesnor url cannot be a basis for http requests: {0}")]
    UrlCannotBeBase(url::Url),
}

impl MetricLabel for UrlError {
    fn slug(&self) -> SharedString {
        "url".into()
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            Self::UrlParse(_) | Self::UrlCannotBeBase(_) => Left("parse".into()),
        }
    }
}
