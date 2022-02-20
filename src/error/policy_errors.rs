use super::{MetricLabel, PortError, TelemetryError};
use crate::SharedString;
use either::{Either, Left, Right};
use thiserror::Error;

/// Set of errors occurring during policy initialization or evaluation.
#[derive(Debug, Error)]
pub enum PolicyError {
    #[error("failed to perform policy IO: {0}")]
    IO(#[from] std::io::Error),

    /// Error occurred during initialization or execution of the policy engine.
    #[error("failure while processing policy in policy engine: {0}")]
    PolicyEngine(#[from] oso::OsoError),

    /// Error in parsing or evaluation of Polar policy.
    #[error("policy engine could not parse policy: {0}")]
    PolicyParse(#[from] polar_core::error::PolarError),

    /// Error in string policy parsing
    #[error("Failed to parse string policy: {0}")]
    StringPolicy(String),

    #[error("Failed to render policy from template: {0}")]
    RenderPolicyTemplate(#[from] handlebars::RenderError),

    #[error("Failed to register template with policy source registry: {0}")]
    PolicyTemplate(#[from] handlebars::TemplateError),
    /// Error in using telemetry data in policies.
    #[error("failed to pull policy data from telemetry: {0}")]
    Telemetry(#[from] TelemetryError),

    #[error("Failure in {0} policy API: {1}")]
    Api(String, #[source] anyhow::Error),

    // #[error("policy data, {0}, not found")]
    // DataNotFound(String),
    #[error("failed to publish policy event: {0}")]
    Publish(#[source] anyhow::Error),

    #[error("policy error : {0}")]
    Other(#[from] anyhow::Error),
}

impl MetricLabel for PolicyError {
    fn slug(&self) -> SharedString {
        "policy".into()
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            Self::IO(_) => Left("io".into()),
            Self::PolicyEngine(_) => Left("engine".into()),
            Self::PolicyParse(_) => Left("parsing".into()),
            Self::StringPolicy(_) => Left("string".into()),
            Self::RenderPolicyTemplate(_) | Self::PolicyTemplate(_) => Left("template".into()),
            Self::Telemetry(e) => Right(Box::new(e)),
            Self::Api(_, _) => Left("api".into()),
            Self::Publish(_) => Left("publish".into()),
            Self::Other(_) => Left("other".into()),
        }
    }
}

impl From<PortError> for PolicyError {
    fn from(that: PortError) -> Self {
        Self::Publish(that.into())
    }
}
