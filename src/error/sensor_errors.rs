use super::{MetricLabel, PortError, TelemetryError};
use crate::phases::sense::SensorSetting;
use crate::SharedString;
use either::{Either, Left, Right};
use thiserror::Error;

/// Set of errors occurring while sensing target environment
#[derive(Debug, Error)]
pub enum SenseError {
    #[error("{0}")]
    IncompatibleSettings(#[from] IncompatibleSensorSettings),

    /// An error related to collecting sensor data from a CVS file.
    #[error("Could not properly load CSV source: {0}")]
    CSV(#[from] csv::Error),

    /// An error requesting data from a HTTP sensor.
    #[error("Could not properly load HTTP source: {0}")]
    HttpRequest(#[from] reqwest::Error),

    /// An error related to sensor HTTP middleware.
    #[error("Error occurred in HTTP middleware during HTTP source load: {0}")]
    HttpMiddleware(#[from] reqwest_middleware::Error),

    /// Error parsing URLs
    #[error("failed to parse sensor url: {0}")]
    UrlParse(#[from] url::ParseError),

    /// Error processing JSON
    #[error("Error processing source JSON: {0}")]
    JSON(#[from] serde_json::Error),

    #[error("Attempt to send via a closed subscription channel: {0}")]
    ClosedSubscription(String),

    #[error("data not found at key, {0}")]
    DataNotFound(String),

    // #[error("failed to parse decision from {0}")]
    // DecisionError(String),
    #[error("{0}")]
    Telemetry(#[from] TelemetryError),

    #[error("{0}")]
    PortError(#[from] PortError),

    #[error("supplied sense url cannot be a base to query: {0}")]
    NotABaseUrl(url::Url),

    #[error("{0}")]
    Task(#[from] tokio::task::JoinError),

    #[error("{0}")]
    Stage(#[from] anyhow::Error),
}

impl MetricLabel for SenseError {
    fn slug(&self) -> SharedString {
        "sense".into()
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            Self::IncompatibleSettings(e) => Right(Box::new(e)),
            Self::CSV(_) => Left("csv".into()),
            Self::HttpRequest(_) => Left("http::request".into()),
            Self::HttpMiddleware(_) => Left("http::middleware".into()),
            Self::UrlParse(_) => Left("http::url".into()),
            Self::JSON(_) => Left("http::json".into()),
            Self::ClosedSubscription(_) => Left("closed_subscription".into()),
            Self::DataNotFound(_) => Left("data_not_found".into()),
            // Self::DecisionError(_) => Left("decision".into()),
            Self::Telemetry(e) => Right(Box::new(e)),
            Self::PortError(e) => Right(Box::new(e)),
            Self::NotABaseUrl(_) => Left("http::url".into()),
            Self::Task(_) => Left("task".into()),
            Self::Stage(_) => Left("stage".into()),
        }
    }
}

#[derive(Debug, Error)]
pub enum IncompatibleSensorSettings {
    #[error("failed to parse sesnor url: {0}")]
    UrlParse(#[from] url::ParseError),

    #[error("sesnor url cannot be a basis for http requests: {0}")]
    UrlCannotBeBase(url::Url),

    #[error("expected {expected} sensor settings but got: {settings:?}")]
    ExpectedTypeError { expected: String, settings: SensorSetting },

    #[error("{0}")]
    InvalidRequestHeaderDetail(#[source] anyhow::Error),

    #[error("{0}")]
    ConfigurationParse(#[source] anyhow::Error),
}

impl MetricLabel for IncompatibleSensorSettings {
    fn slug(&self) -> SharedString {
        "sensor_settings".into()
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            _e @ Self::ExpectedTypeError { .. } => Left("expected_type".into()),
            Self::InvalidRequestHeaderDetail(_) => Left("http::invalid_request_header_detail".into()),
            Self::UrlCannotBeBase(_) | Self::UrlParse(_) => Left("http::url::parse".into()),
            Self::ConfigurationParse(_) => Left("configuration_parse".into()),
        }
    }
}

impl From<reqwest::header::InvalidHeaderName> for IncompatibleSensorSettings {
    fn from(that: reqwest::header::InvalidHeaderName) -> Self {
        Self::InvalidRequestHeaderDetail(that.into())
    }
}

impl From<reqwest::header::InvalidHeaderValue> for IncompatibleSensorSettings {
    fn from(that: reqwest::header::InvalidHeaderValue) -> Self {
        Self::InvalidRequestHeaderDetail(that.into())
    }
}
