use std::convert::Infallible;
use std::fmt;
use std::fmt::Debug;

use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::elements::TelemetryValue;

#[derive(Debug, Error)]
pub enum ProctorError {
    #[error("{0}")]
    CollectionError(#[from] CollectionError),

    #[error("{0}")]
    EligibilityError(#[from] EligibilityError),

    #[error("{0}")]
    DecisionError(#[from] DecisionError),

    #[error("{0}")]
    GovernanceError(#[from] GovernanceError),

    #[error("{0}")]
    PlanError(#[from] PlanError),

    #[error("{0}")]
    GraphError(#[from] GraphError),
}

impl From<PortError> for ProctorError {
    fn from(that: PortError) -> Self {
        ProctorError::GraphError(that.into())
    }
}

impl From<StageError> for ProctorError {
    fn from(that: StageError) -> Self {
        ProctorError::GraphError(that.into())
    }
}

#[derive(Debug, Error)]
pub enum GraphError {
    #[error("{0}")]
    PolicyError(#[from] PolicyError),

    #[error("{0}")]
    StageError(#[from] StageError),

    #[error("{0}")]
    JoinError(#[from] tokio::task::JoinError),

    #[error("{0}")]
    PortError(#[from] PortError),
}

/// Set of errors occurring during telemetry collection
#[derive(Debug, Error)]
pub enum CollectionError {
    /// An error related to collecting from a CVS file.
    #[error("{0}")]
    CsvError(#[from] csv::Error),

    /// An error related to collection via an HTTP source.
    #[error("{0}")]
    HttpError(#[from] reqwest::Error),

    #[error("{0}")]
    SettingsError(#[from] SettingsError),

    #[error("Attempt to send via a closed subscription channel: {0}")]
    ClosedSubscription(String),

    #[error("data not found at key, {0}")]
    DataNotFound(String),

    #[error("failed to parse decision from {0}")]
    ParseError(String),

    #[error("{0}")]
    TelemetryError(#[from] TelemetryError),

    #[error("{0}")]
    PortError(#[from] PortError),

    #[error("{0}")]
    StageError(#[from] anyhow::Error),
}

#[derive(Debug, Error)]
pub enum EligibilityError {
    #[error("data not found at key, {0}")]
    DataNotFound(String),

    #[error("failed to parse decision from {0}")]
    ParseError(String),

    #[error("{0}")]
    TelemetryError(#[from] TelemetryError),

    #[error("{0}")]
    PortError(#[from] PortError),

    #[error("{0}")]
    StageError(#[from] anyhow::Error),
}

#[derive(Debug, Error)]
pub enum DecisionError {
    #[error("data not found at key, {0}")]
    DataNotFound(String),

    #[error("failed to parse decision from {0}")]
    ParseError(String),

    #[error("{0}")]
    TelemetryError(#[from] TelemetryError),

    #[error("{0}")]
    PortError(#[from] PortError),

    #[error("{0}")]
    StageError(#[from] anyhow::Error),
}

#[derive(Debug, Error)]
pub enum GovernanceError {
    #[error("{0}")]
    PortError(#[from] PortError),

    #[error("{0}")]
    StageError(#[from] anyhow::Error),
}

#[derive(Debug, Error)]
pub enum PlanError {
    #[error("data not found at key, {0}")]
    DataNotFound(String),

    #[error("failed to parse decision from {0}")]
    ParseError(String),

    #[error("{0}")]
    TelemetryError(#[from] TelemetryError),

    #[error("{0}")]
    PortError(#[from] PortError),

    #[error("0")]
    ForecastError(anyhow::Error),

    #[error("not enough data to build forecast model - supplied:{supplied} need:{need}")]
    NotEnoughData { supplied: usize, need: usize },

    #[error("duration, {0}ms, exceeds supported limit")]
    DurationLimitExceeded(u128),

    #[error("zero duration for {0} not supported")]
    ZeroDuration(String),

    #[error("failed to solve regression for timestamp {0:?}")]
    RegressionFailed(DateTime<Utc>),

    #[error("{0}")]
    StageError(#[from] anyhow::Error),

    #[error("{0}")]
    IOError(#[from] std::io::Error),

    #[error("{0}")]
    SerdeError(#[from] serde_json::Error),
}

/// Set of errors occurring during policy initialization or evaluation.
#[derive(Debug, Error)]
pub enum PolicyError {
    #[error("{0}")]
    IOError(#[from] std::io::Error),

    /// Error occurred during initialization or execution of the policy engine.
    #[error("{0}")]
    EngineError(#[from] oso::OsoError),

    /// Error in parsing or evaluation of Polar policy.
    #[error("{0}")]
    PolicyError(#[from] polar_core::error::PolarError),

    /// Error in using telemetry data in policies.
    #[error("failed to pull policy data from telemetry: {0}")]
    TelemetryError(#[from] TelemetryError),

    #[error("policy data, {0}, not found")]
    DataNotFound(String),

    #[error("failed to publish policy event: {0}")]
    PublishError(#[source] anyhow::Error), // todo: if only PortError then specialize via #[from]
}

impl From<PortError> for PolicyError {
    fn from(that: PortError) -> Self {
        PolicyError::PublishError(that.into())
    }
}

/// Set of errors occurring while handling telemetry data
#[derive(Debug, Error)]
pub enum TelemetryError {
    /// Invalid Type used in application
    #[error("invalid type used, expected {expected} type but was: {actual:?}")]
    TypeError { expected: String, actual: Option<String> },

    #[error("Invalid type used, expected {0}: {1}")]
    ExpectedTypeError(String, #[source] anyhow::Error),

    #[error("{0}")]
    SerializationError(#[from] flexbuffers::SerializationError),

    #[error("{0}")]
    DeserializationError(#[from] flexbuffers::DeserializationError),

    #[error("{0}")]
    ReaderError(#[from] flexbuffers::ReaderError),

    #[error("{0}")]
    ConvertInfallible(#[from] Infallible),

    #[error("{0}")]
    ValueParseError(#[source] anyhow::Error),

    #[error("type not support {0}")]
    NotSupported(String),
}

#[derive(Debug)]
pub enum TypeExpectation {
    Boolean,
    Integer,
    Float,
    Text,
    Seq,
    Table,
    Unit,
}

impl fmt::Display for TypeExpectation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            TypeExpectation::Boolean => write!(f, "boolean"),
            TypeExpectation::Integer => write!(f, "integer"),
            TypeExpectation::Float => write!(f, "floating point"),
            TypeExpectation::Text => write!(f, "text"),
            TypeExpectation::Seq => write!(f, "sequence"),
            TypeExpectation::Table => write!(f, "table"),
            TypeExpectation::Unit => write!(f, "unit value"),
        }
    }
}

impl From<crate::elements::TelemetryValue> for TypeExpectation {
    fn from(value: TelemetryValue) -> Self {
        use crate::elements::TelemetryValue as TV;

        match value {
            TV::Boolean(_) => TypeExpectation::Boolean,
            TV::Integer(_) => TypeExpectation::Integer,
            TV::Float(_) => TypeExpectation::Float,
            TV::Text(_) => TypeExpectation::Text,
            TV::Seq(_) => TypeExpectation::Seq,
            TV::Table(_) => TypeExpectation::Table,
            TV::Unit => TypeExpectation::Unit,
        }
    }
}

#[derive(Debug, Error)]
pub enum StageError {
    #[error("failure while materializing graph stage value: {0}")]
    MaterializationError(String),

    #[error("{0}")]
    PortError(#[from] PortError),
}

#[derive(Debug, Error)]
pub enum PortError {
    #[error("cannot use detached port, {0}.")]
    Detached(String),

    /// error occurred while attempting to send across a sync channel.
    #[error("{0:?}")]
    ChannelError(#[source] anyhow::Error),
}

impl<T: 'static + Debug + Send + Sync> From<tokio::sync::mpsc::error::SendError<T>> for PortError {
    fn from(that: tokio::sync::mpsc::error::SendError<T>) -> Self {
        PortError::ChannelError(that.into())
    }
}

/// Error variants related to configuration.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SettingsError {
    /// Error working with environment variable
    #[error("{0}")]
    Environment(#[from] std::env::VarError),

    /// Error in configuration settings.
    #[error(transparent)]
    Configuration(#[from] config::ConfigError),

    /// Error in bootstrapping execution from configuration.
    #[error("error during system bootstrap: {message}: {setting}")]
    Bootstrap { message: String, setting: String },

    #[error("{0}")]
    HttpRequestError(#[source] anyhow::Error),

    #[error("{0}")]
    SourceError(#[source] anyhow::Error),

    #[error("{0}")]
    IOError(#[from] std::io::Error),
}

impl From<reqwest::header::InvalidHeaderName> for SettingsError {
    fn from(that: reqwest::header::InvalidHeaderName) -> Self {
        SettingsError::HttpRequestError(that.into())
    }
}

impl From<reqwest::header::InvalidHeaderValue> for SettingsError {
    fn from(that: reqwest::header::InvalidHeaderValue) -> Self {
        SettingsError::HttpRequestError(that.into())
    }
}

impl From<toml::de::Error> for SettingsError {
    fn from(that: toml::de::Error) -> Self {
        SettingsError::SourceError(that.into())
    }
}

#[derive(Debug)]
pub enum UnexpectedType {
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Text(String),
    Unit,
    Seq,
    Table,
}

impl fmt::Display for UnexpectedType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            UnexpectedType::Boolean(b) => write!(f, "boolean `{}`", b),
            UnexpectedType::Integer(i) => write!(f, "integer `{}`", i),
            UnexpectedType::Float(v) => write!(f, "floating point `{}`", v),
            UnexpectedType::Text(ref s) => write!(f, "text {:?}", s),
            UnexpectedType::Unit => write!(f, "unit value"),
            UnexpectedType::Seq => write!(f, "list"),
            UnexpectedType::Table => write!(f, "map"),
        }
    }
}

impl From<crate::elements::TelemetryValue> for UnexpectedType {
    fn from(value: TelemetryValue) -> Self {
        use crate::elements::TelemetryValue as TV;

        match value {
            TV::Boolean(b) => UnexpectedType::Boolean(b),
            TV::Integer(i64) => UnexpectedType::Integer(i64),
            TV::Float(f64) => UnexpectedType::Float(f64),
            TV::Text(rep) => UnexpectedType::Text(rep),
            TV::Unit => UnexpectedType::Unit,
            TV::Seq(_) => UnexpectedType::Seq,
            TV::Table(_) => UnexpectedType::Table,
        }
    }
}
