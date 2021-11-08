use either::{Either, Left, Right};
use std::convert::Infallible;
use std::fmt;
use std::fmt::Debug;

use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::elements::TelemetryValue;
use crate::phases::collection::SourceSetting;
use crate::SharedString;

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
    PhaseError(#[from] anyhow::Error),

    #[error("{0}")]
    GraphError(#[from] GraphError),

    #[error("{0}")]
    PolicyError(#[from] PolicyError),

    #[error("{0}")]
    PrometheusError(#[from] prometheus::Error),
}

impl MetricLabel for ProctorError {
    fn slug(&self) -> SharedString {
        "proctor".into()
    }
    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            Self::CollectionError(e) => Right(Box::new(e)),
            Self::EligibilityError(e) => Right(Box::new(e)),
            Self::DecisionError(e) => Right(Box::new(e)),
            Self::GovernanceError(e) => Right(Box::new(e)),
            Self::PlanError(e) => Right(Box::new(e)),
            Self::PhaseError(_) => Left("phase".into()),
            Self::GraphError(e) => Right(Box::new(e)),
            Self::PolicyError(e) => Right(Box::new(e)),
            Self::PrometheusError(_) => Left("prometheus".into()),
        }
    }
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

impl MetricLabel for GraphError {
    fn slug(&self) -> SharedString {
        "graph".into()
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            Self::PolicyError(e) => Right(Box::new(e)),
            Self::StageError(e) => Right(Box::new(e)),
            Self::JoinError(_) => Left("join".into()),
            Self::PortError(e) => Right(Box::new(e)),
        }
    }
}

#[derive(Debug, Error)]
pub enum IncompatibleSourceSettingsError {
    #[error("expected {expected} source settings but got: {settings:?}")]
    ExpectedTypeError { expected: String, settings: SourceSetting },

    #[error("{0}")]
    InvalidDetailError(#[source] anyhow::Error),

    #[error("{0}")]
    ConfigurationParseError(#[source] anyhow::Error),
}

impl MetricLabel for IncompatibleSourceSettingsError {
    fn slug(&self) -> SharedString {
        "incompatible_source_settings".into()
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            _e @ Self::ExpectedTypeError { .. } => Left("expected_type".into()),
            Self::InvalidDetailError(_) => Left("invalid_detail".into()),
            Self::ConfigurationParseError(_) => Left("configuration_parse".into()),
        }
    }
}

impl From<reqwest::header::InvalidHeaderName> for IncompatibleSourceSettingsError {
    fn from(that: reqwest::header::InvalidHeaderName) -> Self {
        IncompatibleSourceSettingsError::InvalidDetailError(that.into())
    }
}

impl From<reqwest::header::InvalidHeaderValue> for IncompatibleSourceSettingsError {
    fn from(that: reqwest::header::InvalidHeaderValue) -> Self {
        IncompatibleSourceSettingsError::InvalidDetailError(that.into())
    }
}

/// Set of errors occurring during telemetry collection
#[derive(Debug, Error)]
pub enum CollectionError {
    #[error("{0}")]
    IncompatibleSettings(#[from] IncompatibleSourceSettingsError),

    /// An error related to collecting from a CVS file.
    #[error("{0}")]
    CsvError(#[from] csv::Error),

    /// An error related to collection via an HTTP source.
    #[error("{0}")]
    HttpError(#[from] reqwest::Error),

    /// An error related to collection via an HTTP middleware.
    #[error("{0}")]
    HttpMiddlewareError(#[from] reqwest_middleware::Error),

    #[error("Attempt to send via a closed subscription channel: {0}")]
    ClosedSubscription(String),

    #[error("data not found at key, {0}")]
    DataNotFound(String),

    #[error("failed to parse decision from {0}")]
    DecisionError(String),

    #[error("{0}")]
    TelemetryError(#[from] TelemetryError),

    #[error("{0}")]
    PortError(#[from] PortError),

    #[error("{0}")]
    StageError(#[from] anyhow::Error),
}

impl MetricLabel for CollectionError {
    fn slug(&self) -> SharedString {
        "collection".into()
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            Self::IncompatibleSettings(e) => Right(Box::new(e)),
            Self::CsvError(_) => Left("csv".into()),
            Self::HttpError(_) => Left("http".into()),
            Self::HttpMiddlewareError(_) => Left("http_middleware".into()),
            Self::ClosedSubscription(_) => Left("closed_subscription".into()),
            Self::DataNotFound(_) => Left("data_not_found".into()),
            Self::DecisionError(_) => Left("decision".into()),
            Self::TelemetryError(e) => Right(Box::new(e)),
            Self::PortError(e) => Right(Box::new(e)),
            Self::StageError(_) => Left("stage".into()),
        }
    }
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

    #[error("collection error during eligibility phase: {0}")]
    CollectionError(#[from] CollectionError),

    #[error("{0}")]
    StageError(#[from] anyhow::Error),
}

impl MetricLabel for EligibilityError {
    fn slug(&self) -> SharedString {
        "eligibility".into()
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            Self::DataNotFound(_) => Left("data_not_found".into()),
            Self::ParseError(_) => Left("parse".into()),
            Self::TelemetryError(e) => Right(Box::new(e)),
            Self::PortError(e) => Right(Box::new(e)),
            Self::CollectionError(e) => Right(Box::new(e)),
            Self::StageError(_) => Left("stage".into()),
        }
    }
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

impl MetricLabel for DecisionError {
    fn slug(&self) -> SharedString {
        "decision".into()
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            Self::DataNotFound(_) => Left("data_not_found".into()),
            Self::ParseError(_) => Left("parse".into()),
            Self::TelemetryError(e) => Right(Box::new(e)),
            Self::PortError(e) => Right(Box::new(e)),
            Self::StageError(_) => Left("stage".into()),
        }
    }
}

#[derive(Debug, Error)]
pub enum GovernanceError {
    #[error("{0}")]
    PortError(#[from] PortError),

    #[error("{0}")]
    TelemetryError(#[from] TelemetryError),

    #[error("{0}")]
    StageError(#[from] anyhow::Error),
}

impl MetricLabel for GovernanceError {
    fn slug(&self) -> SharedString {
        "governance".into()
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            Self::PortError(e) => Right(Box::new(e)),
            Self::TelemetryError(e) => Right(Box::new(e)),
            Self::StageError(_) => Left("stage".into()),
        }
    }
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

impl MetricLabel for PlanError {
    fn slug(&self) -> SharedString {
        "plan".into()
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            Self::DataNotFound(_) => Left("data_not_found".into()),
            Self::ParseError(_) => Left("parse".into()),
            Self::TelemetryError(e) => Right(Box::new(e)),
            Self::PortError(e) => Right(Box::new(e)),
            Self::ForecastError(_) => Left("forecast".into()),
            _e @ Self::NotEnoughData { .. } => Left("not_enough_data".into()),
            Self::DurationLimitExceeded(_) => Left("duration_limit_exceeded".into()),
            Self::ZeroDuration(_) => Left("zero_duration".into()),
            Self::RegressionFailed(_) => Left("regression_failed".into()),
            Self::StageError(_) => Left("stage".into()),
            Self::IOError(_) => Left("io".into()),
            Self::SerdeError(_) => Left("serde".into()),
        }
    }
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
    PolicyParseError(#[from] polar_core::error::PolarError),

    /// Error in string policy parsing
    #[error("Failed to parse string policy: {0}")]
    StringPolicyError(String),

    #[error("Failed to render policy from template: {0}")]
    RenderError(#[from] handlebars::RenderError),

    #[error("Failed to register template with policy source registry: {0}")]
    TemplateError(#[from] handlebars::TemplateError),
    /// Error in using telemetry data in policies.
    #[error("failed to pull policy data from telemetry: {0}")]
    TelemetryError(#[from] TelemetryError),

    // #[error("policy data, {0}, not found")]
    // DataNotFound(String),
    #[error("failed to publish policy event: {0}")]
    PublishError(#[source] anyhow::Error),

    #[error("policy error : {0}")]
    AnyError(#[from] anyhow::Error),
}

impl MetricLabel for PolicyError {
    fn slug(&self) -> SharedString {
        "policy".into()
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            Self::IOError(_) => Left("io".into()),
            Self::EngineError(_) => Left("engine".into()),
            Self::PolicyParseError(_) => Left("parsing".into()),
            Self::StringPolicyError(_) => Left("string".into()),
            Self::RenderError(_) | Self::TemplateError(_) => Left("template".into()),
            Self::TelemetryError(e) => Right(Box::new(e)),
            Self::PublishError(_) => Left("publish".into()),
            Self::AnyError(_) => Left("any".into()),
        }
    }
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

impl MetricLabel for TelemetryError {
    fn slug(&self) -> SharedString {
        "telemetry".into()
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            _e @ Self::TypeError { .. } => Left("type".into()),
            _e @ Self::ExpectedTypeError(..) => Left("expected_type".into()),
            Self::SerializationError(_) => Left("serializaton".into()),
            Self::DeserializationError(_) => Left("deserialization".into()),
            Self::ReaderError(_) => Left("reader".into()),
            Self::ConvertInfallible(_) => Left("convert_infallible".into()),
            Self::ValueParseError(_) => Left("value_parse".into()),
            Self::NotSupported(_) => Left("not_supported".into()),
        }
    }
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

impl MetricLabel for StageError {
    fn slug(&self) -> SharedString {
        "stage".into()
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            Self::MaterializationError(_) => Left("materialization".into()),
            Self::PortError(e) => Right(Box::new(e)),
        }
    }
}

#[derive(Debug, Error)]
pub enum PortError {
    #[error("cannot use detached port, {0}.")]
    Detached(String),

    /// error occurred while attempting to send across a sync channel.
    #[error("{0:?}")]
    ChannelError(#[source] anyhow::Error),
}

impl MetricLabel for PortError {
    fn slug(&self) -> SharedString {
        "port".into()
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            Self::Detached(_) => Left("detached".into()),
            Self::ChannelError(_) => Left("channel".into()),
        }
    }
}

impl<T: 'static + Debug + Send + Sync> From<tokio::sync::mpsc::error::SendError<T>> for PortError {
    fn from(that: tokio::sync::mpsc::error::SendError<T>) -> Self {
        PortError::ChannelError(that.into())
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
