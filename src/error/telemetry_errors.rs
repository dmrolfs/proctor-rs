use std::convert::Infallible;
use std::fmt;

use either::{Either, Left};
use thiserror::Error;

use super::MetricLabel;
use crate::elements::{TelemetryType, TelemetryValue};

/// Set of errors occurring while handling telemetry data
#[derive(Debug, Error)]
pub enum TelemetryError {
    /// Invalid Type used in application
    #[error("invalid type used, expected {expected} value but was: {actual:?}")]
    TypeError {
        expected: TelemetryType,
        actual: Option<String>,
    },

    #[error("Invalid type used, expected {0}: {1}")]
    ExpectedType(TelemetryType, #[source] anyhow::Error),

    #[error("failed to serialize telemetry data into intermediate form: {0}")]
    Serialization(#[from] flexbuffers::SerializationError),

    #[error("failed to deserialize typed record from intermediate form: {0}")]
    Deserialization(#[from] flexbuffers::DeserializationError),

    #[error("failed to read from intermediate form: {0}")]
    Reader(#[from] flexbuffers::ReaderError),

    #[error("failure during conversion that should not fail: {0}")]
    ConvertInfallible(#[from] Infallible),

    #[error("failed to parse value from telemetry data: {0}")]
    ValueParse(#[source] anyhow::Error),

    #[error("conversion from {from} to {to} is not supported")]
    UnsupportedConversion { from: TelemetryType, to: TelemetryType },

    #[error("type not support {0}")]
    NotSupported(String),
}

impl MetricLabel for TelemetryError {
    fn slug(&self) -> String {
        "telemetry".into()
    }

    fn next(&self) -> Either<String, Box<&dyn MetricLabel>> {
        match self {
            _e @ Self::TypeError { .. } => Left("type".into()),
            _e @ Self::ExpectedType(..) => Left("expected_type".into()),
            Self::Serialization(_) => Left("serializaton".into()),
            Self::Deserialization(_) => Left("deserialization".into()),
            Self::Reader(_) => Left("reader".into()),
            Self::ConvertInfallible(_) => Left("convert_infallible".into()),
            Self::ValueParse(_) => Left("value_parse".into()),
            Self::UnsupportedConversion { .. } => Left("unsupported_conversion".into()),
            Self::NotSupported(_) => Left("not_supported".into()),
        }
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Boolean(b) => write!(f, "boolean `{}`", b),
            Self::Integer(i) => write!(f, "integer `{}`", i),
            Self::Float(v) => write!(f, "floating point `{}`", v),
            Self::Text(ref s) => write!(f, "text {:?}", s),
            Self::Unit => write!(f, "unit value"),
            Self::Seq => write!(f, "list"),
            Self::Table => write!(f, "map"),
        }
    }
}

impl From<crate::elements::TelemetryValue> for UnexpectedType {
    fn from(value: TelemetryValue) -> Self {
        use crate::elements::TelemetryValue as TV;

        match value {
            TV::Boolean(b) => Self::Boolean(b),
            TV::Integer(i64) => Self::Integer(i64),
            TV::Float(f64) => Self::Float(f64),
            TV::Text(rep) => Self::Text(rep),
            TV::Unit => Self::Unit,
            TV::Seq(_) => Self::Seq,
            TV::Table(_) => Self::Table,
        }
    }
}
