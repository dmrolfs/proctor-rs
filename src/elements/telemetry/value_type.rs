use std::fmt;

use serde::{Deserialize, Serialize};

use super::TelemetryValue;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TelemetryValueType {
    Boolean,
    Integer,
    Float,
    Text,
    Seq,
    Table,
    Unit,
}

impl fmt::Display for TelemetryValueType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Self::Boolean => write!(f, "boolean"),
            Self::Integer => write!(f, "integer"),
            Self::Float => write!(f, "floating point"),
            Self::Text => write!(f, "text"),
            Self::Seq => write!(f, "sequence"),
            Self::Table => write!(f, "table"),
            Self::Unit => write!(f, "unit value"),
        }
    }
}

impl From<TelemetryValue> for TelemetryValueType {
    fn from(value: TelemetryValue) -> Self {
        (&value).into()
    }
}

impl From<&TelemetryValue> for TelemetryValueType {
    fn from(value: &TelemetryValue) -> Self {
        use crate::elements::TelemetryValue as TV;

        match value {
            TV::Boolean(_) => Self::Boolean,
            TV::Integer(_) => Self::Integer,
            TV::Float(_) => Self::Float,
            TV::Text(_) => Self::Text,
            TV::Seq(_) => Self::Seq,
            TV::Table(_) => Self::Table,
            TV::Unit => Self::Unit,
        }
    }
}
