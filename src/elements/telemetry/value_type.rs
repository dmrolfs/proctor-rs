use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::TelemetryValue;
use crate::error::TelemetryError;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TelemetryType {
    Boolean,
    Integer,
    Float,
    Text,
    Seq,
    Table,
    Unit,
}

impl fmt::Display for TelemetryType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

impl TelemetryType {
    pub fn cast_telemetry<T>(&self, value: T) -> Result<TelemetryValue, TelemetryError>
    where
        T: Into<TelemetryValue>,
    {
        let telemetry = value.into();
        if self == &telemetry.as_telemetry_type() {
            return Ok(telemetry);
        }

        use self::TelemetryValue as V;

        match (&telemetry, self) {
            (V::Integer(from), Self::Float) => Ok(V::Float(*from as f64)),
            (V::Integer(from), Self::Text) => Ok(V::Text(from.to_string())),

            (V::Float(from), Self::Integer) => Ok(V::Integer(*from as i64)),
            (V::Float(from), Self::Text) => Ok(V::Text(from.to_string())),

            (V::Text(from), Self::Unit) if from.is_empty() => Ok(V::Unit),
            (V::Text(from), Self::Boolean) => Ok(V::Boolean(
                bool::from_str(from.as_str()).map_err(|err| TelemetryError::ValueParse(err.into()))?,
            )),
            (V::Text(from), Self::Integer) => Ok(V::Integer(
                i64::from_str(from.as_str()).map_err(|err| TelemetryError::ValueParse(err.into()))?,
            )),
            (V::Text(from), Self::Float) => Ok(V::Float(
                f64::from_str(from.as_str()).map_err(|err| TelemetryError::ValueParse(err.into()))?,
            )),

            // (V::Table(from), Self::Seq) => {
            //     let items: Vec<(String, TelemetryValue)> = (*from).into_iter().collect();
            //     Ok(V::Seq((*from).into_iter().collect()))
            // },
            (tv, Self::Seq) if tv.as_telemetry_type() != Self::Table => Ok(V::Seq(vec![telemetry])),

            (from, to) => Err(TelemetryError::UnsupportedConversion { from: from.as_telemetry_type(), to: *to }),
        }
    }
}

impl From<TelemetryValue> for TelemetryType {
    fn from(value: TelemetryValue) -> Self {
        (&value).into()
    }
}

impl From<&TelemetryValue> for TelemetryType {
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
