use crate::error::{TelemetryError, TypeExpectation};
use config::Value as ConfigValue;
use oso::{FromPolar, PolarValue, ToPolar};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{de, Serialize, Serializer};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, LinkedList, VecDeque};
use std::convert::TryFrom;
use std::fmt;
use std::num::{ParseFloatError, ParseIntError};

#[derive(Debug, Clone)]
pub enum TelemetryValue {
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Text(String),
    Seq(Seq),
    Table(Table),
    Unit,
}

pub type Seq = Vec<TelemetryValue>;
pub type Table = HashMap<String, TelemetryValue>;

impl TelemetryValue {
    #[tracing::instrument(level = "trace")]
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Unit => true,
            Self::Text(rep) => rep.is_empty(),
            Self::Seq(rep) => rep.is_empty(),
            Self::Table(rep) => rep.is_empty(),
            _ => false,
        }
    }

    #[tracing::instrument(level = "trace")]
    pub fn extend(&mut self, that: &TelemetryValue) {
        use std::ops::BitOr;

        match (self, that) {
            (Self::Unit, Self::Unit) => (),
            (Self::Boolean(ref mut lhs), Self::Boolean(rhs)) => {
                *lhs = lhs.bitor(rhs);
            }
            (Self::Integer(ref mut lhs), Self::Integer(rhs)) => *lhs = *lhs + *rhs,
            (Self::Float(ref mut lhs), Self::Float(rhs)) => *lhs = *lhs + *rhs,
            (Self::Text(ref mut lhs), Self::Text(rhs)) => lhs.extend(rhs.chars()),
            (Self::Seq(lhs), Self::Seq(ref rhs)) => lhs.extend(rhs.clone()),
            (Self::Table(lhs), Self::Table(rhs)) => lhs.extend(rhs.clone()),
            (lhs, rhs) => panic!("mismatched telemetry merge types: {:?} + {:?}.", lhs, rhs),
        };
    }
}

impl Default for TelemetryValue {
    #[tracing::instrument(level = "trace")]
    fn default() -> Self {
        Self::Unit
    }
}

impl fmt::Display for TelemetryValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unit => write!(f, "unit"),
            Self::Boolean(value) => write!(f, "{}", value),
            Self::Integer(value) => write!(f, "{}", value),
            Self::Float(value) => write!(f, "{}", value),
            Self::Text(value) => write!(f, "{}", value),
            Self::Seq(values) => write!(f, "{:?}", values),
            Self::Table(values) => write!(f, "{:?}", values),
        }
    }
}

impl PartialEq for TelemetryValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Boolean(lhs), Self::Boolean(rhs)) => lhs == rhs,
            (Self::Integer(lhs), Self::Integer(rhs)) => lhs == rhs,
            (Self::Float(lhs), Self::Float(rhs)) => lhs == rhs,
            (Self::Text(lhs), Self::Text(rhs)) => lhs == rhs,
            (Self::Seq(lhs), Self::Seq(rhs)) => lhs == rhs,
            (Self::Table(lhs), Self::Table(rhs)) => lhs == rhs,
            _ => false,
        }
    }
}

impl Eq for TelemetryValue {}

impl ToPolar for TelemetryValue {
    #[tracing::instrument(level = "trace")]
    fn to_polar(self) -> PolarValue {
        match self {
            Self::Integer(value) => PolarValue::Integer(value),
            Self::Float(value) => PolarValue::Float(value),
            Self::Boolean(value) => PolarValue::Boolean(value),
            Self::Text(rep) => PolarValue::String(rep),
            Self::Seq(values) => {
                let vs = values.into_iter().map(|v| v.to_polar()).collect();
                PolarValue::List(vs)
            }
            Self::Table(table) => {
                let vs = table.into_iter().map(|(k, v)| (k, v.to_polar())).collect();
                PolarValue::Map(vs)
            }
            Self::Unit => PolarValue::Boolean(false),
        }
    }
}

impl FromPolar for TelemetryValue {
    #[tracing::instrument(level = "trace")]
    fn from_polar(polar: PolarValue) -> oso::Result<Self> {
        TelemetryValue::try_from(polar).map_err(|_| oso::errors::OsoError::FromPolar)
    }
}

impl Into<ConfigValue> for TelemetryValue {
    #[tracing::instrument(level = "trace")]
    fn into(self) -> ConfigValue {
        match self {
            Self::Integer(value) => ConfigValue::new(None, value),
            Self::Float(value) => ConfigValue::new(None, value),
            Self::Boolean(value) => ConfigValue::new(None, value),
            Self::Text(rep) => ConfigValue::new(None, rep),
            Self::Seq(values) => {
                let vs: Vec<ConfigValue> = values.into_iter().map(|v| v.into()).collect();
                ConfigValue::new(None, vs)
            }
            Self::Table(table) => {
                let tbl: HashMap<String, ConfigValue> =
                    table.into_iter().map(|(k, v)| (k, v.into())).collect::<HashMap<_, _>>();
                ConfigValue::new(None, tbl)
            }
            Self::Unit => ConfigValue::new(None, Option::<String>::None),
        }
    }
}

impl From<bool> for TelemetryValue {
    fn from(value: bool) -> Self {
        TelemetryValue::Boolean(value)
    }
}

impl From<usize> for TelemetryValue {
    fn from(value: usize) -> Self {
        TelemetryValue::Integer(value as i64)
    }
}

macro_rules! from_int_to_telemetry {
    ($i:ty) => {
        impl From<$i> for TelemetryValue {
            fn from(value: $i) -> Self {
                Self::Integer(value.into())
            }
        }
    };
}

from_int_to_telemetry!(u8);
from_int_to_telemetry!(i8);
from_int_to_telemetry!(u16);
from_int_to_telemetry!(i16);
from_int_to_telemetry!(u32);
from_int_to_telemetry!(i32);
from_int_to_telemetry!(i64);

macro_rules! from_float_to_telemetry {
    ($f:ty) => {
        impl From<$f> for TelemetryValue {
            fn from(value: $f) -> Self {
                Self::Float(value.into())
            }
        }
    };
}

from_float_to_telemetry!(f32);
from_float_to_telemetry!(f64);

impl From<String> for TelemetryValue {
    fn from(rep: String) -> Self {
        Self::Text(rep)
    }
}

impl<'a> From<&'a str> for TelemetryValue {
    fn from(rep: &'a str) -> Self {
        Self::Text(rep.to_string())
    }
}

impl<T: Into<TelemetryValue>> From<Vec<T>> for TelemetryValue {
    fn from(values: Vec<T>) -> Self {
        Self::Seq(values.into_iter().map(|v| v.into()).collect())
    }
}

impl<T: Into<TelemetryValue>> From<VecDeque<T>> for TelemetryValue {
    fn from(values: VecDeque<T>) -> Self {
        Self::Seq(values.into_iter().map(|v| v.into()).collect())
    }
}

impl<T: Into<TelemetryValue>> From<LinkedList<T>> for TelemetryValue {
    fn from(values: LinkedList<T>) -> Self {
        Self::Seq(values.into_iter().map(|v| v.into()).collect())
    }
}

impl<T: Into<TelemetryValue>> From<HashSet<T>> for TelemetryValue {
    fn from(values: HashSet<T>) -> Self {
        Self::Seq(values.into_iter().map(|v| v.into()).collect())
    }
}

impl<T: Into<TelemetryValue>> From<BTreeSet<T>> for TelemetryValue {
    fn from(values: BTreeSet<T>) -> Self {
        Self::Seq(values.into_iter().map(|v| v.into()).collect())
    }
}

impl<T: Into<TelemetryValue>> From<BinaryHeap<T>> for TelemetryValue {
    fn from(values: BinaryHeap<T>) -> Self {
        Self::Seq(values.into_iter().map(|v| v.into()).collect())
    }
}

impl<'a, T: Clone + Into<TelemetryValue>> From<&'a [T]> for TelemetryValue {
    fn from(values: &'a [T]) -> Self {
        Self::Seq(values.iter().cloned().map(|v| v.into()).collect())
    }
}

impl<T: Into<TelemetryValue>> From<HashMap<String, T>> for TelemetryValue {
    fn from(table: HashMap<String, T>) -> Self {
        Self::Table(table.into_iter().map(|(k, v)| (k, v.into())).collect())
    }
}

impl<T: Into<TelemetryValue>> From<HashMap<&str, T>> for TelemetryValue {
    fn from(table: HashMap<&str, T>) -> Self {
        Self::Table(table.into_iter().map(|(k, v)| (k.to_string(), v.into())).collect())
    }
}

impl<T: Into<TelemetryValue>> From<BTreeMap<String, T>> for TelemetryValue {
    fn from(table: BTreeMap<String, T>) -> Self {
        Self::Table(table.into_iter().map(|(k, v)| (k, v.into())).collect())
    }
}

impl<T: Into<TelemetryValue>> From<BTreeMap<&str, T>> for TelemetryValue {
    fn from(table: BTreeMap<&str, T>) -> Self {
        Self::Table(table.into_iter().map(|(k, v)| (k.to_string(), v.into())).collect())
    }
}

impl<T: Into<TelemetryValue>> From<Option<T>> for TelemetryValue {
    fn from(value: Option<T>) -> Self {
        match value {
            Some(v) => v.into(),
            None => TelemetryValue::Unit,
        }
    }
}

impl From<PolarValue> for TelemetryValue {
    fn from(polar: PolarValue) -> Self {
        // Self::from_polar(polar).expect("failed to convert polar value into telemetry")
        match polar {
            PolarValue::Boolean(value) => Self::Boolean(value),
            PolarValue::Integer(value) => Self::Integer(value),
            PolarValue::Float(value) => Self::Float(value),
            PolarValue::String(rep) => Self::Text(rep),
            PolarValue::List(values) => {
                let vs = values.into_iter().map(|v| v.into()).collect();
                Self::Seq(vs)
            }
            PolarValue::Map(table) => {
                let vs = table.into_iter().map(|(k, v)| (k, v.into())).collect();
                Self::Table(vs)
            }
            PolarValue::Instance(_) => {
                Self::Unit
                // maybe Unit?
                // Err(StageError::TypeError("PolarValue::Instance is not a supported telemetry type.".to_string()))
            }
            PolarValue::Variable(_) => {
                Self::Unit
                // maybe Unit?
                // Err(StageError::TypeError("PolarValue::Variable is not a supported telemetry type.".to_string()))
            }
        }
    }
}

impl TryFrom<TelemetryValue> for bool {
    type Error = TelemetryError;

    fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
        match telemetry {
            TelemetryValue::Boolean(value) => Ok(value),
            TelemetryValue::Integer(value) => Ok(value != 0),
            TelemetryValue::Float(value) => Ok(value != 0.0),

            TelemetryValue::Text(ref value) => {
                match value.to_lowercase().as_ref() {
                    "1" | "true" | "on" | "yes" => Ok(true),
                    "0" | "false" | "off" | "no" => Ok(false),

                    // Unexpected string value
                    rep => Err(TelemetryError::TypeError {
                        expected: format!("{} value", TypeExpectation::Boolean),
                        actual: Some(rep.to_string()),
                    }),
                }
            }

            value => Err(TelemetryError::TypeError {
                expected: format!("{} value", TypeExpectation::Boolean),
                actual: Some(format!("{:?}", value)),
            }),
        }
    }
}

macro_rules! try_from_telemetry_into_int {
    ($i:ty) => {
        impl TryFrom<TelemetryValue> for $i {
            type Error = TelemetryError;

            fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
                match telemetry {
                    TelemetryValue::Integer(i64) => <$i>::try_from(i64).map_err(|_err| TelemetryError::TypeError {
                        expected: format!("{}", stringify!($i)),
                        actual: Some(format!("{:?}", i64)),
                    }),
                    TelemetryValue::Float(f64) => Ok(f64.round() as $i),
                    TelemetryValue::Boolean(b) => Ok(if b { 1 } else { 0 }),
                    TelemetryValue::Text(ref rep) => match rep.to_lowercase().as_ref() {
                        "true" | "on" | "yes" => Ok(1),
                        "false" | "off" | "no" => Ok(0),
                        _ => rep
                            .parse()
                            .map_err(|err: ParseIntError| TelemetryError::ValueParseError(err.into())),
                    },
                    value => Err(TelemetryError::TypeError {
                        expected: format!("a telemetry {}", TypeExpectation::Integer),
                        actual: Some(value.to_string()),
                    }),
                }
            }
        }
    };
}

try_from_telemetry_into_int!(usize);
try_from_telemetry_into_int!(u8);
try_from_telemetry_into_int!(i8);
try_from_telemetry_into_int!(u16);
try_from_telemetry_into_int!(i16);
try_from_telemetry_into_int!(u32);
try_from_telemetry_into_int!(i32);
try_from_telemetry_into_int!(i64);

impl TryFrom<TelemetryValue> for f64 {
    type Error = TelemetryError;
    fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
        match telemetry {
            TelemetryValue::Float(f64) => Ok(f64),
            TelemetryValue::Integer(i64) => Ok(i64 as f64),
            TelemetryValue::Boolean(b) => Ok(if b { 1.0 } else { 0.0 }),
            TelemetryValue::Text(ref rep) => match rep.to_lowercase().as_ref() {
                "true" | "on" | "yes" => Ok(1.0),
                "false" | "off" | "no" => Ok(0.0),
                rep => rep
                    .parse()
                    .map_err(|err: ParseFloatError| TelemetryError::ValueParseError(err.into())),
            },
            value => Err(TelemetryError::TypeError {
                expected: format!("a telemetry {}", TypeExpectation::Float),
                actual: Some(format!("{:?}", value)),
            }),
        }
    }
}

impl TryFrom<TelemetryValue> for f32 {
    type Error = TelemetryError;
    fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
        match telemetry {
            TelemetryValue::Float(f64) => Ok(f64 as f32),
            TelemetryValue::Integer(i64) => Ok(i64 as f32),
            TelemetryValue::Boolean(b) => Ok(if b { 1.0 } else { 0.0 }),
            TelemetryValue::Text(ref rep) => match rep.to_lowercase().as_ref() {
                "true" | "on" | "yes" => Ok(1.0),
                "false" | "off" | "no" => Ok(0.0),
                rep => rep
                    .parse()
                    .map_err(|err: ParseFloatError| TelemetryError::ValueParseError(err.into())),
            },
            _ => Err(TelemetryError::TypeError {
                expected: format!("a telemetry {}", TypeExpectation::Float),
                actual: Some(format!("{:?}", telemetry)),
            }),
        }
    }
}

impl TryFrom<TelemetryValue> for String {
    type Error = TelemetryError;

    fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
        match telemetry {
            TelemetryValue::Text(value) => Ok(value),
            TelemetryValue::Boolean(value) => Ok(value.to_string()),
            TelemetryValue::Integer(value) => Ok(value.to_string()),
            TelemetryValue::Float(value) => Ok(value.to_string()),

            value => Err(TelemetryError::TypeError {
                expected: format!("a telemetry {}", TypeExpectation::Text),
                actual: Some(format!("{:?}", value)),
            }),
        }
    }
}

impl<T: From<TelemetryValue>> TryFrom<TelemetryValue> for HashMap<String, T> {
    type Error = TelemetryError;

    fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
        if let TelemetryValue::Table(value) = telemetry {
            Ok(value.into_iter().map(|(k, v)| (k, v.into())).collect())
        } else {
            Err(TelemetryError::TypeError {
                expected: format!("a telemetry {}", TypeExpectation::Table),
                actual: Some(format!("{:?}", telemetry)),
            })
        }
    }
}

impl<T: From<TelemetryValue>> TryFrom<TelemetryValue> for BTreeMap<String, T> {
    type Error = TelemetryError;

    fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
        if let TelemetryValue::Table(value) = telemetry {
            Ok(value.into_iter().map(|(k, v)| (k, v.into())).collect())
        } else {
            Err(TelemetryError::TypeError {
                expected: format!("a telemetry {}", TypeExpectation::Table),
                actual: Some(format!("{:?}", telemetry)),
            })
        }
    }
}

impl<T: From<TelemetryValue>> TryFrom<TelemetryValue> for Vec<T> {
    type Error = TelemetryError;

    fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
        if let TelemetryValue::Seq(value) = telemetry {
            Ok(value.into_iter().map(|v| v.into()).collect())
        } else {
            Err(TelemetryError::TypeError {
                expected: format!("a telemetry {}", TypeExpectation::Seq),
                actual: Some(format!("{:?}", telemetry)),
            })
        }
    }
}

impl<T: From<TelemetryValue>> TryFrom<TelemetryValue> for VecDeque<T> {
    type Error = TelemetryError;

    fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
        if let TelemetryValue::Seq(value) = telemetry {
            Ok(value.into_iter().map(|v| v.into()).collect())
        } else {
            Err(TelemetryError::TypeError {
                expected: format!("a telemetry {}", TypeExpectation::Seq),
                actual: Some(format!("{:?}", telemetry)),
            })
        }
    }
}

impl<T: From<TelemetryValue>> TryFrom<TelemetryValue> for LinkedList<T> {
    type Error = TelemetryError;

    fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
        if let TelemetryValue::Seq(value) = telemetry {
            Ok(value.into_iter().map(|v| v.into()).collect())
        } else {
            Err(TelemetryError::TypeError {
                expected: format!("a telemetry {}", TypeExpectation::Seq),
                actual: Some(format!("{:?}", telemetry)),
            })
        }
    }
}

impl<T: Eq + std::hash::Hash + From<TelemetryValue>> TryFrom<TelemetryValue> for HashSet<T> {
    type Error = TelemetryError;

    fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
        if let TelemetryValue::Seq(value) = telemetry {
            Ok(value.into_iter().map(|v| v.into()).collect())
        } else {
            Err(TelemetryError::TypeError {
                expected: format!("a telemetry {}", TypeExpectation::Seq),
                actual: Some(format!("{:?}", telemetry)),
            })
        }
    }
}

impl<T: Ord + From<TelemetryValue>> TryFrom<TelemetryValue> for BTreeSet<T> {
    type Error = TelemetryError;

    fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
        if let TelemetryValue::Seq(value) = telemetry {
            Ok(value.into_iter().map(|v| v.into()).collect())
        } else {
            Err(TelemetryError::TypeError {
                expected: format!("a telemetry {}", TypeExpectation::Seq),
                actual: Some(format!("{:?}", telemetry)),
            })
        }
    }
}

impl<T: Ord + From<TelemetryValue>> TryFrom<TelemetryValue> for BinaryHeap<T> {
    type Error = TelemetryError;

    fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
        if let TelemetryValue::Seq(value) = telemetry {
            Ok(value.into_iter().map(|v| v.into()).collect())
        } else {
            Err(TelemetryError::TypeError {
                expected: format!("a telemetry {}", TypeExpectation::Seq),
                actual: Some(format!("{:?}", telemetry)),
            })
        }
    }
}

impl<'de> Serialize for TelemetryValue {
    #[tracing::instrument(level = "trace", skip(serializer))]
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Unit => {
                // serializer.serialize_unit()
                serializer.serialize_unit_variant("TelemetryValue", 6, "Unit")
            }
            Self::Boolean(b) => {
                serializer.serialize_bool(*b)
                // serializer.serialize_newtype_variant("TelemetryValue", 2, "Boolean", b)
            }
            Self::Integer(i) => {
                serializer.serialize_i64(*i)
                // serializer.serialize_newtype_variant("TelemetryValue", 0, "Integer", i)
            }
            Self::Float(f) => {
                serializer.serialize_f64(*f)
                // serializer.serialize_newtype_variant("TelemetryValue", 1, "Float", f)
            }
            Self::Text(t) => {
                serializer.serialize_str(t.as_str())
                // serializer.serialize_newtype_variant("TelemetryValue", 3, "Text", t)
            }
            Self::Seq(values) => {
                let mut seq = serializer.serialize_seq(Some(values.len()))?;
                for element in values {
                    seq.serialize_element(element)?;
                }
                seq.end()
                // serializer.serialize_newtype_variant("TelemetryValue",4, "Seq", values)
            }
            Self::Table(table) => {
                let mut map = serializer.serialize_map(Some(table.len()))?;
                for (k, v) in table {
                    map.serialize_entry(k, v)?;
                }
                map.end()
                // serializer.serialize_newtype_variant("TelemetryValue",5, "Table", table)
            }
        }
    }
}

impl<'de> de::Deserialize<'de> for TelemetryValue {
    #[inline]
    #[tracing::instrument(level = "info", skip(deserializer))]
    fn deserialize<D>(deserializer: D) -> ::std::result::Result<TelemetryValue, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        #[derive(Debug)]
        struct ValueVisitor;

        impl<'de> de::Visitor<'de> for ValueVisitor {
            type Value = TelemetryValue;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("any valid telemetry value")
            }

            #[inline]
            #[tracing::instrument(level = "info")]
            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
                tracing::info!(?value, "deserializing bool value");
                Ok(TelemetryValue::Boolean(value))
            }

            #[inline]
            #[tracing::instrument(level = "info")]
            fn visit_i8<E>(self, value: i8) -> Result<Self::Value, E> {
                tracing::info!(?value, "deserializing i8 value");
                Ok(TelemetryValue::Integer(value as i64))
            }

            #[inline]
            #[tracing::instrument(level = "info")]
            fn visit_i16<E>(self, value: i16) -> Result<Self::Value, E> {
                tracing::info!(?value, "deserializing i16 value");
                Ok(TelemetryValue::Integer(value as i64))
            }

            #[inline]
            #[tracing::instrument(level = "info")]
            fn visit_i32<E>(self, value: i32) -> Result<Self::Value, E> {
                tracing::info!(?value, "deserializing i32 value");
                Ok(TelemetryValue::Integer(value as i64))
            }

            #[inline]
            #[tracing::instrument(level = "info")]
            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
                tracing::info!(?value, "deserializing i64 value");
                Ok(TelemetryValue::Integer(value))
            }

            #[inline]
            #[tracing::instrument(level = "info")]
            fn visit_u8<E>(self, value: u8) -> Result<Self::Value, E> {
                tracing::info!(?value, "deserializing u8 value");
                Ok(TelemetryValue::Integer(value as i64))
            }

            #[inline]
            #[tracing::instrument(level = "info")]
            fn visit_u16<E>(self, value: u16) -> Result<Self::Value, E> {
                tracing::info!(?value, "deserializing u16 value");
                Ok(TelemetryValue::Integer(value as i64))
            }

            #[inline]
            #[tracing::instrument(level = "error")]
            fn visit_u32<E>(self, value: u32) -> Result<Self::Value, E> {
                tracing::error!(?value, "deserializing u32 value");
                Ok(TelemetryValue::Integer(value as i64))
            }

            #[inline]
            #[tracing::instrument(level = "info")]
            fn visit_f32<E>(self, value: f32) -> Result<Self::Value, E> {
                tracing::info!(?value, "deserializing f32 value");
                Ok(TelemetryValue::Float(value as f64))
            }
            #[inline]
            #[tracing::instrument(level = "info")]
            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
                tracing::info!(?value, "deserializing f64 value");
                Ok(TelemetryValue::Float(value))
            }

            #[inline]
            #[tracing::instrument(level = "info")]
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                tracing::info!(?value, "deserializing &str value");
                self.visit_string(String::from(value))
            }

            #[inline]
            #[tracing::instrument(level = "info")]
            fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
                tracing::info!(?value, "deserializing string value");
                Ok(TelemetryValue::Text(value))
            }

            #[inline]
            #[tracing::instrument(level = "info")]
            fn visit_none<E>(self) -> ::std::result::Result<TelemetryValue, E> {
                tracing::info!("deserializing None value");
                Ok(TelemetryValue::Unit)
            }

            #[inline]
            #[tracing::instrument(level = "info", skip(deserializer))]
            fn visit_some<D>(self, deserializer: D) -> ::std::result::Result<TelemetryValue, D::Error>
            where
                D: de::Deserializer<'de>,
            {
                tracing::info!("deserializing Some value");
                de::Deserialize::deserialize(deserializer)
            }

            #[inline]
            #[tracing::instrument(level = "info")]
            fn visit_unit<E>(self) -> ::std::result::Result<TelemetryValue, E> {
                tracing::info!("deserializing unit value");
                Ok(TelemetryValue::Unit)
            }

            #[inline]
            #[tracing::instrument(level = "info", skip(visitor))]
            fn visit_seq<V>(self, mut visitor: V) -> ::std::result::Result<TelemetryValue, V::Error>
            where
                V: de::SeqAccess<'de>,
            {
                let mut vec = Seq::new();

                while let Some(elem) = visitor.next_element()? {
                    tracing::info!(value=?elem, "adding deserialized seq item");
                    vec.push(elem);
                }

                Ok(TelemetryValue::Seq(vec))
            }

            #[tracing::instrument(level = "info", skip(visitor))]
            fn visit_map<V>(self, mut visitor: V) -> ::std::result::Result<TelemetryValue, V::Error>
            where
                V: de::MapAccess<'de>,
            {
                let mut table = Table::new();
                while let Some((key, value)) = visitor.next_entry()? {
                    tracing::info!(?key, ?value, "visiting next entry");
                    match value {
                        TelemetryValue::Unit => (),
                        val => {
                            tracing::info!(?key, value=?val, "adding deserialized entry.");
                            table.insert(key, val);
                        }
                    }
                }

                Ok(TelemetryValue::Table(table))
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fmt::Debug;
    use pretty_assertions::assert_eq;
    use serde::{Deserialize, Serialize};
    use serde_test::{assert_tokens, Token};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Foo {
        bar: TelemetryValue,
    }

    #[test]
    fn test_telemetry_value_integer_serde() {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_telemetry_value_integer_serde");
        let _main_span_guard = main_span.enter();

        let foo = Foo {
            bar: TelemetryValue::Integer(37),
        };
        let json_foo = serde_json::to_string(&foo).unwrap();
        assert_eq!(json_foo, r#"{"bar":37}"#);
        tracing::warn!("deserialize: {}", json_foo);

        // tracing::warn!("asserting tokens...");
        // assert_tokens(
        //     &data,
        //     &vec![
        //         Token::NewtypeStruct {name:"Telemetry"},
        //         Token::NewtypeVariant { name:"TelemetryValue", variant:"Table", },
        //         Token::Map {len:Some(1)},
        //         Token::Str("data"),
        //         Token::NewtypeVariant { name:"TelemetryValue", variant:"Integer",},
        //         Token::I64(33),
        //         Token::MapEnd,
        //     ],
        // );
    }

    #[test]
    fn test_from_into_telemetry_bool() {
        use std::convert::TryFrom;

        let actual_true: TelemetryValue = true.into();
        assert_eq!(actual_true, TelemetryValue::Boolean(true));
        let actual_true_back = bool::try_from(actual_true).unwrap();
        assert_eq!(actual_true_back, true);

        let actual_no: TelemetryValue = "no".into();
        assert_eq!(actual_no, TelemetryValue::Text("no".to_string()));
        let actual_no_back = bool::try_from(actual_no).unwrap();
        assert_eq!(actual_no_back, false);

        let actual_yes: TelemetryValue = "yes".into();
        assert_eq!(actual_yes, TelemetryValue::Text("yes".to_string()));
        let actual_yes_back = bool::try_from(actual_yes).unwrap();
        assert_eq!(actual_yes_back, true);
    }

    #[test]
    fn test_telemetry_value_boolean_serde() {
        let data = TelemetryValue::Boolean(true);
        assert_tokens(
            &data,
            &vec![
                // Token::NewtypeVariant {
                //     name: "TelemetryValue",
                //     variant: "Boolean",
                // },
                Token::Bool(true),
            ],
        )
    }

    #[test]
    fn test_telemetry_value_text_serde() {
        let data = TelemetryValue::Text("Foo Bar Zed".to_string());
        assert_tokens(
            &data,
            &vec![
                // Token::NewtypeVariant {
                //     name: "TelemetryValue",
                //     variant: "Text",
                // },
                Token::Str("Foo Bar Zed"),
            ],
        )
    }

    #[test]
    fn test_telemetry_value_list_serde() {
        let data = TelemetryValue::Seq(vec![
            12.into(),
            std::f64::consts::FRAC_2_SQRT_PI.into(),
            false.into(),
            "2014-11-28T12:45:59.324310806Z".into(),
            vec![
                TelemetryValue::Integer(37),
                TelemetryValue::Float(3.14),
                TelemetryValue::Text("Otis".to_string()),
            ]
            .into(),
            maplit::btreemap! { "foo" => "bar", }.into(),
            // TelemetryValue::Unit,
        ]);
        assert_tokens(
            &data,
            &vec![
                // Token::NewtypeVariant {
                //     name: "TelemetryValue",
                //     variant: "List",
                // },
                Token::Seq { len: Some(6) },
                // Token::NewtypeVariant {
                //     name: "TelemetryValue",
                //     variant: "Integer",
                // },
                Token::I64(12),
                // Token::NewtypeVariant {
                //     name: "TelemetryValue",
                //     variant: "Float",
                // },
                Token::F64(std::f64::consts::FRAC_2_SQRT_PI),
                // Token::NewtypeVariant {
                //     name: "TelemetryValue",
                //     variant: "Boolean",
                // },
                Token::Bool(false),
                // Token::NewtypeVariant {
                //     name: "TelemetryValue",
                //     variant: "Text",
                // },
                Token::Str("2014-11-28T12:45:59.324310806Z"),
                // Token::NewtypeVariant {
                //     name: "TelemetryValue",
                //     variant: "List",
                // },
                Token::Seq { len: Some(3) },
                // Token::NewtypeVariant {
                //     name: "TelemetryValue",
                //     variant: "Integer",
                // },
                Token::I64(37),
                // Token::NewtypeVariant {
                //     name: "TelemetryValue",
                //     variant: "Float",
                // },
                Token::F64(3.14),
                // Token::NewtypeVariant {
                //     name: "TelemetryValue",
                //     variant: "Text",
                // },
                Token::Str("Otis"),
                Token::SeqEnd,
                // Token::NewtypeVariant {
                //     name: "TelemetryValue",
                //     variant: "Map",
                // },
                Token::Map { len: Some(1) },
                Token::Str("foo"),
                // Token::NewtypeVariant {
                //     name: "TelemetryValue",
                //     variant: "Text",
                // },
                Token::Str("bar"),
                Token::MapEnd,
                // Token::UnitVariant {
                //     name: "TelemetryValue",
                //     variant: "Unit",
                // },
                Token::SeqEnd,
            ],
        )
    }

    #[test]
    fn test_telemetry_value_map_serde() {
        let data = TelemetryValue::Table(
            maplit::hashmap! {
                "foo".to_string() => "bar".into(),
                // "zed".to_string() => TelemetryValue::Unit,
            }
            .into(),
        );

        // let expected = vec![
        //     // Token::NewtypeVariant {
        //     //     name: "TelemetryValue",
        //     //     variant: "Map",
        //     // },
        //     Token::Map { len: Some(2) },
        //     Token::Str("foo"),
        //     // Token::NewtypeVariant {
        //     //     name: "TelemetryValue",
        //     //     variant: "Text",
        //     // },
        //     Token::Str("bar"),
        //     // Token::Str("zed"),
        //     // Token::Str("Unit"),
        //     // Token::UnitVariant {
        //     //     name: "TelemetryValue",
        //     //     variant: "Unit",
        //     // },
        //     Token::MapEnd,
        // ];

        // let result = std::panic::catch_unwind(|| {
        assert_tokens(
            &data,
            &vec![
                // Token::NewtypeVariant {
                //     name: "TelemetryValue",
                //     variant: "Map",
                // },
                Token::Map { len: Some(1) },
                Token::Str("foo"),
                // Token::NewtypeVariant {
                //     name: "TelemetryValue",
                //     variant: "Text",
                // },
                Token::Str("bar"),
                // Token::Str("zed"),
                // Token::Str("Unit"),
                // Token::UnitVariant {
                //     name: "TelemetryValue",
                //     variant: "Unit",
                // },
                Token::MapEnd,
            ],
        );
        // });
        // result.unwrap();

        // if result.is_err() {
        //     assert_tokens(
        //         &data,
        //         &vec![
        //             Token::Map { len: Some(2) },
        //             Token::Str("zed"),
        //             Token::Str("Unit"),
        // Token::UnitVariant {
        //     name: "TelemetryValue",
        //     variant: "Unit",
        // },
        // Token::Str("foo"),
        // Token::Str("bar"),
        // Token::MapEnd,
        // ]
        // );
        // }
    }
}

// impl TryFrom<PolarValue> for TelemetryValue {
//     type Error = StageError;
//
//     fn try_from(value: PolarValue) -> Result<Self, Self::Error> {
//         match value {
//             PolarValue::Boolean(value) => Ok(TelemetryValue::Boolean(value)),
//             PolarValue::Integer(value) => Ok(TelemetryValue::Integer(value)),
//             PolarValue::Float(value) => Ok(TelemetryValue::Float(value)),
//             PolarValue::String(rep) => Ok(TelemetryValue::Text(rep)),
//             PolarValue::List(values) => {
//                 let vs = values.into_iter().map(|v| v.into()).collect();
//                 Ok(TelemetryValue::Seq(vs))
//             },
//             PolarValue::Map(table) => {
//                 let vs = table.into_iter().map(|(k, v)| (k, v.into())).collect();
//                 Ok(TelemetryValue::Table(vs))
//             },
//             PolarValue::Instance(_) => {
//                 // maybe Unit?
//                 Err(StageError::TypeError("PolarValue::Instance is not a supported telemetry type.".to_string()))
//             },
//             PolarValue::Variable(_) => {
//                 // maybe Unit?
//                 Err(StageError::TypeError("PolarValue::Variable is not a supported telemetry type.".to_string()))
//             },
//         }
//     }
// }
