use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, LinkedList, VecDeque};
use std::convert::TryFrom;
use std::fmt;
use std::num::{ParseFloatError, ParseIntError};
use std::ops::{Deref, DerefMut};

use config::Value as ConfigValue;
use oso::{FromPolar, PolarValue, ResultSet, ToPolar};
use pretty_snowflake::{Id, Label, Labeling};
use serde::de::{EnumAccess, Error};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use super::TelemetryType;
use crate::error::TelemetryError;

pub type SeqValue = Vec<TelemetryValue>;

pub type TableType = HashMap<String, TelemetryValue>;

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableValue(pub Box<TableType>);
// pub type Table = Box<HashMap<String, TelemetryValue>>; // to shrink TelemetryValue size

impl TableValue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from(table: TableType) -> Self {
        Self(Box::new(table))
    }
}

impl Deref for TableValue {
    type Target = TableType;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TableValue {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl fmt::Display for TableValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", "{")?;
        let len = self.0.len();
        for (i, (k, v)) in self.iter().enumerate() {
            if i < len - 1 {
                write!(f, "{}->{}, ", k, v)?
            } else {
                write!(f, "{}->{}", k, v)?
            }
        }
        write!(f, "{}", "}")
    }
}

impl From<TableType> for TableValue {
    fn from(that: TableType) -> Self {
        Self::from(that)
    }
}

impl IntoIterator for TableValue {
    type IntoIter = std::collections::hash_map::IntoIter<String, TelemetryValue>;
    type Item = (String, TelemetryValue);

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl ToPolar for TableValue {
    fn to_polar(self) -> PolarValue {
        self.0.to_polar()
    }
}

#[derive(Debug, Clone)]
pub enum TelemetryValue {
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Text(String),
    Seq(SeqValue),
    Table(TableValue),
    Unit,
}

impl fmt::Display for TelemetryValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Integer(v) => write!(f, "{}", v),
            Self::Float(v) => write!(f, "{}", v),
            Self::Boolean(v) => write!(f, "{}", v),
            Self::Text(v) => write!(f, "{}", v),
            Self::Seq(vs) => {
                write!(f, "{}", "[")?;
                let len = vs.len();
                for (i, v) in vs.iter().enumerate() {
                    if i < len - 1 {
                        write!(f, "{}, ", v)?
                    } else {
                        write!(f, "{}", v)?
                    }
                }
                write!(f, "{}", "]")
            },
            Self::Table(vs) => write!(f, "{}", vs),
            Self::Unit => write!(f, "{}", "()"),
        }
    }
}

impl TelemetryValue {
    #[tracing::instrument(level = "trace", skip(values, combine_fn))]
    pub fn combine_values(
        values: impl IntoIterator<Item = TelemetryValue>,
        combine_fn: impl Fn(&TelemetryValue, &TelemetryValue) -> Result<TelemetryValue, TelemetryError>,
    ) -> Result<TelemetryValue, TelemetryError> {
        let mut items: Vec<TelemetryValue> = values.into_iter().collect();
        if let Some(head) = items.pop() {
            items
                .into_iter()
                .fold(Ok(head), |acc, t| acc.and_then(|a| a.combine(t, &combine_fn)))
        } else {
            Ok(TelemetryValue::Unit)
        }
    }

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
            },
            (Self::Integer(ref mut lhs), Self::Integer(rhs)) => *lhs = *lhs + *rhs,
            (Self::Float(ref mut lhs), Self::Float(rhs)) => *lhs = *lhs + *rhs,
            (Self::Text(ref mut lhs), Self::Text(rhs)) => lhs.extend(rhs.chars()),
            (Self::Seq(lhs), Self::Seq(ref rhs)) => lhs.extend(rhs.clone()),
            (Self::Table(lhs), Self::Table(rhs)) => lhs.extend(rhs.clone().into_iter()),
            (lhs, rhs) => panic!("mismatched telemetry merge types: {:?} + {:?}.", lhs, rhs),
        };
    }

    #[tracing::instrument(level = "trace", skip(combine_fn))]
    pub fn combine(
        &self, that: TelemetryValue,
        combine_fn: &impl Fn(&TelemetryValue, &TelemetryValue) -> Result<TelemetryValue, TelemetryError>,
    ) -> Result<TelemetryValue, TelemetryError> {
        use self::TelemetryType as TT;

        let lhs_type = self.as_telemetry_type();

        match lhs_type {
            TT::Unit => Ok(self.clone()),
            TT::Boolean => {
                let rhs = that.try_cast(lhs_type)?;
                combine_fn(self, &rhs)
            },
            TT::Integer => {
                let rhs = that.try_cast(lhs_type)?;
                combine_fn(self, &rhs)
            },
            TT::Float => {
                let rhs = that.try_cast(lhs_type)?;
                combine_fn(self, &rhs)
            },
            TT::Text => {
                let rhs = that.try_cast(lhs_type)?;
                combine_fn(self, &rhs)
            },
            TT::Seq => {
                let rhs = that.try_cast(lhs_type)?;
                combine_fn(self, &rhs)
            },
            TT::Table => {
                let rhs = that.try_cast(lhs_type)?;
                combine_fn(self, &rhs)
            },
        }
    }

    pub fn as_telemetry_type(&self) -> TelemetryType {
        match self {
            Self::Unit => TelemetryType::Unit,
            Self::Boolean(_) => TelemetryType::Boolean,
            Self::Integer(_) => TelemetryType::Integer,
            Self::Float(_) => TelemetryType::Float,
            Self::Text(_) => TelemetryType::Text,
            Self::Seq(_) => TelemetryType::Seq,
            Self::Table(_) => TelemetryType::Table,
        }
    }

    pub fn try_cast(self, cast: TelemetryType) -> Result<TelemetryValue, TelemetryError> {
        cast.cast_telemetry(self)
    }
}

impl Default for TelemetryValue {
    #[tracing::instrument(level = "trace")]
    fn default() -> Self {
        Self::Unit
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
            },
            Self::Table(table) => {
                let vs = table.into_iter().map(|(k, v)| (k, v.to_polar())).collect();
                PolarValue::Map(vs)
            },
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
            },
            Self::Table(table) => {
                let tbl: HashMap<String, ConfigValue> =
                    table.into_iter().map(|(k, v)| (k, v.into())).collect::<HashMap<_, _>>();
                ConfigValue::new(None, tbl)
            },
            Self::Unit => ConfigValue::new(None, Option::<String>::None),
        }
    }
}

impl From<ResultSet> for TelemetryValue {
    fn from(that: ResultSet) -> Self {
        let mut table = TableValue::default();
        for key in that.keys() {
            let typed_value = that
                .get_typed(key)
                .expect("failed to convert polar value into telemetry value");
            match typed_value {
                TelemetryValue::Unit => (),
                value => {
                    let _ = table.insert(key.to_string(), value);
                },
            }
        }

        TelemetryValue::Table(table)
    }
}

// pub const ID_LABEL: &'static str = "label";
pub const ID_SNOWFLAKE: &'static str = "snowflake";
pub const ID_PRETTY: &'static str = "pretty";

impl<T: Label> From<Id<T>> for TelemetryValue {
    fn from(that: Id<T>) -> Self {
        let pretty = format!("{}", that);
        let snowflake: i64 = that.clone().into();
        TelemetryValue::Table(
            maplit::hashmap! {
                // ID_LABEL.to_string() => that.label.into(),
                ID_SNOWFLAKE.to_string() => snowflake.into(),
                ID_PRETTY.to_string() => pretty.into(),
            }
            .into(),
        )
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
    ($($i:ty)*) => {
        $(
            impl From<$i> for TelemetryValue {
                fn from(value: $i) -> Self {
                    Self::Integer(value.into())
                }
            }
        )*
    };
}

from_int_to_telemetry!(u8 i8 u16 i16 u32 i32 i64);

macro_rules! from_float_to_telemetry {
    ($($f:ty)*) => {
        $(
            impl From<$f> for TelemetryValue {
                fn from(value: $f) -> Self {
                    Self::Float(value.into())
                }
            }
        )*
    };
}

from_float_to_telemetry!(f32 f64);

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
        Self::Table(
            table
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect::<HashMap<_, _>>()
                .into(),
        )
    }
}

impl<T: Into<TelemetryValue>> From<HashMap<&str, T>> for TelemetryValue {
    fn from(table: HashMap<&str, T>) -> Self {
        Self::Table(
            table
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.into()))
                .collect::<HashMap<_, _>>()
                .into(),
        )
    }
}

impl<T: Into<TelemetryValue>> From<BTreeMap<String, T>> for TelemetryValue {
    fn from(table: BTreeMap<String, T>) -> Self {
        Self::Table(
            table
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect::<HashMap<_, _>>()
                .into(),
        )
    }
}

impl<T: Into<TelemetryValue>> From<BTreeMap<&str, T>> for TelemetryValue {
    fn from(table: BTreeMap<&str, T>) -> Self {
        Self::Table(
            table
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.into()))
                .collect::<HashMap<_, _>>()
                .into(),
        )
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
            },
            PolarValue::Map(table) => {
                let vs = table
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect::<HashMap<_, _>>()
                    .into();
                Self::Table(vs)
            },
            PolarValue::Instance(_) => {
                Self::Unit
                // maybe Unit?
                // Err(StageError::TypeError("PolarValue::Instance is not a supported telemetry
                // type.".to_string()))
            },
            PolarValue::Variable(_) => {
                Self::Unit
                // maybe Unit?
                // Err(StageError::TypeError("PolarValue::Variable is not a supported telemetry
                // type.".to_string()))
            },
        }
    }
}

/// Converting from TelemetryValue into Id does not consider previously labeling, but rather applies
/// labeling for the deserialized type. So telemetry values coming out of the clearinghouse will
/// have the original unique identifier relabeled for this type instead.
impl<T: Label> TryFrom<TelemetryValue> for Id<T> {
    type Error = TelemetryError;

    #[tracing::instrument(level = "info")]
    fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
        let label = <T as Label>::labeler().label();
        let dmr_label = label.clone();
        tracing::warn!(to_label=%dmr_label, "DMR: converting telemetry into Id<T>");

        let result = match telemetry {
            TelemetryValue::Seq(mut seq) if seq.len() == 2 => {
                // let label = seq.pop().map(String::try_from).transpose()?.unwrap();
                let snowflake = seq.pop().map(i64::try_from).transpose()?.unwrap();
                let pretty = seq.pop().map(String::try_from).transpose()?.unwrap();
                Ok(Self::direct(label, snowflake, pretty))
            },
            TelemetryValue::Table(mut table) => {
                // let label = table.remove(ID_LABEL).map(String::try_from).transpose()?.unwrap();
                let snowflake = table.remove(ID_SNOWFLAKE).map(i64::try_from).transpose()?.unwrap();
                let pretty = table.remove(ID_PRETTY).map(String::try_from).transpose()?.unwrap();
                Ok(Self::direct(label, snowflake, pretty))
            },
            value => Err(TelemetryError::TypeError {
                expected: format!("a telemetry {}", TelemetryType::Table),
                actual: Some(format!("{:?}", value)),
            }),
        };

        tracing::warn!(to_label=%dmr_label, "DMR: converted telemetry into Id<T>: {:#?}", result);
        result
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
                        expected: format!("{} value", TelemetryType::Boolean),
                        actual: Some(rep.to_string()),
                    }),
                }
            },

            value => Err(TelemetryError::TypeError {
                expected: format!("{} value", TelemetryType::Boolean),
                actual: Some(format!("{:?}", value)),
            }),
        }
    }
}

macro_rules! try_from_telemetry_into_int {
    ($($i:ty)*) => {
        $(
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
                            expected: format!("a telemetry {}", TelemetryType::Integer),
                            actual: Some(value.to_string()),
                        }),
                    }
                }
            }
        )*
    };
}

try_from_telemetry_into_int!(usize u8 i8 u16 i16 u32 i32 i64);

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
                expected: format!("a telemetry {}", TelemetryType::Float),
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
                expected: format!("a telemetry {}", TelemetryType::Float),
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
                expected: format!("a telemetry {}", TelemetryType::Text),
                actual: Some(format!("{:?}", value)),
            }),
        }
    }
}

// const SECS: &'static str = crate::serde::date::SECS_KEY;
// const NANOS: &'static str = crate::serde::date::NANOS_KEY;
//
// impl TryFrom<TelemetryValue> for DateTime<Utc> {
//     type Error = TelemetryError;
//
//     fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
//         match telemetry {
//             TelemetryValue::Table(table) => {
//                 let secs = table.get(SECS).cloned().map(|secs| i64::try_from(secs)).transpose()?;
//                 let nanos = table.get(NANOS).cloned().map(|ns| u32::try_from(ns)).transpose()?;
//                 Ok(Utc.timestamp(secs.unwrap_or(0), nanos.unwrap_or(0)))
//             }
//             TelemetryValue::Integer(ts_millis) => Ok(Utc.timestamp_millis(ts_millis)),
//             TelemetryValue::Float(ts_millis) => Ok(Utc.timestamp_millis(ts_millis as i64)),
//             TelemetryValue::Text(rep) => Utc
//                 .datetime_from_str(rep.as_str(), "%+")
//                 .map_err(|err| TelemetryError::ValueParseError(err.into())),
//             value => Err(TelemetryError::TypeError {
//                 expected: format!("a telemetry {}", TypeExpectation::Integer),
//                 actual: Some(format!("{:?}", value)),
//             }),
//         }
//     }
// }

impl<T: From<TelemetryValue>> TryFrom<TelemetryValue> for HashMap<String, T> {
    type Error = TelemetryError;

    fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
        if let TelemetryValue::Table(value) = telemetry {
            Ok(value.into_iter().map(|(k, v)| (k, v.into())).collect())
        } else {
            Err(TelemetryError::TypeError {
                expected: format!("a telemetry {}", TelemetryType::Table),
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
                expected: format!("a telemetry {}", TelemetryType::Table),
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
                expected: format!("a telemetry {}", TelemetryType::Seq),
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
                expected: format!("a telemetry {}", TelemetryType::Seq),
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
                expected: format!("a telemetry {}", TelemetryType::Seq),
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
                expected: format!("a telemetry {}", TelemetryType::Seq),
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
                expected: format!("a telemetry {}", TelemetryType::Seq),
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
                expected: format!("a telemetry {}", TelemetryType::Seq),
                actual: Some(format!("{:?}", telemetry)),
            })
        }
    }
}

impl<'de> Serialize for TelemetryValue {
    #[tracing::instrument(level = "trace", skip(serializer))]
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Unit => serializer.serialize_unit_variant("TelemetryValue", 6, "Unit"),
            Self::Boolean(b) => serializer.serialize_bool(*b),
            Self::Integer(i) => serializer.serialize_i64(*i),
            Self::Float(f) => serializer.serialize_f64(*f),
            Self::Text(t) => serializer.serialize_str(t.as_str()),
            Self::Seq(values) => {
                let mut seq = serializer.serialize_seq(Some(values.len()))?;
                for element in values {
                    seq.serialize_element(element)?;
                }
                seq.end()
            },
            Self::Table(table) => {
                let mut map = serializer.serialize_map(Some(table.len()))?;
                for (k, v) in table.iter() {
                    map.serialize_entry(k, v)?;
                }
                map.end()
            },
        }
    }
}

impl<'de> de::Deserialize<'de> for TelemetryValue {
    #[inline]
    #[tracing::instrument(level = "debug", skip(deserializer))]
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
            #[tracing::instrument(level = "debug")]
            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
                Ok(TelemetryValue::Boolean(value))
            }

            #[inline]
            #[tracing::instrument(level = "debug")]
            fn visit_i8<E>(self, value: i8) -> Result<Self::Value, E> {
                Ok(TelemetryValue::Integer(value as i64))
            }

            #[inline]
            #[tracing::instrument(level = "debug")]
            fn visit_i16<E>(self, value: i16) -> Result<Self::Value, E> {
                Ok(TelemetryValue::Integer(value as i64))
            }

            #[inline]
            #[tracing::instrument(level = "debug")]
            fn visit_i32<E>(self, value: i32) -> Result<Self::Value, E> {
                Ok(TelemetryValue::Integer(value as i64))
            }

            #[inline]
            #[tracing::instrument(level = "debug")]
            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
                Ok(TelemetryValue::Integer(value))
            }

            #[inline]
            #[tracing::instrument(level = "debug")]
            fn visit_u8<E>(self, value: u8) -> Result<Self::Value, E> {
                Ok(TelemetryValue::Integer(value as i64))
            }

            #[inline]
            #[tracing::instrument(level = "debug")]
            fn visit_u16<E>(self, value: u16) -> Result<Self::Value, E> {
                Ok(TelemetryValue::Integer(value as i64))
            }

            #[inline]
            #[tracing::instrument(level = "error")]
            fn visit_u32<E>(self, value: u32) -> Result<Self::Value, E> {
                tracing::error!(?value, "deserializing u32 value");
                Ok(TelemetryValue::Integer(value as i64))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if value <= (i64::MAX as u64) {
                    tracing::trace!(?value, "deserializing u64 as i64 value");
                    Ok(TelemetryValue::Integer(value as i64))
                } else {
                    Err(E::custom(format!(
                        "cannot not deserialize u64 value, {}, into telemetry integer value (maximum: {})",
                        value,
                        i64::MAX
                    )))
                }
            }

            #[inline]
            #[tracing::instrument(level = "debug")]
            fn visit_f32<E>(self, value: f32) -> Result<Self::Value, E> {
                Ok(TelemetryValue::Float(value as f64))
            }

            #[inline]
            #[tracing::instrument(level = "debug")]
            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
                Ok(TelemetryValue::Float(value))
            }

            #[inline]
            #[tracing::instrument(level = "debug")]
            fn visit_char<E>(self, value: char) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(TelemetryValue::Text(String::from(value)))
            }

            #[inline]
            #[tracing::instrument(level = "debug")]
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_string(String::from(value))
            }

            #[inline]
            #[tracing::instrument(level = "debug")]
            fn visit_borrowed_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                self.visit_string(String::from(value))
            }

            #[inline]
            #[tracing::instrument(level = "debug")]
            fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
                Ok(TelemetryValue::Text(value))
            }

            fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                tracing::error!(?value, "deserializing bytes into telemetry is not supported");
                Err(E::custom("deserializing bytes into telemetry is not supported"))
            }

            fn visit_borrowed_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                tracing::error!(?value, "deserializing borrowed bytes into telemetry is not supported");
                Err(E::custom(
                    "deserializing borrowed bytes into telemetry is not supported",
                ))
            }

            fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
            where
                E: Error,
            {
                tracing::error!(?value, "deserializing byte buf into telemetry is not supported");
                Err(E::custom("deserializing byte buf into telemetry is not supported"))
            }

            #[inline]
            #[tracing::instrument(level = "debug")]
            fn visit_none<E>(self) -> ::std::result::Result<TelemetryValue, E> {
                Ok(TelemetryValue::Unit)
            }

            #[inline]
            #[tracing::instrument(level = "debug", skip(deserializer))]
            fn visit_some<D>(self, deserializer: D) -> ::std::result::Result<TelemetryValue, D::Error>
            where
                D: de::Deserializer<'de>,
            {
                de::Deserialize::deserialize(deserializer)
            }

            #[inline]
            #[tracing::instrument(level = "debug")]
            fn visit_unit<E>(self) -> ::std::result::Result<TelemetryValue, E> {
                Ok(TelemetryValue::Unit)
            }

            #[inline]
            #[tracing::instrument(level = "debug", skip(_deserializer))]
            fn visit_newtype_struct<D>(self, _deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                tracing::error!("deserializing newtype struct into telemetry is not supported");
                Err(D::Error::custom(
                    "deserializing newtype struct into telemetry is not supported",
                ))
            }

            #[inline]
            #[tracing::instrument(level = "debug", skip(visitor))]
            fn visit_seq<V>(self, mut visitor: V) -> ::std::result::Result<TelemetryValue, V::Error>
            where
                V: de::SeqAccess<'de>,
            {
                let mut vec = SeqValue::new();

                while let Some(elem) = visitor.next_element()? {
                    tracing::debug!(value=?elem, "adding deserialized seq item");
                    vec.push(elem);
                }

                Ok(TelemetryValue::Seq(vec))
            }

            #[tracing::instrument(level = "debug", skip(visitor))]
            fn visit_map<V>(self, mut visitor: V) -> ::std::result::Result<TelemetryValue, V::Error>
            where
                V: de::MapAccess<'de>,
            {
                let mut table = TableValue::default();
                while let Some((key, value)) = visitor.next_entry()? {
                    tracing::debug!(?key, ?value, "visiting next entry");
                    match value {
                        TelemetryValue::Unit => (),
                        val => {
                            tracing::debug!(?key, value=?val, "adding deserialized entry.");
                            table.insert(key, val);
                        },
                    }
                }

                Ok(TelemetryValue::Table(table))
            }

            fn visit_enum<A>(self, _data: A) -> Result<Self::Value, A::Error>
            where
                A: EnumAccess<'de>,
            {
                tracing::error!("deserializing enum into telemetry is not supported");
                Err(A::Error::custom("deserializing enum into telemetry is not supported"))
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

#[cfg(test)]
mod tests {
    use claim::*;
    use fmt::Debug;
    use once_cell::sync::Lazy;
    use pretty_assertions::assert_eq;
    use pretty_snowflake::CustomLabeling;
    use serde::{Deserialize, Serialize};
    use serde_test::{assert_tokens, Token};

    use super::*;
    use crate::elements::Telemetry;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Foo {
        bar: TelemetryValue,
    }

    impl Label for Foo {
        type Labeler = CustomLabeling;

        fn labeler() -> Self::Labeler {
            CustomLabeling::new("test_foo")
        }
    }

    #[test]
    fn test_id_value_serde() {
        let id = Id::<Foo>::direct("test_foo", 12345, "abcdef");
        let actual = format!("{:?}", id);
        assert_eq!(actual, format!("test_foo::{}", id));

        let telemetry_id: TelemetryValue = id.clone().into();

        let result = std::panic::catch_unwind(|| {
            assert_tokens(
                &telemetry_id,
                &vec![
                    Token::Map { len: Some(2) },
                    Token::Str("snowflake"),
                    Token::I64(12345),
                    Token::Str("pretty"),
                    Token::Str("abcdef"),
                    Token::MapEnd,
                ],
            )
        });

        if result.is_err() {
            assert_tokens(
                &telemetry_id,
                &vec![
                    Token::Map { len: Some(2) },
                    Token::Str("pretty"),
                    Token::Str("abcdef"),
                    Token::Str("snowflake"),
                    Token::I64(12345),
                    Token::MapEnd,
                ],
            )
        }
    }

    #[derive(Debug, Label, Clone, Serialize, Deserialize)]
    struct Bar {
        correlation_id: Id<Bar>,
    }

    #[test]
    fn test_deser_id_value() {
        let id = Id::<Foo>::direct("test_foo", 12345, "abcdef");
        let telemetry_id: TelemetryValue = id.clone().into();

        let telemetry_json = assert_ok!(serde_json::to_string(&telemetry_id));
        // assert_eq!(telemetry_json, "{\"snowflake\":12345,\"pretty\":\"abcdef\"}");
        let bar1: Id<Bar> = assert_ok!(serde_json::from_str(&telemetry_json));
        let bar1_sf: i64 = bar1.clone().into();
        assert_eq!(bar1_sf, 12345_i64);
        let bar1_p: String = bar1.clone().into();
        assert_eq!(bar1_p.as_str(), "abcdef");

        let telemetry: Telemetry = maplit::hashmap! { "correlation_id".to_string() => telemetry_id }.into();

        let bar: Bar = assert_ok!(telemetry.try_into());
        let actual = format!("{:?}", bar.correlation_id);
        assert_eq!(actual.as_str(), "Bar::abcdef");
    }

    #[test]
    fn test_telemetry_value_integer_serde() {
        Lazy::force(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_telemetry_value_integer_serde");
        let _main_span_guard = main_span.enter();

        let foo = Foo { bar: TelemetryValue::Integer(37) };
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
        assert_tokens(&data, &vec![Token::Bool(true)])
    }

    #[test]
    fn test_telemetry_value_text_serde() {
        let data = TelemetryValue::Text("Foo Bar Zed".to_string());
        assert_tokens(&data, &vec![Token::Str("Foo Bar Zed")])
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
        ]);
        assert_tokens(
            &data,
            &vec![
                Token::Seq { len: Some(6) },
                Token::I64(12),
                Token::F64(std::f64::consts::FRAC_2_SQRT_PI),
                Token::Bool(false),
                Token::Str("2014-11-28T12:45:59.324310806Z"),
                Token::Seq { len: Some(3) },
                Token::I64(37),
                Token::F64(3.14),
                Token::Str("Otis"),
                Token::SeqEnd,
                Token::Map { len: Some(1) },
                Token::Str("foo"),
                Token::Str("bar"),
                Token::MapEnd,
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

        // let result = std::panic::catch_unwind(|| {
        assert_tokens(
            &data,
            &vec![
                Token::Map { len: Some(1) },
                Token::Str("foo"),
                Token::Str("bar"),
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
//                 Err(StageError::TypeError("PolarValue::Instance is not a supported telemetry
// type.".to_string()))             },
//             PolarValue::Variable(_) => {
//                 // maybe Unit?
//                 Err(StageError::TypeError("PolarValue::Variable is not a supported telemetry
// type.".to_string()))             },
//         }
//     }
// }
