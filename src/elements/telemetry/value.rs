use super::ToTelemetry;
use config::Value as ConfigValue;
use oso::{FromPolar, PolarValue, ToPolar};
use serde::de::{Error, Unexpected, Visitor};
use serde::{de, Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TelemetryValue {
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Text(String),
    List(Vec<TelemetryValue>),
    Map(HashMap<String, TelemetryValue>),
    Nil,
}

impl TelemetryValue {
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Nil => true,
            Self::Text(rep) => rep.is_empty(),
            Self::List(rep) => rep.is_empty(),
            Self::Map(rep) => rep.is_empty(),
            _ => false,
        }
    }
}

impl Default for TelemetryValue {
    fn default() -> Self {
        Self::Nil
    }
}

impl ToPolar for TelemetryValue {
    fn to_polar(self) -> PolarValue {
        match self {
            Self::Integer(value) => PolarValue::Integer(value),
            Self::Float(value) => PolarValue::Float(value),
            Self::Boolean(value) => PolarValue::Boolean(value),
            Self::Text(rep) => PolarValue::String(rep),
            Self::List(values) => {
                let vs = values.into_iter().map(|v| v.to_polar()).collect();
                PolarValue::List(vs)
            }
            Self::Map(table) => {
                let vs = table.into_iter().map(|(k, v)| (k, v.to_polar())).collect();
                PolarValue::Map(vs)
            }
            Self::Nil => PolarValue::Boolean(false),
        }
    }
}

impl FromPolar for TelemetryValue {
    fn from_polar(val: PolarValue) -> oso::Result<Self> {
        Ok(val.to_telemetry())
    }
}

impl Into<ConfigValue> for TelemetryValue {
    fn into(self) -> ConfigValue {
        match self {
            Self::Integer(value) => ConfigValue::new(None, value),
            Self::Float(value) => ConfigValue::new(None, value),
            Self::Boolean(value) => ConfigValue::new(None, value),
            Self::Text(rep) => ConfigValue::new(None, rep),
            Self::List(values) => {
                let vs: Vec<ConfigValue> = values.into_iter().map(|v| v.into()).collect();
                ConfigValue::new(None, vs)
            }
            Self::Map(table) => {
                let tbl: HashMap<String, ConfigValue> =
                    table.into_iter().map(|(k, v)| (k, v.into())).collect::<HashMap<_, _>>();
                ConfigValue::new(None, tbl)
            }
            Self::Nil => ConfigValue::new(None, Option::<String>::None),
        }
    }
}

// impl<'de> de::Deserialize<'de> for TelemetryValue {
//     #[inline]
//     fn deserialize<D>(deserializer: D) -> ::std::result::Result<TelemetryValue, D::Error>
//     where
//         D: de::Deserializer<'de>,
//     {
//         struct ValueVisitor;
//
//         impl<'de> de::Visitor<'de> for ValueVisitor {
//             type Value = TelemetryValue;
//
//             fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//                 f.write_str("any valid telemetry value")
//             }
//
//             #[inline]
//             fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E> { Ok(v.into()) }
//
//             #[inline]
//             fn visit_i8<E>(self, v: i8) -> Result<Self::Value, E> { Ok((v as i64).into()) }
//
//             #[inline]
//             fn visit_i16<E>(self, v: i16) -> Result<Self::Value, E> { Ok((v as i64).into()) }
//
//             #[inline]
//             fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E> { Ok((v as i64).into()) }
//
//             #[inline]
//             fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> { Ok(v.into()) }
//
//             #[inline]
//             fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E> { Ok((v as i64).into()) }
//
//             #[inline]
//             fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E> { Ok((v as i64).into()) }
//
//             #[inline]
//             fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E> { Ok((v as i64).into()) }
//
//             #[inline]
//             fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> { Ok(v.into()) }
//
//             #[inline]
//             fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> { Ok(v.into()) }
//
//             #[inline]
//             fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
//             where
//                 E: de::Error,
//             {
//                 self.visit_string(String::from(v))
//             }
//
//             #[inline]
//             fn visit_string<E>(self, v: String) -> Result<Self::Value, E> { Ok(v.into()) }
//
//             #[inline]
//             fn visit_seq<V>(self, mut visitor: V) -> ::std::result::Result<TelemetryValue, V::Error>
//             where
//                 V: de::SeqAccess<'de>,
//             {
//                 let mut vec = vec![];
//                 while let Some(elem) = visitor.next_element()? {
//                     vec.push(elem);
//                 }
//
//                 Ok(vec.into())
//             }
//
//             fn visit_map<V>(self, mut visitor: V) -> ::std::result::Result<TelemetryValue, V::Error>
//             where
//                 V: de::MapAccess<'de>,
//             {
//                 let mut table = HashMap::new();
//                 while let Some((key, value)) = visitor.next_entry()? {
//                     table.insert(key, value);
//                 }
//
//                 Ok(table.into())
//             }
//         }
//
//         deserializer.deserialize_any(ValueVisitor)
//     }
// }

// pub fn polar_to_config(polar: PolarValue) -> ConfigValue {
//     match polar {
//         PolarValue::Integer(value) => ConfigValue::new(None, value),
//         PolarValue::Float(value) => ConfigValue::new(None, value),
//         PolarValue::Boolean(value) => ConfigValue::new(None, value),
//         PolarValue::String(value) => ConfigValue::new(None, value),
//         PolarValue::List(values) => {
//             let vs: Vec<ConfigValue> = values.into_iter().map(|v| polar_to_config(v)).collect();
//             ConfigValue::new(None, vs)
//         },
//         PolarValue::Map(table) => {
//             let tbl = table
//                 .into_iter()
//                 .map(|(k,v)| { (k, polar_to_config(v)) })
//                 .collect::<HashMap<_, _>>();
//             ConfigValue::new(None, tbl)
//         },
//         PolarValue::Variable(_) => ConfigValue::new(None, Option::<String>::None),
//         PolarValue::Instance(_) => ConfigValue::new(None, Option::<String>::None),
//     }
// }
//
// pub fn config_to_polar(config: ConfigValue) -> PolarValue {
//     match config {
//         PolarValue::Integer(value) => ConfigValue::new(None, value),
//         PolarValue::Float(value) => ConfigValue::new(None, value),
//         PolarValue::Boolean(value) => ConfigValue::new(None, value),
//         PolarValue::String(value) => ConfigValue::new(None, value),
//         PolarValue::List(values) => {
//             let vs: Vec<ConfigValue> = values.into_iter().map(|v| polar_to_config(v)).collect();
//             ConfigValue::new(None, vs)
//         },
//         PolarValue::Map(table) => {
//             let tbl = table
//                 .into_iter()
//                 .map(|(k,v)| { (k, polar_to_config(v)) })
//                 .collect::<HashMap<_, _>>();
//             ConfigValue::new(None, tbl)
//         },
//         PolarValue::Variable(_) => ConfigValue::new(None, Option::<String>::None),
//         PolarValue::Instance(_) => ConfigValue::new(None, Option::<String>::None),
//     }
// }
