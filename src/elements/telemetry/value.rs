use super::{FromTelemetry, ToTelemetry};
use crate::graph::GraphResult;
use config::Value as ConfigValue;
use oso::{FromPolar, PolarValue, ToPolar};
use serde::{de, Serialize};
use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Serialize)]
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
    #[tracing::instrument(level="trace", skip())]
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Unit => true,
            Self::Text(rep) => rep.is_empty(),
            Self::Seq(rep) => rep.is_empty(),
            Self::Table(rep) => rep.is_empty(),
            _ => false,
        }
    }

    #[tracing::instrument(level="trace", skip())]
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
    #[tracing::instrument(level="trace", skip())]
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

// impl TryFrom<bool> for TelemetryValue {
//     type Error = GraphError;
//     fn try_from(value: bool) -> Result<Self, Self::Error> {
//         Ok(TelemetryValue::Boolean(value))
//     }
// }

// impl<T: ToTelemetry> From<T> for TelemetryValue {
//     fn from(that: T) -> Self {
//         that.to_telemetry()
//     }
// }

impl FromTelemetry for TelemetryValue {
    #[tracing::instrument(level="trace", skip())]
    fn from_telemetry(val: TelemetryValue) -> GraphResult<Self> {
        Ok(val)
    }
}

impl ToPolar for TelemetryValue {
    #[tracing::instrument(level="trace", skip())]
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
    #[tracing::instrument(level="trace", skip())]
    fn from_polar(val: PolarValue) -> oso::Result<Self> {
        Ok(val.to_telemetry())
    }
}

impl Into<ConfigValue> for TelemetryValue {
    #[tracing::instrument(level="trace", skip())]
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

impl<'de> de::Deserialize<'de> for TelemetryValue {
    #[inline]
    #[tracing::instrument(level="trace", skip(deserializer))]
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
            #[tracing::instrument(level="trace", skip())]
            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E> {
                Ok(v.to_telemetry())
            }

            #[inline]
            #[tracing::instrument(level="trace", skip())]
            fn visit_i8<E>(self, v: i8) -> Result<Self::Value, E> {
                Ok((v as i64).to_telemetry())
            }

            #[inline]
            #[tracing::instrument(level="trace", skip())]
            fn visit_i16<E>(self, v: i16) -> Result<Self::Value, E> {
                Ok((v as i64).to_telemetry())
            }

            #[inline]
            #[tracing::instrument(level="trace", skip())]
            fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E> {
                Ok((v as i64).to_telemetry())
            }

            #[inline]
            #[tracing::instrument(level="trace", skip())]
            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
                Ok(v.to_telemetry())
            }

            #[inline]
            #[tracing::instrument(level="trace", skip())]
            fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E> {
                Ok((v as i64).to_telemetry())
            }

            #[inline]
            #[tracing::instrument(level="trace", skip())]
            fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E> {
                Ok((v as i64).to_telemetry())
            }

            #[inline]
            #[tracing::instrument(level="trace", skip())]
            fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E> {
                Ok((v as i64).to_telemetry())
            }

            // #[inline]
            // fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
            //     let smaller: u32 = v as u32;
            //     Ok(smaller.into())
            // }

            #[inline]
            #[tracing::instrument(level="trace", skip())]
            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> {
                Ok(v.to_telemetry())
            }

            #[inline]
            #[tracing::instrument(level="trace", skip())]
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_string(String::from(v))
            }

            #[inline]
            #[tracing::instrument(level="trace", skip())]
            fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
                Ok(v.to_telemetry())
            }

            #[inline]
            #[tracing::instrument(level="trace", skip(visitor))]
            fn visit_seq<V>(self, mut visitor: V) -> ::std::result::Result<TelemetryValue, V::Error>
            where
                V: de::SeqAccess<'de>,
            {
                let mut vec: Seq = vec![];
                while let Some(elem) = visitor.next_element()? {
                    vec.push(elem);
                }

                Ok(vec.to_telemetry())
            }

            #[tracing::instrument(level="trace", skip(visitor))]
            fn visit_map<V>(self, mut visitor: V) -> ::std::result::Result<TelemetryValue, V::Error>
            where
                V: de::MapAccess<'de>,
            {
                let mut table: Table = HashMap::new();
                while let Some((key, value)) = visitor.next_entry()? {
                    table.insert(key, value);
                }

                Ok(table.to_telemetry())
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use oso::{FromPolar, PolarClass, ToPolar};
    use serde::{Deserialize, Serialize};
    use serde_test::{assert_tokens, Token};
    use std::iter::FromIterator;

    #[test]
    fn test_telemetry_value_integer_serde() {
        let data = TelemetryValue::Integer(33);
        assert_tokens(
            &data,
            &vec![
                Token::NewtypeVariant {
                    name: "TelemetryValue",
                    variant: "Integer",
                },
                Token::I64(33),
            ],
        )
    }

    #[test]
    fn test_telemetry_value_float_serde() {
        let data = TelemetryValue::Float(std::f64::consts::LN_2);
        assert_tokens(
            &data,
            &vec![
                Token::NewtypeVariant {
                    name: "TelemetryValue",
                    variant: "Float",
                },
                Token::F64(std::f64::consts::LN_2),
            ],
        )
    }

    #[test]
    fn test_telemetry_value_boolean_serde() {
        let data = TelemetryValue::Boolean(true);
        assert_tokens(
            &data,
            &vec![
                Token::NewtypeVariant {
                    name: "TelemetryValue",
                    variant: "Boolean",
                },
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
                Token::NewtypeVariant {
                    name: "TelemetryValue",
                    variant: "Text",
                },
                Token::Str("Foo Bar Zed"),
            ],
        )
    }

    #[test]
    fn test_telemetry_value_nil_serde() {
        let data = TelemetryValue::Unit;
        assert_tokens(
            &data,
            &vec![Token::UnitVariant {
                name: "TelemetryValue",
                variant: "Nil",
            }],
        )
    }

    #[test]
    fn test_telemetry_value_list_serde() {
        let data = TelemetryValue::Seq(vec![
            12.to_telemetry(),
            std::f64::consts::FRAC_2_SQRT_PI.to_telemetry(),
            false.to_telemetry(),
            "2014-11-28T12:45:59.324310806Z".to_telemetry(),
            vec![37.to_telemetry(), 3.14.to_telemetry(), "Otis".to_telemetry()].to_telemetry(),
            maplit::btreemap! { "foo".to_string() => "bar".to_telemetry(), }.to_telemetry(),
            TelemetryValue::Unit,
        ]);
        assert_tokens(
            &data,
            &vec![
                Token::NewtypeVariant {
                    name: "TelemetryValue",
                    variant: "List",
                },
                Token::Seq { len: Some(7) },
                Token::NewtypeVariant {
                    name: "TelemetryValue",
                    variant: "Integer",
                },
                Token::I64(12),
                Token::NewtypeVariant {
                    name: "TelemetryValue",
                    variant: "Float",
                },
                Token::F64(std::f64::consts::FRAC_2_SQRT_PI),
                Token::NewtypeVariant {
                    name: "TelemetryValue",
                    variant: "Boolean",
                },
                Token::Bool(false),
                Token::NewtypeVariant {
                    name: "TelemetryValue",
                    variant: "Text",
                },
                Token::Str("2014-11-28T12:45:59.324310806Z"),
                Token::NewtypeVariant {
                    name: "TelemetryValue",
                    variant: "List",
                },
                Token::Seq { len: Some(3) },
                Token::NewtypeVariant {
                    name: "TelemetryValue",
                    variant: "Integer",
                },
                Token::I64(37),
                Token::NewtypeVariant {
                    name: "TelemetryValue",
                    variant: "Float",
                },
                Token::F64(3.14),
                Token::NewtypeVariant {
                    name: "TelemetryValue",
                    variant: "Text",
                },
                Token::Str("Otis"),
                Token::SeqEnd,
                Token::NewtypeVariant {
                    name: "TelemetryValue",
                    variant: "Map",
                },
                Token::Map { len: Some(1) },
                Token::Str("foo"),
                Token::NewtypeVariant {
                    name: "TelemetryValue",
                    variant: "Text",
                },
                Token::Str("bar"),
                Token::MapEnd,
                Token::UnitVariant {
                    name: "TelemetryValue",
                    variant: "Nil",
                },
                Token::SeqEnd,
            ],
        )
    }

    #[test]
    fn test_telemetry_value_map_serde() {
        let data = TelemetryValue::Table(maplit::hashmap! {
            "foo".to_string() => "bar".to_telemetry(),
            "zed".to_string() => TelemetryValue::Unit,
        });

        let mut expected = vec![
            Token::NewtypeVariant {
                name: "TelemetryValue",
                variant: "Map",
            },
            Token::Map { len: Some(2) },
            Token::Str("foo"),
            Token::NewtypeVariant {
                name: "TelemetryValue",
                variant: "Text",
            },
            Token::Str("bar"),
            Token::Str("zed"),
            Token::UnitVariant {
                name: "TelemetryValue",
                variant: "Nil",
            },
            Token::MapEnd,
        ];

        let result = std::panic::catch_unwind(|| {
            assert_tokens(&data, &expected);
        });

        if result.is_err() {
            expected.swap(2, 5);
            expected.swap(3, 6);
            expected.swap(4, 5);
            expected.swap(5, 6);
            assert_tokens(&data, &expected);
        }
    }
}
