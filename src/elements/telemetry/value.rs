use super::{FromTelemetry, ToTelemetry};
use crate::graph::GraphResult;
use config::Value as ConfigValue;
use oso::{FromPolar, PolarValue, ToPolar};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{de, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
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
    #[tracing::instrument(level = "trace", skip())]
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Unit => true,
            Self::Text(rep) => rep.is_empty(),
            Self::Seq(rep) => rep.is_empty(),
            Self::Table(rep) => rep.is_empty(),
            _ => false,
        }
    }

    #[tracing::instrument(level = "trace", skip())]
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
    #[tracing::instrument(level = "trace", skip())]
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
    #[tracing::instrument(level = "trace", skip())]
    fn from_telemetry(val: TelemetryValue) -> GraphResult<Self> {
        Ok(val)
    }
}

impl ToPolar for TelemetryValue {
    #[tracing::instrument(level = "trace", skip())]
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
    #[tracing::instrument(level = "trace", skip())]
    fn from_polar(val: PolarValue) -> oso::Result<Self> {
        Ok(val.to_telemetry())
    }
}

impl Into<ConfigValue> for TelemetryValue {
    #[tracing::instrument(level = "trace", skip())]
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
    #[tracing::instrument(level = "trace", skip(deserializer))]
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
            #[tracing::instrument(level = "trace", skip())]
            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E> {
                tracing::trace!(value=?v, "deserializing bool value");
                Ok(TelemetryValue::Boolean(v))
            }

            #[inline]
            #[tracing::instrument(level = "trace", skip())]
            fn visit_i8<E>(self, v: i8) -> Result<Self::Value, E> {
                tracing::trace!(value=?v, "deserializing i8 value");
                Ok(TelemetryValue::Integer(v as i64))
            }

            #[inline]
            #[tracing::instrument(level = "trace", skip())]
            fn visit_i16<E>(self, v: i16) -> Result<Self::Value, E> {
                tracing::trace!(value=?v, "deserializing i16 value");
                Ok(TelemetryValue::Integer(v as i64))
            }

            #[inline]
            #[tracing::instrument(level = "trace", skip())]
            fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E> {
                tracing::trace!(value=?v, "deserializing i32 value");
                Ok(TelemetryValue::Integer(v as i64))
            }

            #[inline]
            #[tracing::instrument(level = "trace", skip())]
            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
                tracing::trace!(value=?v, "deserializing i64 value");
                Ok(TelemetryValue::Integer(v))
            }

            #[inline]
            #[tracing::instrument(level = "trace", skip())]
            fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E> {
                tracing::trace!(value=?v, "deserializing u8 value");
                Ok(TelemetryValue::Integer(v as i64))
            }

            #[inline]
            #[tracing::instrument(level = "trace", skip())]
            fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E> {
                tracing::trace!(value=?v, "deserializing u16 value");
                Ok(TelemetryValue::Integer(v as i64))
            }

            #[inline]
            #[tracing::instrument(level = "trace", skip())]
            fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E> {
                tracing::trace!(value=?v, "deserializing u32 value");
                Ok(TelemetryValue::Integer(v as i64))
            }

            // #[inline]
            // fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
            //     let smaller: u32 = v as u32;
            //     Ok(smaller.into())
            // }

            #[inline]
            #[tracing::instrument(level = "trace", skip())]
            fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E> {
                tracing::trace!(value=?v, "deserializing f32 value");
                Ok(TelemetryValue::Float(v as f64))
            }
            #[inline]
            #[tracing::instrument(level = "trace", skip())]
            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> {
                tracing::trace!(value=?v, "deserializing f64 value");
                Ok(TelemetryValue::Float(v))
            }

            #[inline]
            #[tracing::instrument(level = "trace", skip())]
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                tracing::trace!(value=?v, "deserializing &str value");
                self.visit_string(String::from(v))
            }

            #[inline]
            #[tracing::instrument(level = "trace", skip())]
            fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
                tracing::trace!(value=?v, "deserializing string value");
                Ok(TelemetryValue::Text(v))
            }

            #[inline]
            #[tracing::instrument(level = "trace", skip())]
            fn visit_none<E>(self) -> ::std::result::Result<TelemetryValue, E> {
                Ok(TelemetryValue::Unit)
            }

            #[inline]
            #[tracing::instrument(level = "trace", skip(deserializer))]
            fn visit_some<D>(self, deserializer: D) -> ::std::result::Result<TelemetryValue, D::Error>
            where
                D: de::Deserializer<'de>,
            {
                de::Deserialize::deserialize(deserializer)
            }

            #[inline]
            #[tracing::instrument(level = "trace", skip())]
            fn visit_unit<E>(self) -> ::std::result::Result<TelemetryValue, E> {
                Ok(TelemetryValue::Unit)
            }

            #[inline]
            #[tracing::instrument(level = "trace", skip(visitor))]
            fn visit_seq<V>(self, mut visitor: V) -> ::std::result::Result<TelemetryValue, V::Error>
            where
                V: de::SeqAccess<'de>,
            {
                let mut vec = Seq::new();

                while let Some(elem) = visitor.next_element()? {
                    tracing::trace!(value=?elem, "adding deserialized seq item");
                    vec.push(elem);
                }

                Ok(TelemetryValue::Seq(vec))
            }

            #[tracing::instrument(level = "trace", skip(visitor))]
            fn visit_map<V>(self, mut visitor: V) -> ::std::result::Result<TelemetryValue, V::Error>
            where
                V: de::MapAccess<'de>,
            {
                let mut table = Table::new();
                while let Some((key, value)) = visitor.next_entry()? {
                    match value {
                        TelemetryValue::Unit => (),
                        val => {
                            tracing::trace!(?key, value=?val, "adding deserialized entry.");
                            table.insert(key, val);
                        }
                    }
                }

                Ok(TelemetryValue::Table(table))
            }

            // #[tracing::instrument(level="trace", skip())]
            // fn visit_enum<A>(self, data: A) -> Result<Self::Value, <A as EnumAccess<'_>>::Error> where
            //     A: EnumAccess<'de>, {
            //     todo!()
            // }
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
    use fmt::Debug;
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

        // let tel = Telemetry::from_iter(maplit::hashmap! {"foo".to_string() => TelemetryValue::Integer(42)});
        // let tel_json = serde_json::to_string(&tel).unwrap();
        // // assert_eq!(tel_json, r#"{"Table":{"foo":{"Integer":42}}}"#);
        // assert_eq!(tel_json, r#"{"Table":{"foo":{"Integer":42}}}"#);
        // tracing::warn!("testing deser from json: {}", tel_json);
        // let actual_tel: Telemetry = serde_json::from_str(tel_json.as_str()).unwrap();
        // assert_eq!(actual_tel, tel);

        let foo = Foo {
            bar: TelemetryValue::Integer(37),
        };
        let json_foo = serde_json::to_string(&foo).unwrap();
        // assert_eq!(json_foo, r#"{"bar":{"Integer":37}}"#);
        assert_eq!(json_foo, r#"{"bar":37}"#);
        tracing::warn!("deserialize: {}", json_foo);
        // let actual_foo: Foo = serde_json::from_str(json_foo.as_str()).unwrap();
        // tracing::warn!(actual=?actual_foo, expected=?foo, "checking result");
        // assert_eq!(actual_foo, foo);

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

    // #[test]
    // fn test_telemetry_value_float_serde() {
    //     let value = TelemetryValue::Float(std::f64::consts::LN_2);
    //
    //     let foo = Foo { bar: value.clone() };
    //     let json_foo = serde_json::to_string(&foo).unwrap();
    //     assert_eq!(json_foo, format!(r#"{{"bar":{{"Float":{}}}}}"#, std::f64::consts::LN_2));
    //     let actual_foo: Foo = serde_json::from_str(json_foo.as_str()).unwrap();
    //     assert_eq!(actual_foo, foo);
    //
    //     let data = Telemetry::from_iter(maplit::hashmap! {"data".to_string() => value.clone()});
    //     let data_json = serde_json::to_string(&data).unwrap();
    //     assert_eq!(data_json, format!(r#"{{"Table":{{"foo":{{"Integer":{}}}}}}}"#, std::f64::consts::LN_2));
    //     let actual_data: Telemetry = serde_json::from_str(data_json.as_str()).unwrap();
    //     assert_eq!(actual_data, data);
    //
    //     assert_tokens(
    //         &data,
    //         &vec![
    //             // Token::NewtypeVariant {
    //             //     name: "TelemetryValue",
    //             //     variant: "Float",
    //             // },
    //             Token::F64(std::f64::consts::LN_2),
    //         ],
    //     )
    // }

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
    // fn test_telemetry_value_nil_serde() {
    //     let data = TelemetryValue::Unit;
    //     assert_tokens(
    //         &data,
    //         &vec![
    //             Token::UnitVariant { name: "TelemetryValue", variant: "Unit",}
    //         ],
    //     )
    // }
    #[test]
    fn test_telemetry_value_list_serde() {
        let data = TelemetryValue::Seq(vec![
            12.to_telemetry(),
            std::f64::consts::FRAC_2_SQRT_PI.to_telemetry(),
            false.to_telemetry(),
            "2014-11-28T12:45:59.324310806Z".to_telemetry(),
            vec![37.to_telemetry(), 3.14.to_telemetry(), "Otis".to_telemetry()].to_telemetry(),
            maplit::btreemap! { "foo".to_string() => "bar".to_telemetry(), }.to_telemetry(),
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
        let data = TelemetryValue::Table(maplit::hashmap! {
            "foo".to_string() => "bar".to_telemetry(),
            // "zed".to_string() => TelemetryValue::Unit,
        });

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
