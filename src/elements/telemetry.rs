pub use from_telemetry::FromTelemetry;
pub use to_telemetry::ToTelemetry;
pub use value::TelemetryValue;

mod de;
mod from_telemetry;
mod ser;
mod to_telemetry;
mod value;

use crate::error::GraphError;
use crate::graph::GraphResult;
use oso::ToPolar;
use oso::PolarClass;
use ser::TelemetrySerializer;
use serde::{de as serde_de, Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::iter::{FromIterator, IntoIterator};
use std::fmt::Debug;

#[derive(PolarClass, Debug, Clone, PartialEq)]
pub struct Telemetry(TelemetryValue);

impl Default for Telemetry {
    #[tracing::instrument(level="trace", skip())]
    fn default() -> Self {
        Self(TelemetryValue::Table(HashMap::default()))
    }
}

impl Telemetry {
    #[tracing::instrument(level="trace", skip())]
    pub fn new() -> Self {
        Self::default()
    }

    #[tracing::instrument(level="trace", skip())]
    pub fn from_value<V: Into<TelemetryValue> + Debug>(value: V) -> Self {
        let val = value.into();
        match val {
            inner @ TelemetryValue::Table(_) => Self(inner),
            v => {
                panic!("telemetry root value must be a Table, but was provided a: {:?}", v);
            }
        }
    }

    /// Attempt to deserialize the entire telemetry into the requested type.
    #[tracing::instrument(level="trace", skip())]
    pub fn try_into<T: serde_de::DeserializeOwned>(self) -> GraphResult<T> {
        T::deserialize(self)
        // T::from_telemetry(TelemetryValue::Map(self.0))
        // todo optimize to transform directly
        // let config_shape: config::Value = TelemetryValue::Map(self.0).into();
        // tracing::info!(?config_shape, "after conversion into config");
        // config_shape.try_into().map_err(|err| err.into())
    }

    /// Attempt to serialize the entire telemetry from the given type.
    #[tracing::instrument(level="trace", skip())]
    pub fn try_from<T: Serialize + Debug>(from: &T) -> GraphResult<Self> {
        let mut serializer = TelemetrySerializer::default();
        from.serialize(&mut serializer)?;
        Ok(serializer.output)
    }

    #[inline]
    #[tracing::instrument(level="trace", skip())]
    pub fn extend(&mut self, that: Self) { self.0.extend(&that.0); }

    // pub fn is_empty(&self) -> bool {
    //     if let Value::Map(my_data) = &self.0 {
    //         my_data.is_empty()
    //     } else {
    //         panic!(
    //             "{:?}",
    //             GraphError::GraphPrecondition(format!("telemetry data not a Map but a {:?}", self.0))
    //         );
    //     }
    // }

    // /// Returns a reference to the value corresponding to the key.
    // ///
    // /// # Examples
    // ///
    // /// Basic usage:
    // ///
    // /// ```
    // /// use proctor::elements::TelemetryData;
    // /// use serde_cbor::Value;
    // ///
    // /// let mut telemetry = TelemetryData::new();
    // /// telemetry.insert("a", Value::Integer(1));
    // /// assert_eq!(telemetry.get::<i32>("a").unwrap(), Some(1));
    // /// assert_eq!(telemetry.get::<i32>("b").unwrap(), None);
    // /// ```
    // pub fn get<V: DeserializeOwned>(&self, key: &str) -> GraphResult<Option<V>> {
    //     let cbor_key = serde_cbor::value::to_value(key)?;
    //     let my_data: BTreeMap<Value, Value> = serde_cbor::value::from_value(self.0.clone())?;
    //     match my_data.get(&cbor_key) {
    //         Some(v) => serde_cbor::value::from_value::<V>(v.clone())
    //             .map_err(|err| err.into())
    //             .map(|r| Some(r)),
    //         None => Ok(None),
    //     }
    // }
    //
    // /// Inserts a key-value pair into telemetry.
    // ///
    // /// If telemetry did not have this key present, `None` is returned.
    // ///
    // /// If telemetry did have this key present, the value is updated, and the old
    // /// value is returned. The key is not updated, though; this matters for
    // /// types that can be `==` without being identical.
    // ///
    // /// # Examples
    // ///
    // /// Basic usage:
    // ///
    // /// ```
    // /// use proctor::elements::TelemetryData;
    // /// use serde_cbor::Value;
    // ///
    // /// let mut telemetry = TelemetryData::new();
    // /// assert_eq!(telemetry.insert("a", 37), None);
    // /// assert_eq!(telemetry.is_empty(), false);
    // ///
    // /// telemetry.insert("a", 54);
    // /// assert_eq!(telemetry.insert("a", 75), Some(54));
    // /// ```
    // pub fn insert<V: Serialize + DeserializeOwned>(&mut self, key: &str, value: V) -> Option<V> {
    //     let mut my_data: BTreeMap<Value, Value> = serde_cbor::value::from_value(self.0.clone()).unwrap();
    //     let cbor_key = serde_cbor::value::to_value(key).unwrap();
    //     let cbor_value = serde_cbor::value::to_value(value).unwrap();
    //     let replaced = my_data.insert(cbor_key, cbor_value);
    //     self.0 = Value::Map(my_data);
    //
    //     match replaced {
    //         Some(replaced) => serde_cbor::value::from_value::<V>(replaced).map(|r| Some(r)).unwrap(),
    //         None => None,
    //     }
    // }
    //
    // /// Returns `true` if the telemetry contains a value for the specified key.
    // ///
    // /// The key may be anything that can implements `Into<String>`.
    // ///
    // /// # Examples
    // ///
    // /// Basic usage:
    // ///
    // /// ```
    // /// use proctor::elements::TelemetryData;
    // ///
    // /// let mut telemetry = TelemetryData::new();
    // /// telemetry.insert("foo", 17);
    // /// assert_eq!(telemetry.contains_key("foo"), true);
    // /// assert_eq!(telemetry.contains_key("bar"), false);
    // /// ```
    // pub fn remove<V: DeserializeOwned>(&mut self, key: &str) -> GraphResult<Option<V>> {
    //     let mut my_data: BTreeMap<Value, Value> = serde_cbor::value::from_value(self.0.clone())?;
    //     let cbor_key = serde_cbor::value::to_value(key)?;
    //     let removed_value = my_data.remove(&cbor_key);
    //     self.0 = Value::Map(my_data);
    //     match removed_value {
    //         Some(removed) => serde_cbor::value::from_value::<V>(removed)
    //             .map(|r| Some(r))
    //             .map_err(|err| err.into()),
    //         None => Ok(None),
    //     }
    // }
    //
    // pub fn contains_key(&self, key: &str) -> bool {
    //     if let Ok(cbor_key) = serde_cbor::value::to_value(key) {
    //         match &self.0 {
    //             Value::Map(my_data) => my_data.contains_key(&cbor_key),
    //             _ => false,
    //         }
    //     } else {
    //         false
    //     }
    // }
    //
    // /// Retains only the elements specified by the predicate.
    // ///
    // /// In other words, remove all pairs `(k, v)` such that `f(&k, &mut v)` returns `false`.
    // ///
    // /// # Examples
    // ///
    // /// ```
    // /// use proctor::elements::TelemetryData;
    // /// use serde_cbor::Value;
    // ///
    // /// let mut telemetry: TelemetryData = (0..8).map(|x| (x.to_string(), Value::Integer(x*10))).collect();
    // /// // Keep only the elements with even-numbered keys.
    // /// telemetry.retain(|key, _| {
    // ///     let key = TelemetryData::from_cbor::<String>(key.clone()).unwrap().parse::<i32>().unwrap();
    // ///     key % 2 == 0
    // /// });
    // /// assert!(telemetry.values().into_iter().eq(vec![
    // ///     (0.to_string(), Value::Integer(0)),
    // ///     (2.to_string(), Value::Integer(20)),
    // ///     (4.to_string(), Value::Integer(40)),
    // ///     (6.to_string(), Value::Integer(60))
    // /// ]));
    // /// ```
    // pub fn retain<F>(&mut self, mut f: F)
    // where
    //     F: FnMut(&Value, &Value) -> bool,
    // {
    //     let my_data: BTreeMap<Value, Value> = serde_cbor::value::from_value(self.0.clone()).unwrap();
    //     let retained = my_data.into_iter().filter(|(k, v)| f(k, v)).collect();
    //     self.0 = Value::Map(retained);
    //
    //     // self.drain_filter(|k, v| !f(k, v));
    // }

    #[tracing::instrument(level="trace", skip(oso))]
    pub fn load_knowledge_base(oso: &mut oso::Oso) -> GraphResult<()> {
        oso.register_class(Telemetry::get_polar_class())?;

        oso.register_class(
            oso::ClassBuilder::<TelemetryValue>::with_default()
                .add_method("to_polar", |v: &TelemetryValue| v.clone().to_polar())
                .build(),
        )?;

        Ok(())
    }
}

impl ToTelemetry for Telemetry {
    #[tracing::instrument(level="trace", skip())]
    fn to_telemetry(self) -> TelemetryValue {
        self.0
    }
}

impl FromTelemetry for Telemetry {
    #[tracing::instrument(level="trace", skip())]
    fn from_telemetry(val: TelemetryValue) -> GraphResult<Self> {
        match val {
            inner @ TelemetryValue::Table(_) => Ok(Telemetry(inner)),
            _ => Err(GraphError::TypeError("Table".to_string())),
        }
    }
}

impl std::ops::Deref for Telemetry {
    type Target = HashMap<String, TelemetryValue>;

    #[tracing::instrument(level="trace", skip())]
    fn deref(&self) -> &Self::Target {
        if let TelemetryValue::Table(ref inner) = self.0 {
            &*inner
        } else {
            panic!("root telemetry value must be a Table, but is {:?}", self.0);
        }
    }
}

impl std::ops::DerefMut for Telemetry {
    #[tracing::instrument(level="trace", skip())]
    fn deref_mut(&mut self) -> &mut Self::Target {
        if let TelemetryValue::Table(ref mut inner) = self.0 {
            &mut *inner
        } else {
            panic!("root telemetry value must be a Table, but is {:?}.", self.0);
        }
    }
}

impl Into<Telemetry> for HashMap<String, TelemetryValue> {
    #[tracing::instrument(level="trace", skip())]
    fn into(self) -> Telemetry {
        Telemetry(TelemetryValue::Table(self))
    }
}

impl Into<Telemetry> for BTreeMap<String, TelemetryValue> {
    #[tracing::instrument(level="trace", skip())]
    fn into(self) -> Telemetry {
        Telemetry::from_iter(self)
    }
}

impl std::ops::Add for Telemetry {
    type Output = Self;
    #[tracing::instrument(level="trace", skip())]
    fn add(mut self, rhs: Self) -> Self::Output {
        self.0.extend(&rhs.0);
        self
    }
}

impl FromIterator<(String, TelemetryValue)> for Telemetry {
    #[tracing::instrument(level="trace", skip(iter))]
    fn from_iter<T: IntoIterator<Item = (String, TelemetryValue)>>(iter: T) -> Self {
        Telemetry(TelemetryValue::Table(HashMap::from_iter(iter)))
    }
}

impl<'a> IntoIterator for &'a Telemetry {
    type Item = (&'a String, &'a TelemetryValue);
    type IntoIter = std::collections::hash_map::Iter<'a, String, TelemetryValue>;

    #[inline]
    #[tracing::instrument(level="trace", skip())]
    fn into_iter(self) -> Self::IntoIter {
        if let TelemetryValue::Table(ref table) = self.0 {
            table.iter()
        } else {
            panic!("telemetry root value must be a Table, but was {:?}", self.0);
        }
    }
}

impl<'a> IntoIterator for &'a mut Telemetry {
    type Item = (&'a String, &'a mut TelemetryValue);
    type IntoIter = std::collections::hash_map::IterMut<'a, String, TelemetryValue>;

    #[inline]
    #[tracing::instrument(level="trace", skip())]
    fn into_iter(self) -> Self::IntoIter {
        if let TelemetryValue::Table(ref mut table) = self.0 {
            table.iter_mut()
        } else {
            panic!("telemetry root value must be a Table, but was {:?}", self.0);
        }
    }
}

impl IntoIterator for Telemetry {
    type Item = (String, TelemetryValue);
    type IntoIter = std::collections::hash_map::IntoIter<String, TelemetryValue>;

    #[inline]
    #[tracing::instrument(level="trace", skip())]
    fn into_iter(self) -> Self::IntoIter {
        if let TelemetryValue::Table(table) = self.0 {
            table.into_iter()
        } else {
            panic!("telemetry root value must be a Table, but was {:?}", self.0);
        }
    }
}

// /////////////////////////////////////////////////////
// // Unit Tests ///////////////////////////////////////
//
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use oso::{FromPolar, PolarClass, ToPolar};
    use serde::{Deserialize, Serialize};
    use serde_test::{assert_tokens, Token};
    use std::iter::FromIterator;
    use itertools::Itertools;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Data {
        #[serde(default)]
        #[serde(
            rename = "task.last_failure",
            serialize_with = "crate::serde::serialize_optional_datetime",
            deserialize_with = "crate::serde::deserialize_optional_datetime"
        )]
        pub last_failure: Option<DateTime<Utc>>,
        #[serde(rename = "cluster.is_deploying")]
        pub is_deploying: bool,
        #[serde(rename = "cluster.last_deployment", with = "crate::serde")]
        pub last_deployment: DateTime<Utc>,
    }

    #[test]
    fn test_telemetry_map_struct() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct TestData {
            place: HashMap<String, i32>,
        }

        let data = TestData {
            place: maplit::hashmap! {
                    "foo".to_string() => 37,
                    "bar".to_string() => 19,
                }
        };

        let telemetry = Telemetry::from_iter(
            maplit::hashmap! {
                "place".to_string() => TelemetryValue::Table(maplit::hashmap! {
                    "foo".to_string() => 37.to_telemetry(),
                    "bar".to_string() => 19.to_telemetry(),
                })
            });

        let actual_data: TestData = telemetry.clone().try_into().unwrap();
        assert_eq!( actual_data, data);

        let actual_telemetry = Telemetry::try_from(&actual_data).unwrap();
        assert_eq!(actual_telemetry, telemetry);
    }

    #[test]
    fn test_telemetry_struct_array() {
        #[derive(Debug, Serialize, Deserialize)]
        struct TestData {
            #[serde(rename = "arr")]
            elements: Vec<String>,
        }

        impl PartialEq for TestData {
            fn eq(&self, other: &Self) -> bool {
                let mut result = self.elements.len() == other.elements.len();
                if result {
                    let result = self.elements
                        .iter()
                        .zip_eq(&other.elements)
                        .all(|(lhs, rhs)| lhs == rhs);
                }
                result
            }
        }

        let data  = TestData {
            elements: vec!["Otis".to_string(), "Stella".to_string(), "Neo".to_string()],
        };

        let telemetry = Telemetry::from_iter(
            maplit::hashmap! {
                "arr".to_string() => TelemetryValue::Seq(vec![
                    TelemetryValue::Text("Otis".to_string()),
                    TelemetryValue::Text("Stells".to_string()),
                    TelemetryValue::Text("Neo".to_string()),
                ])
            }
        );

        let actual_data: TestData = telemetry.clone().try_into().unwrap();
        assert_eq!(actual_data, data);

        let actual_telemetry = Telemetry::try_from(&actual_data).unwrap();
        assert_eq!(actual_telemetry, telemetry);
    }

    #[test]
    fn test_telemetry_simple_enum() {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_telemetry_enum");
        let _main_span_guard = main_span.enter();

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        #[serde(rename_all = "lowercase")]
        enum Diode {
            Off,
            Brightness(i32),
            Blinking(i32, i32),
            Pattern { name: String, infinite: bool },
        }
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct TestData {
            diodes: HashMap<String, Diode>,
        }

        let data = TestData {
            diodes: maplit::hashmap! {
                // "green".to_string() => Diode::Off,
                // "lt-green".to_string() => Diode::Off,
                "red".to_string() => Diode::Brightness(100),
                // "blue".to_string() => Diode::Blinking(300, 700),
                // "white".to_string() => Diode::Pattern { name: "christmas".to_string(), infinite: true,},
            }
        };

        let telemetry = Telemetry::from_iter(
            maplit::hashmap! {
                "diodes".to_string() => TelemetryValue::Table(maplit::hashmap! {
                    // "green".to_string() => TelemetryValue::Table(maplit::hashmap! { "off".to_string() => TelemetryValue::Unit}),
                    // "lt-green".to_string() => TelemetryValue::Table(maplit::hashmap! { "off".to_string() => TelemetryValue::Unit}),
                    "red".to_string() => TelemetryValue::Table(maplit::hashmap!{"brightness".to_string() => TelemetryValue::Integer(100)}),
                    // "blue".to_string() => TelemetryValue::Table(maplit::hashmap!{"blinking".to_string() => TelemetryValue::Seq(vec![TelemetryValue::Integer(300), TelemetryValue::Integer(700)])}),
                    // "white".to_string() => TelemetryValue::Table(maplit::hashmap!{"pattern".to_string() => TelemetryValue::Table(maplit::hashmap! {
                    //     "name".to_string() => TelemetryValue::Text("christmas".to_string()),
                    //     "infinite".to_string() => TelemetryValue::Boolean(true),
                    // })})
                }),
            }
        );

        tracing::warn!("deserializing actual data from telemetry...");
        let actual_data: TestData = telemetry.clone().try_into().unwrap();
        tracing::warn!(?actual_data, "deserialized actual data");
        assert_eq!(actual_data, data);

        let actual_telemetry = Telemetry::try_from(&actual_data).unwrap();
        assert_eq!(actual_telemetry, telemetry);
    }

    #[test]
    fn test_telemetry_enum() {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_telemetry_enum");
        let _main_span_guard = main_span.enter();

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        #[serde(rename_all = "lowercase")]
        enum Diode {
            Off,
            Brightness(i32),
            Blinking(i32, i32),
            Pattern { name: String, infinite: bool },
        }
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct TestData {
            diodes: HashMap<String, Diode>,
        }

        let data = TestData {
            diodes: maplit::hashmap! {
                "green".to_string() => Diode::Off,
                "red".to_string() => Diode::Brightness(100),
                "blue".to_string() => Diode::Blinking(300, 700),
                "white".to_string() => Diode::Pattern { name: "christmas".to_string(), infinite: true,},
            }
        };

        let telemetry = Telemetry::from_iter(
            maplit::hashmap! {
                "diodes".to_string() => TelemetryValue::Table(maplit::hashmap! {
                    "green".to_string() => TelemetryValue::Table(maplit::hashmap! { "off".to_string() => TelemetryValue::Unit}),
                    "red".to_string() => TelemetryValue::Table(maplit::hashmap!{"brightness".to_string() => TelemetryValue::Integer(100)}),
                    "blue".to_string() => TelemetryValue::Table(maplit::hashmap!{"blinking".to_string() => TelemetryValue::Seq(vec![TelemetryValue::Integer(300), TelemetryValue::Integer(700)])}),
                    "white".to_string() => TelemetryValue::Table(maplit::hashmap!{"pattern".to_string() => TelemetryValue::Table(maplit::hashmap! {
                        "name".to_string() => TelemetryValue::Text("christmas".to_string()),
                        "infinite".to_string() => TelemetryValue::Boolean(true),
                    })})
                }),
            }
        );

        tracing::warn!("deserializing actual data from telemetry...");
        let actual_data: TestData = telemetry.clone().try_into().unwrap();
        tracing::warn!(?actual_data, "deserialized actual data");
        assert_eq!(actual_data, data);

        let actual_telemetry = Telemetry::try_from(&actual_data).unwrap();
        assert_eq!(actual_telemetry, telemetry);

        // assert_eq!(s.diodes["green"], Diode::Off);
        // assert_eq!(s.diodes["red"], Diode::Brightness(100));
        // assert_eq!(s.diodes["blue"], Diode::Blinking(300, 700));
        // assert_eq!(
        //     s.diodes["white"],
        //     Diode::Pattern {
        //         name: "christmas".into(),
        //         infinite: true,
        //     }
        // );
    }

    // impl FromTelemetry for Data {
    //     fn from_telemetry(val: TelemetryValue) -> GraphResult<Self> {
    //         if let TelemetryValue::Map(table) = val {
    //             let last_failure = table.get(&"task.last_failure".to_string()).map(|t| );
    //             let is_deploying
    //         } else {
    //             Err(crate::error::GraphError::TypeError("TelemetryValue::Map".to_string()))
    //         }
    //     }
    // }

    // #[test]
    // fn test_telemetry_serde_tokens() {
    //     let data = Telemetry::from_iter(maplit::hashmap! {
    //         "task.last_failure".to_string() => "2014-11-28T12:45:59.324310806Z".to_telemetry(),
    //         // "cluster.is_deploying".to_string() => false.to_telemetry(),
    //         // "cluster.last_deployment".to_string() => "2014-11-28T10:11:37.246310806Z".to_telemetry(),
    //     });
    //
    //     assert_tokens(
    //         &data,
    //         &vec![
    //             Token::NewtypeStruct { name: "Telemetry" },
    //             Token::Map { len: Some(1) },
    //             Token::Str("task.last_failure"),
    //             Token::NewtypeVariant {
    //                 name: "TelemetryValue",
    //                 variant: "Text",
    //             },
    //             Token::Str("2014-11-28T12:45:59.324310806Z"),
    //             // Token::Str("cluster.is_deploying"),
    //             // Token::NewtypeVariant { name:"TelemetryValue", variant:"Boolean"},
    //             // Token::Bool(false),
    //             // Token::Str("cluster.last_deployment"),
    //             // Token::NewtypeVariant { name:"TelemetryValue", variant:"Text"},
    //             // Token::Str("2014-11-28T10:11:37.246310806Z",),
    //             Token::MapEnd,
    //         ],
    //     );
    // }

    #[test]
    fn test_telemetry_try_into_tokens() {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let data = Telemetry::from_iter(maplit::hashmap! {
            "task.last_failure".to_string() => "2014-11-28T12:45:59.324310806Z".to_telemetry(),
            // "cluster.is_deploying".to_string() => false.to_telemetry(),
            // "cluster.last_deployment".to_string() => "2014-11-28T10:11:37.246310806Z".to_telemetry(),
        });

        let config_shape: config::Value = data.0.into();
        // assert_tokens(
        //     &config_shape,
        //    &vec![
        //        Token::NewtypeStruct { name:"Telemetry" },
        //        Token::Map{ len: Some(1), },
        //        Token::Str("task.last_failure"),
        //        Token::NewtypeVariant { name:"TelemetryValue", variant:"Text"},
        //        Token::Str("2014-11-28T12:45:59.324310806Z"),
        //        // Token::Str("cluster.is_deploying"),
        //        // Token::NewtypeVariant { name:"TelemetryValue", variant:"Boolean"},
        //        // Token::Bool(false),
        //        // Token::Str("cluster.last_deployment"),
        //        // Token::NewtypeVariant { name:"TelemetryValue", variant:"Text"},
        //        // Token::Str("2014-11-28T10:11:37.246310806Z",),
        //        Token::MapEnd,
        //    ]
        // )
    }

    // #[test]
    // fn test_telemetry_identity_try_into() -> anyhow::Result<()> {
    //     lazy_static::initialize(&crate::tracing::TEST_TRACING);
    //     let data = Telemetry::from_iter(maplit::hashmap! {
    //         "task.last_failure".to_string() => "2014-11-28T12:45:59.324310806Z".to_telemetry(),
    //         // "cluster.is_deploying".to_string() => false.to_telemetry(),
    //         // "cluster.last_deployment".to_string() => "2014-11-28T10:11:37.246310806Z".to_telemetry(),
    //     });
    //
    //     // let foo: Option<bool> = data
    //     //     .get("cluster.is_deploying")
    //     //     .map(|f| bool::from_telemetry(f.clone()).unwrap());
    //     // assert_eq!(foo, Some(false));
    //
    //     let expected = Telemetry(
    //         maplit::hashmap! {
    //             "task.last_failure".to_string() => TelemetryValue::Text("2014-11-28T12:45:59.324310806Z".to_string()),
    //             // "cluster.is_deploying".to_string() => TelemetryValue::Boolean(false),
    //             // "cluster.last_deployment".to_string() => TelemetryValue::Text("2014-11-28T10:11:37.246310806Z".to_string()),
    //         }
    //         .to_telemetry(),
    //     );
    //     tracing::info!(telemetry=?data, ?expected, "expected conversion from telemetry data.");
    //     let actual = data.try_into::<Telemetry>();
    //     tracing::info!(?actual, "actual conversion from telemetry data.");
    //     assert_eq!(actual?, expected);
    //     Ok(())
    // }

    #[test]
    fn test_telemetry_data_try_into_deserializer() -> anyhow::Result<()> {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_telemetry_data_try_into_deserializer");
        let _main_span_guard = main_span.enter();

        let data = Telemetry::from_iter(maplit::btreemap! {
            "task.last_failure".to_string() => "2014-11-28T12:45:59.324310806Z".to_telemetry(),
            "cluster.is_deploying".to_string() => false.to_telemetry(),
            "cluster.last_deployment".to_string() => "2014-11-28T10:11:37.246310806Z".to_telemetry(),
        });

        let foo: Option<bool> = data
            .get("cluster.is_deploying")
            .map(|f| bool::from_telemetry(f.clone()).unwrap());
        assert_eq!(foo, Some(false));

        let expected = Data {
            last_failure: Some(DateTime::parse_from_str("2014-11-28T12:45:59.324310806Z", "%+")?.with_timezone(&Utc)),
            is_deploying: false,
            last_deployment: DateTime::parse_from_str("2014-11-28T10:11:37.246310806Z", "%+")?.with_timezone(&Utc),
        };
        tracing::info!("expected: {:?} from data:{:?}", expected, data);
        let actual = data.try_into::<Data>();
        tracing::info!("actual: {:?}", actual);
        assert_eq!(actual?, expected);
        Ok(())
    }

    #[test]
    fn test_telemetry_retain() {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        tracing::info!("start test_telemetry_retain...");
        let mut telemetry: Telemetry = (0..8)
            .map(|x| (x.to_string(), TelemetryValue::Integer(x * 10)))
            .collect();
        tracing::info!(?telemetry, "test_telemetry_retain telemetry");
        // Keep only the elements with even-numbered keys.
        telemetry.retain(|k, v| {
            tracing::info!(key=%k, telemetry_value=?v, "assessing to retain...");
            let key = k.parse::<i32>().unwrap();
            tracing::info!("assessing to retain: {} % 2 == 0 => {}", key, key % 2 == 0);
            key % 2 == 0
        });
        tracing::info!(?telemetry, "after retain eval.");

        assert_eq!(
            telemetry,
            Telemetry::from_iter(vec![
                (0.to_string(), TelemetryValue::Integer(0)),
                (2.to_string(), TelemetryValue::Integer(20)),
                (4.to_string(), TelemetryValue::Integer(40)),
                (6.to_string(), TelemetryValue::Integer(60)),
            ])
        );
    }

    // #[test]
    // fn test_zero_capacities() {
    //     type HM = TelemetryData;
    //
    //     let m = HM::new();
    //     assert_eq!(m.capacity(), 0);
    //
    //     let m = HM::default();
    //     assert_eq!(m.capacity(), 0);
    //
    //     let m = HM::with_capacity(0);
    //     assert_eq!(m.capacity(), 0);
    //
    //     let mut m = HM::new();
    //     m.insert("1", 1);
    //     m.insert("2", 2);
    //     m.remove("1");
    //     m.remove("2");
    //     m.shrink_to_fit();
    //     assert_eq!(m.capacity(), 0);
    //
    //     let mut m = HM::new();
    //     m.reserve(0);
    //     assert_eq!(m.capacity(), 0);
    // }

    // #[test]
    // fn test_create_capacity_zero() {
    //     let mut m = HashMap::with_capacity(0);
    //
    //     assert!(m.insert("1".to_string(), "1".to_string()).is_none());
    //
    //     assert!(m.contains_key(&"1".to_string()));
    //     assert!(!m.contains_key(&"0".to_string()));
    // }

    // #[test]
    // fn test_insert() {
    //     let mut m = HashMap::new();
    //     assert_eq!(m.len(), 0);
    //     assert!(m.insert("1".to_string(), "2".to_string()).is_none());
    //     assert_eq!(m.len(), 1);
    //     assert!(m.insert("2".to_string(), "4".to_string()).is_none());
    //     assert_eq!(m.len(), 2);
    //     assert_eq!(*m.get(&"1".to_string()).unwrap(), "2".to_string());
    //     assert_eq!(*m.get(&"2".to_string()).unwrap(), "4".to_string());
    // }

    // #[test]
    // fn test_clone() {
    //     let mut m = HashMap::new();
    //     assert_eq!(m.len(), 0);
    //     assert!(m.insert("1".to_string(), "2".to_string()).is_none());
    //     assert_eq!(m.len(), 1);
    //     assert!(m.insert("2".to_string(), "4".to_string()).is_none());
    //     assert_eq!(m.len(), 2);
    //     let m2 = m.clone();
    //     assert_eq!(*m2.get(&"1".to_string()).unwrap(), "2".to_string());
    //     assert_eq!(*m2.get(&"2".to_string()).unwrap(), "4".to_string());
    //     assert_eq!(m2.len(), 2);
    // }

    // #[test]
    // fn test_empty_iter() {
    //     let mut m: TelemetryData = TelemetryData::new();
    //     assert_eq!(m.drain().next(), None);
    //     assert_eq!(m.keys().next(), None);
    //     assert_eq!(m.values().next(), None);
    //     assert_eq!(m.values_mut().next(), None);
    //     assert_eq!(m.iter().next(), None);
    //     assert_eq!(m.iter_mut().next(), None);
    //     assert_eq!(m.len(), 0);
    //     assert!(m.is_empty());
    //     assert_eq!(m.into_iter().next(), None);
    // }

    // #[test]
    // fn test_iterate() {
    //     let mut m = TelemetryData::with_capacity(4);
    //     for i in 0..32 {
    //         assert!(m.insert(i.to_string(), (i * 2).to_string()).is_none());
    //     }
    //     assert_eq!(m.len(), 32);
    //
    //     let mut observed: u32 = 0;
    //
    //     for (k, v) in &m {
    //         let k_val = i32::from_str((*k).as_str()).unwrap();
    //         assert_eq!(*v, (k_val * 2).to_string());
    //         observed |= 1 << k_val;
    //     }
    //     assert_eq!(observed, 0xFFFF_FFFF);
    // }

    // #[test]
    // fn test_keys() {
    //     let vec = vec![
    //         ("1".to_string(), "a".to_string()),
    //         ("2".to_string(), "b".to_string()),
    //         ("3".to_string(), "c".to_string()),
    //     ];
    //     let map = TelemetryData(vec.into_iter().collect());
    //     let keys: Vec<_> = map.keys().cloned().collect();
    //     assert_eq!(keys.len(), 3);
    //     assert!(keys.contains(&"1".to_string()));
    //     assert!(keys.contains(&"2".to_string()));
    //     assert!(keys.contains(&"3".to_string()));
    // }

    // #[test]
    // fn test_values() {
    //     let vec = vec![
    //         ("1".to_string(), "a".to_string()),
    //         ("2".to_string(), "b".to_string()),
    //         ("3".to_string(), "c".to_string()),
    //     ];
    //     let map = TelemetryData(vec.into_iter().collect());
    //     let values: Vec<_> = map.values().cloned().collect();
    //     assert_eq!(values.len(), 3);
    //     assert!(values.contains(&"a".to_string()));
    //     assert!(values.contains(&"b".to_string()));
    //     assert!(values.contains(&"c".to_string()));
    // }

    // #[test]
    // fn test_values_mut() {
    //     let vec = vec![
    //         ("1".to_string(), "1".to_string()),
    //         ("2".to_string(), "2".to_string()),
    //         ("3".to_string(), "3".to_string()),
    //     ];
    //     let mut map = TelemetryData(vec.into_iter().collect());
    //     for value in map.values_mut() {
    //         let val = i32::from_str((*value).as_str()).unwrap();
    //         *value = (val * 2).to_string();
    //     }
    //     let values: Vec<_> = map.values().cloned().collect();
    //     assert_eq!(values.len(), 3);
    //     assert!(values.contains(&"2".to_string()));
    //     assert!(values.contains(&"4".to_string()));
    //     assert!(values.contains(&"6".to_string()));
    // }
}
