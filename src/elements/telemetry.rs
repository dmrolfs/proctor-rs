pub use from_telemetry::FromTelemetry;
pub use to_telemetry::ToTelemetry;
pub use value::{Seq, Table, TelemetryValue};

mod de;
mod from_telemetry;
mod ser;
mod to_telemetry;
mod value;

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::iter::{FromIterator, IntoIterator};

use flexbuffers;
use oso::PolarClass;
use oso::ToPolar;
use serde::{de as serde_de, Deserialize, Serialize};

use crate::error::{PolicyError, TelemetryError};

#[derive(PolarClass, Debug, Clone, PartialEq)]
pub struct Telemetry(TelemetryValue);

impl Default for Telemetry {
    #[tracing::instrument(level = "trace", skip())]
    fn default() -> Self {
        Self(TelemetryValue::Table(HashMap::default()))
    }
}

impl Telemetry {
    #[tracing::instrument(level = "trace", skip())]
    pub fn new() -> Self {
        Self::default()
    }

    /// Attempt to deserialize the entire telemetry into the requested type.
    #[tracing::instrument(level = "debug", name = "try into telemetry", skip())]
    pub fn try_into<T: serde_de::DeserializeOwned>(self) -> Result<T, TelemetryError> {
        let mut serializer = flexbuffers::FlexbufferSerializer::new();
        self.0.serialize(&mut serializer)?;
        let reader = flexbuffers::Reader::get_root(serializer.view())?;
        let result = T::deserialize(reader)?;
        Ok(result)
    }

    // todo: DMR - I can't get this to work wrt Enum Unit Variants - see commented out try_form portion
    // of test_telemetry_simple_enum()
    /// Attempt to serialize the entire telemetry from the given type.
    #[tracing::instrument(level = "debug", name = "try from telemetry", skip())]
    pub fn try_from<T: Serialize + Debug>(from: &T) -> Result<Self, TelemetryError> {
        // todo: mimic technique in config-rs more closely
        let mut serializer = flexbuffers::FlexbufferSerializer::new();
        from.serialize(&mut serializer)?;
        let data = serializer.take_buffer();
        let reader = flexbuffers::Reader::get_root(&*data)?;
        let root = TelemetryValue::deserialize(reader)?;
        Ok(Telemetry(root))
    }

    #[inline]
    pub fn extend(&mut self, that: Self) {
        self.0.extend(&that.0);
    }

    #[tracing::instrument(level = "trace", skip(oso))]
    pub fn initialize_policy_engine(oso: &mut oso::Oso) -> Result<(), PolicyError> {
        oso.register_class(Telemetry::get_polar_class())?;

        oso.register_class(
            oso::ClassBuilder::<TelemetryValue>::with_default()
                .add_method("to_polar", |v: &TelemetryValue| v.clone().to_polar())
                .build(),
        )?;

        Ok(())
    }
}

impl From<TelemetryValue> for Telemetry {
    fn from(value: TelemetryValue) -> Self {
        match value {
            inner @ TelemetryValue::Table(_) => Self(inner),
            tv => unreachable!("can only convert Tables into Telemetry, not {:?}", tv),
        }
    }
}

impl Into<TelemetryValue> for Telemetry {
    fn into(self) -> TelemetryValue {
        self.0
    }
}

impl std::ops::Deref for Telemetry {
    type Target = HashMap<String, TelemetryValue>;

    fn deref(&self) -> &Self::Target {
        if let TelemetryValue::Table(ref inner) = self.0 {
            &*inner
        } else {
            panic!("root telemetry value must be a Table, but is {:?}", self.0);
        }
    }
}

impl std::ops::DerefMut for Telemetry {
    fn deref_mut(&mut self) -> &mut Self::Target {
        if let TelemetryValue::Table(ref mut inner) = self.0 {
            &mut *inner
        } else {
            panic!("root telemetry value must be a Table, but is {:?}.", self.0);
        }
    }
}

impl Into<Telemetry> for HashMap<String, TelemetryValue> {
    #[tracing::instrument(level = "trace", skip())]
    fn into(self) -> Telemetry {
        Telemetry(TelemetryValue::Table(self))
    }
}

impl Into<Telemetry> for BTreeMap<String, TelemetryValue> {
    #[tracing::instrument(level = "trace", skip())]
    fn into(self) -> Telemetry {
        self.into_iter().collect()
    }
}

impl std::ops::Add for Telemetry {
    type Output = Self;

    #[tracing::instrument(level = "trace", skip())]
    fn add(mut self, rhs: Self) -> Self::Output {
        self.0.extend(&rhs.0);
        self
    }
}

impl FromIterator<(String, TelemetryValue)> for Telemetry {
    fn from_iter<T: IntoIterator<Item = (String, TelemetryValue)>>(iter: T) -> Self {
        Telemetry(TelemetryValue::Table(iter.into_iter().collect()))
    }
}

impl<'a> FromIterator<(&'a str, TelemetryValue)> for Telemetry {
    fn from_iter<T: IntoIterator<Item = (&'a str, TelemetryValue)>>(iter: T) -> Self {
        Telemetry(TelemetryValue::Table(
            iter.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        ))
    }
}

impl<'a> IntoIterator for &'a Telemetry {
    type IntoIter = std::collections::hash_map::Iter<'a, String, TelemetryValue>;
    type Item = (&'a String, &'a TelemetryValue);

    #[inline]
    #[tracing::instrument(level = "trace", skip())]
    fn into_iter(self) -> Self::IntoIter {
        if let TelemetryValue::Table(ref table) = self.0 {
            table.iter()
        } else {
            panic!("telemetry root value must be a Table, but was {:?}", self.0);
        }
    }
}

impl<'a> IntoIterator for &'a mut Telemetry {
    type IntoIter = std::collections::hash_map::IterMut<'a, String, TelemetryValue>;
    type Item = (&'a String, &'a mut TelemetryValue);

    #[inline]
    #[tracing::instrument(level = "trace", skip())]
    fn into_iter(self) -> Self::IntoIter {
        if let TelemetryValue::Table(ref mut table) = self.0 {
            table.iter_mut()
        } else {
            panic!("telemetry root value must be a Table, but was {:?}", self.0);
        }
    }
}

impl IntoIterator for Telemetry {
    type IntoIter = std::collections::hash_map::IntoIter<String, TelemetryValue>;
    type Item = (String, TelemetryValue);

    #[inline]
    #[tracing::instrument(level = "trace", skip())]
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
    use ::serde::{Deserialize, Serialize};
    use chrono::{DateTime, Utc};
    use itertools::Itertools;
    use pretty_assertions::assert_eq;

    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Data {
        #[serde(default)]
        #[serde(
            rename = "task.last_failure",
            serialize_with = "crate::serde::date::serialize_optional_datetime_map",
            deserialize_with = "crate::serde::date::deserialize_optional_datetime"
        )]
        pub last_failure: Option<DateTime<Utc>>,
        #[serde(rename = "cluster.is_deploying")]
        pub is_deploying: bool,
        #[serde(rename = "cluster.last_deployment", with = "crate::serde")]
        pub last_deployment: DateTime<Utc>,
    }

    #[test]
    fn test_basic_serde() {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_telemetry_basic_serde");
        let _main_span_guard = main_span.enter();

        let telemetry = TelemetryValue::Table(maplit::hashmap! {
            "place".to_string() => TelemetryValue::Table(maplit::hashmap! {
                "foo".to_string() => 37.into(),
                "bar".to_string() => 19.into(),
            })
        });

        let mut serializer = flexbuffers::FlexbufferSerializer::new();
        telemetry.serialize(&mut serializer).unwrap();
        let reader = flexbuffers::Reader::get_root(serializer.view()).unwrap();
        tracing::warn!("reader map: {:?}", reader.as_map());
        let actual = TelemetryValue::deserialize(reader).unwrap();
        tracing::warn!(?actual, expected=?telemetry, "actual vs expected");
        assert_eq!(actual, telemetry);
    }

    #[test]
    fn test_simple_struct() {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_simple_struct");
        let _main_span_guard = main_span.enter();

        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct TestData {
            foo: i32,
            bar: String,
            zed: f64,
        }

        let data = TestData {
            foo: 37,
            bar: "Otis".to_string(),
            zed: std::f64::consts::LN_2,
        };

        let telemetry: Telemetry = maplit::hashmap! {
                "foo".to_string() => 37.into(),
                "bar".to_string() => "Otis".into(),
                "zed".to_string() => std::f64::consts::LN_2.into(),
        }
        .into_iter()
        .collect();

        let actual_data: TestData = telemetry.clone().try_into().unwrap();
        assert_eq!(actual_data, data);

        let actual_telemetry = Telemetry::try_from(&actual_data).unwrap();
        assert_eq!(actual_telemetry, telemetry);
    }

    #[test]
    fn test_telemetry_map_struct() {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_telemetry_map_struct");
        let _main_span_guard = main_span.enter();

        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct TestData {
            place: HashMap<String, i32>,
        }

        let data = TestData {
            place: maplit::hashmap! {
                "foo".to_string() => 37,
                "bar".to_string() => 19,
            },
        };

        let telemetry: Telemetry = maplit::hashmap! {
            "place".to_string() => TelemetryValue::Table(maplit::hashmap! {
                "foo".to_string() => 37.into(),
                "bar".to_string() => 19.into(),
            })
        }
        .into_iter()
        .collect();

        let actual_data: TestData = telemetry.clone().try_into().unwrap();
        assert_eq!(actual_data, data);

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
                    result = self.elements.iter().zip_eq(&other.elements).all(|(lhs, rhs)| lhs == rhs);
                }
                result
            }
        }

        let data = TestData {
            elements: vec!["Otis".to_string(), "Stella".to_string(), "Neo".to_string()],
        };

        let telemetry: Telemetry = maplit::hashmap! {
            "arr".to_string() => TelemetryValue::Seq(vec![
                TelemetryValue::Text("Otis".to_string()),
                TelemetryValue::Text("Stella".to_string()),
                TelemetryValue::Text("Neo".to_string()),
            ])
        }
        .into_iter()
        .collect();

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
                "green".to_string() => Diode::Off,
                // "lt-green".to_string() => Diode::Off,
                // "red".to_string() => Diode::Brightness(100),
                // "blue".to_string() => Diode::Blinking(300, 700),
                // "white".to_string() => Diode::Pattern { name: "christmas".to_string(), infinite: true,},
            },
        };

        let telemetry: Telemetry = maplit::hashmap! {
            "diodes".to_string() => TelemetryValue::Table(maplit::hashmap! {
                "green".to_string() => TelemetryValue::Text("off".to_string()),
                // "green".to_string() => TelemetryValue::Table(maplit::hashmap! { "off".to_string() => TelemetryValue::Unit}),
                // "lt-green".to_string() => TelemetryValue::Table(maplit::hashmap! { "off".to_string() => TelemetryValue::Unit}),
                // "red".to_string() => TelemetryValue::Table(maplit::hashmap!{"brightness".to_string() => TelemetryValue::Integer(100)}),
                // "blue".to_string() => TelemetryValue::Table(maplit::hashmap!{"blinking".to_string() => TelemetryValue::Seq(vec![TelemetryValue::Integer(300), TelemetryValue::Integer(700)])}),
                // "white".to_string() => TelemetryValue::Table(maplit::hashmap!{"pattern".to_string() => TelemetryValue::Table(maplit::hashmap! {
                //     "name".to_string() => TelemetryValue::Text("christmas".to_string()),
                //     "infinite".to_string() => TelemetryValue::Boolean(true),
                // })})
            }),
        }
        .into_iter()
        .collect();

        tracing::warn!("deserializing actual data from telemetry...");
        let actual_data: TestData = telemetry.clone().try_into().unwrap();
        tracing::warn!(?actual_data, "deserialized actual data");
        assert_eq!(actual_data, data);

        tracing::warn!(?actual_data, "creating telemetry from actual_data");
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
            },
        };

        let telemetry: Telemetry = maplit::hashmap! {
            "diodes".to_string() => TelemetryValue::Table(maplit::hashmap! {
                // "green".to_string() => TelemetryValue::Table(maplit::hashmap! { "off".to_string() => TelemetryValue::Unit}),
                "green".to_string() => TelemetryValue::Text("off".to_string()),
                "red".to_string() => TelemetryValue::Table(maplit::hashmap!{"brightness".to_string() => TelemetryValue::Integer(100)}),
                "blue".to_string() => TelemetryValue::Table(maplit::hashmap!{"blinking".to_string() => TelemetryValue::Seq(vec![TelemetryValue::Integer(300), TelemetryValue::Integer(700)])}),
                "white".to_string() => TelemetryValue::Table(maplit::hashmap!{"pattern".to_string() => TelemetryValue::Table(maplit::hashmap! {
                    "name".to_string() => TelemetryValue::Text("christmas".to_string()),
                    "infinite".to_string() => TelemetryValue::Boolean(true),
                })})
            }),
        }.into_iter().collect();

        tracing::warn!("deserializing actual data from telemetry...");
        let actual_data: TestData = telemetry.clone().try_into().unwrap();
        tracing::warn!(?actual_data, "deserialized actual data");
        assert_eq!(actual_data, data);

        tracing::warn!(?actual_data, "creating telemetry from actual_data");
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

    use std::convert::TryFrom;

    use crate::elements::TelemetryValue;

    #[test]
    fn test_telemetry_data_try_into_deserializer() -> anyhow::Result<()> {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_telemetry_data_try_into_deserializer");
        let _main_span_guard = main_span.enter();

        let data: Telemetry = maplit::btreemap! {
            "task.last_failure".to_string() => "2014-11-28T12:45:59.324310806Z".into(),
            "cluster.is_deploying".to_string() => false.into(),
            "cluster.last_deployment".to_string() => "2014-11-28T10:11:37.246310806Z".into(),
        }
        .into_iter()
        .collect();

        let foo: Option<bool> = data
            .get("cluster.is_deploying")
            .and_then(|f| bool::try_from(f.clone()).ok()); // bool::from_telemetry(f.clone()).unwrap());
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
        let mut telemetry: Telemetry = (0..8).map(|x| (x.to_string(), TelemetryValue::Integer(x * 10))).collect();
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
            vec![
                (0.to_string(), TelemetryValue::Integer(0)),
                (2.to_string(), TelemetryValue::Integer(20)),
                (4.to_string(), TelemetryValue::Integer(40)),
                (6.to_string(), TelemetryValue::Integer(60)),
            ]
            .into_iter()
            .collect()
        );
    }
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
// impl FromTelemetry for Data {
//     fn from_telemetry(val: TelemetryValue) -> GraphResult<Self> {
//         if let TelemetryValue::Map(table) = val {
//             let last_failure = table.get(&"task.last_failure".to_string()).map(|t| );
//             let is_deploying
//         } else {
//             Err(crate::error::StageError::TypeError("TelemetryValue::Map".to_string()))
//         }
//     }
// }

// #[test]
// fn test_telemetry_serde_tokens() {
//     let data = Telemetry::from_iter(maplit::hashmap! {
//         "task.last_failure".to_string() => "2014-11-28T12:45:59.324310806Z".into(),
//         // "cluster.is_deploying".to_string() => false.into(),
//         // "cluster.last_deployment".to_string() => "2014-11-28T10:11:37.246310806Z".into(),
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

//  #[test]
// #[ignore]
// fn test_telemetry_try_into_tokens() {
// lazy_static::initialize(&crate::tracing::TEST_TRACING);
// let data = Telemetry::from_iter(maplit::hashmap! {
//     "task.last_failure".to_string() => "2014-11-28T12:45:59.324310806Z".into(),
//     // "cluster.is_deploying".to_string() => false.into(),
//     // "cluster.last_deployment".to_string() => "2014-11-28T10:11:37.246310806Z".into(),
// });
//
// let config_shape: config::Value = data.0.into();
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
// }

// #[test]
// fn test_telemetry_identity_try_into() -> anyhow::Result<()> {
//     lazy_static::initialize(&crate::tracing::TEST_TRACING);
//     let data = Telemetry::from_iter(maplit::hashmap! {
//         "task.last_failure".to_string() => "2014-11-28T12:45:59.324310806Z".into(),
//         // "cluster.is_deploying".to_string() => false.into(),
//         // "cluster.last_deployment".to_string() => "2014-11-28T10:11:37.246310806Z".into(),
//     });
//
//     // let foo: Option<bool> = data
//     //     .get("cluster.is_deploying")
//     //     .map(|f| bool::from_telemetry(f.clone()).unwrap());
//     // assert_eq!(foo, Some(false));
//
//     let expected = Telemetry(
//         maplit::hashmap! {
//             "task.last_failure".to_string() =>
// TelemetryValue::Text("2014-11-28T12:45:59.324310806Z".to_string()),             //
// "cluster.is_deploying".to_string() => TelemetryValue::Boolean(false),             //
// "cluster.last_deployment".to_string() =>
// TelemetryValue::Text("2014-11-28T10:11:37.246310806Z".to_string()),         }
//         .into(),
//     );
//     tracing::info!(telemetry=?data, ?expected, "expected conversion from telemetry data.");
//     let actual = data.try_into::<Telemetry>();
//     tracing::info!(?actual, "actual conversion from telemetry data.");
//     assert_eq!(actual?, expected);
//     Ok(())
// }
