use crate::error::GraphError;
use crate::graph::GraphResult;
use oso::PolarClass;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_cbor::Value;
use std::collections::BTreeMap;
use std::iter::{FromIterator, IntoIterator};

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TelemetryData(Value);

impl Default for TelemetryData {
    fn default() -> Self {
        Self(Value::Map(BTreeMap::default()))
    }
}

impl TelemetryData {
    pub fn from_cbor<T: DeserializeOwned>(value: Value) -> GraphResult<T> {
        serde_cbor::value::from_value(value).map_err(|err| err.into())
    }

    pub fn new() -> Self {
        Self::default()
    }

    // pub fn with_capacity(capacity: usize) -> Self {
    //     Self(HashMap::with_capacity(capacity))
    // }

    pub fn from_data(data: BTreeMap<String, Value>) -> Self {
        let payload = serde_cbor::value::to_value(data).unwrap();
        Self(payload)

        // let payload = data
        //     .into_iter()
        //     .map(|(key, value)| {
        //         let key_rep = serde_cbor::value::to_value(key).unwrap();
        //         (key_rep, value)
        //     })
        //     .collect();
        // Self(Value::Map(payload))
        // let payload = serde_cbor::to_vec(&data).unwrap();
        // Self(payload)
        // let mut config = Config::new();
        // for (k, v) in data {
        //     config.set(k.as_str(), v);
        // }
        // Self(config)
    }

    pub fn try_into<T: DeserializeOwned>(self) -> GraphResult<T> {
        serde_cbor::value::from_value::<T>(self.0).map_err(|err| err.into())
        // serde_cbor::from_slice::<T>(&self.0).map_err(|err| err.into())
        // self.0.try_into().map_err(|err| err.into())
        // let mut c = config::Config::default();
        // c.merge(self.0)?;
        // let c = config::Config::try_from(&self.0)?;
        // c.try_into().map_err(|err| err.into())
    }

    pub fn merge(&mut self, that: Self) {
        let mut my_data: BTreeMap<Value, Value> = serde_cbor::value::from_value(self.0.clone()).unwrap();
        let that_data: BTreeMap<Value, Value> = serde_cbor::value::from_value(that.0.clone()).unwrap();

        my_data.extend(that_data);
        self.0 = Value::Map(my_data);

        // my_data.extend(iter);
        // let my_value = Value::Map(my_data);
        // self.0 = my_value;
        // self.0 = Value::Map(my_data);

        // let mut data = self.dictionary().unwrap();
        // let that_data = that.dictionary().unwrap();
        // data.extend(that_data);
        // self.0 = serde_cbor::to_vec(&data).unwrap();
    }

    pub fn dictionary(&self) -> BTreeMap<String, Value> {
        serde_cbor::value::from_value(self.0.clone()).unwrap()
        // let dict: BTreeMap<String, Value> = serde_cbor::from_slice(&self.0)?;
        // Ok(dict)

        // let value = serde_cbor::from_slice::<serde_cbor::Value>(&self.0)?;
        // if let serde_cbor::Value::Map(map) = value {
        //     let keys = map.into_iter()
        //         .map(|(k,v)| {
        //             let rep = serde_cbor::value::from_value::<String>(k)?;
        //             (rep, v)
        //         })
        //         .into_keys()
        //     Ok(keys)
        // } else {
        //     GraphError::GraphSerde(format!("{:?} type doesn't support keys", value))
        // }
        // let foo: serde_cbor::Value = serde_cbor::from_slice(self.0).unwrap();
        // foo.
        // let table = self.0.clone().collect().unwrap();
        // table.keys()
    }

    pub fn is_empty(&self) -> bool {
        if let Value::Map(my_data) = &self.0 {
            my_data.is_empty()
        } else {
            panic!(
                "{:?}",
                GraphError::GraphPrecondition(format!("telemetry data not a Map but a {:?}", self.0))
            );
        }
    }

    /// Returns a reference to the value corresponding to the key.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use proctor::elements::TelemetryData;
    /// use serde_cbor::Value;
    ///
    /// let mut telemetry = TelemetryData::new();
    /// telemetry.insert("a", Value::Integer(1));
    /// assert_eq!(telemetry.get::<i32>("a").unwrap(), Some(1));
    /// assert_eq!(telemetry.get::<i32>("b").unwrap(), None);
    /// ```
    pub fn get<V: DeserializeOwned>(&self, key: &str) -> GraphResult<Option<V>> {
        let cbor_key = serde_cbor::value::to_value(key)?;
        let my_data: BTreeMap<Value, Value> = serde_cbor::value::from_value(self.0.clone())?;
        match my_data.get(&cbor_key) {
            Some(v) => serde_cbor::value::from_value::<V>(v.clone())
                .map_err(|err| err.into())
                .map(|r| Some(r)),
            None => Ok(None),
        }
    }

    /// Inserts a key-value pair into telemetry.
    ///
    /// If telemetry did not have this key present, `None` is returned.
    ///
    /// If telemetry did have this key present, the value is updated, and the old
    /// value is returned. The key is not updated, though; this matters for
    /// types that can be `==` without being identical.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use proctor::elements::TelemetryData;
    /// use serde_cbor::Value;
    ///
    /// let mut telemetry = TelemetryData::new();
    /// assert_eq!(telemetry.insert("a", 37), None);
    /// assert_eq!(telemetry.is_empty(), false);
    ///
    /// telemetry.insert("a", 54);
    /// assert_eq!(telemetry.insert("a", 75), Some(54));
    /// ```
    pub fn insert<V: Serialize + DeserializeOwned>(&mut self, key: &str, value: V) -> Option<V> {
        let mut my_data: BTreeMap<Value, Value> = serde_cbor::value::from_value(self.0.clone()).unwrap();
        let cbor_key = serde_cbor::value::to_value(key).unwrap();
        let cbor_value = serde_cbor::value::to_value(value).unwrap();
        let replaced = my_data.insert(cbor_key, cbor_value);
        self.0 = Value::Map(my_data);

        match replaced {
            Some(replaced) => serde_cbor::value::from_value::<V>(replaced).map(|r| Some(r)).unwrap(),
            None => None,
        }
    }

    /// Returns `true` if the telemetry contains a value for the specified key.
    ///
    /// The key may be anything that can implements `Into<String>`.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use proctor::elements::TelemetryData;
    ///
    /// let mut telemetry = TelemetryData::new();
    /// telemetry.insert("foo", 17);
    /// assert_eq!(telemetry.contains_key("foo"), true);
    /// assert_eq!(telemetry.contains_key("bar"), false);
    /// ```
    pub fn remove<V: DeserializeOwned>(&mut self, key: &str) -> GraphResult<Option<V>> {
        let mut my_data: BTreeMap<Value, Value> = serde_cbor::value::from_value(self.0.clone())?;
        let cbor_key = serde_cbor::value::to_value(key)?;
        let removed_value = my_data.remove(&cbor_key);
        self.0 = Value::Map(my_data);
        match removed_value {
            Some(removed) => serde_cbor::value::from_value::<V>(removed)
                .map(|r| Some(r))
                .map_err(|err| err.into()),
            None => Ok(None),
        }
    }

    pub fn contains_key(&self, key: &str) -> bool {
        if let Ok(cbor_key) = serde_cbor::value::to_value(key) {
            match &self.0 {
                Value::Map(my_data) => my_data.contains_key(&cbor_key),
                _ => false,
            }
        } else {
            false
        }
    }

    /// Retains only the elements specified by the predicate.
    ///
    /// In other words, remove all pairs `(k, v)` such that `f(&k, &mut v)` returns `false`.
    ///
    /// # Examples
    ///
    /// ```
    /// use proctor::elements::TelemetryData;
    /// use serde_cbor::Value;
    ///
    /// let mut telemetry: TelemetryData = (0..8).map(|x| (x.to_string(), Value::Integer(x*10))).collect();
    /// // Keep only the elements with even-numbered keys.
    /// telemetry.retain(|key, _| {
    ///     let key = TelemetryData::from_cbor::<String>(key.clone()).unwrap().parse::<i32>().unwrap();
    ///     key % 2 == 0
    /// });
    /// assert!(telemetry.dictionary().into_iter().eq(vec![
    ///     (0.to_string(), Value::Integer(0)),
    ///     (2.to_string(), Value::Integer(20)),
    ///     (4.to_string(), Value::Integer(40)),
    ///     (6.to_string(), Value::Integer(60))
    /// ]));
    /// ```
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&Value, &Value) -> bool,
    {
        let my_data: BTreeMap<Value, Value> = serde_cbor::value::from_value(self.0.clone()).unwrap();
        let retained = my_data.into_iter().filter(|(k, v)| f(k, v)).collect();
        self.0 = Value::Map(retained);

        // self.drain_filter(|k, v| !f(k, v));
    }
}

// impl config::Source for TelemetryData {
//     fn clone_into_box(&self) -> Box<dyn Source + Send + Sync> {
//         self.0.clone_into_box()
//     }
//
//     fn collect(&self) -> std::result::Result<HashMap<String, Value>, config::ConfigError> { //Result<HashMap<String, Value>> {
//         self.0.collect()
//     }
// }

// impl std::ops::Deref for TelemetryData {
//     type Target = Config;
//
//     fn deref(&self) -> &Self::Target {
//         let inner = &self.0;
//         &*inner
//     }
// }

// impl std::ops::DerefMut for TelemetryData {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         let inner = &mut self.0;
//         &mut *inner
//     }
// }

// impl Into<TelemetryData> for HashMap<String, String> {
//     fn into(self) -> TelemetryData {
//         TelemetryData(self)
//     }
// }

impl Into<TelemetryData> for BTreeMap<String, Value> {
    fn into(self) -> TelemetryData {
        TelemetryData::from_data(self)
    }
}

impl std::ops::Add for TelemetryData {
    type Output = Self;
    fn add(self, rhs: Self) -> Self::Output {
        let mut my_data: BTreeMap<Value, Value> = serde_cbor::value::from_value(self.0).unwrap();
        let that_data: BTreeMap<Value, Value> = serde_cbor::value::from_value(rhs.0).unwrap();
        my_data.extend(that_data);
        Self(Value::Map(my_data))

        // let mut lhs = self.dictionary();
        // let rhs = self.dictionary();
        // lhs.extend(rhs);
        // let payload = serde_cbor::value::to_value(lhs).unwrap();
        // // let mut lhs = serde_cbor::value::to_value(self.0);
        // // lhs.merge(rhs.0);
        // Self(payload)
    }
}

impl FromIterator<(String, Value)> for TelemetryData {
    fn from_iter<T: IntoIterator<Item = (String, Value)>>(iter: T) -> Self {
        let data = iter.into_iter().collect::<BTreeMap<_, _>>();
        let payload = serde_cbor::value::to_value(data).unwrap();
        Self(payload)
        // let mut data = TelemetryData::default();
        // for (k, v) in iter {
        //     data.set(k.as_str(), v);
        // }
        // data
    }
}

// impl<'a> IntoIterator for &'a TelemetryData {
//     type Item = (&'a String, &'a String);
//     type IntoIter = config::Confi
//
//     #[inline]
//     fn into_iter(self) -> Self::IntoIter {
//         self.0.iter()
//     }
// }

// impl<'a> IntoIterator for &'a mut TelemetryData {
//     type Item = (&'a String, &'a mut String);
//     type IntoIter = std::collections::hash_map::IterMut<'a, String, String>;
//
//     #[inline]
//     fn into_iter(self) -> Self::IntoIter {
//         self.0.iter_mut()
//     }
// }

// impl IntoIterator for TelemetryData {
//     type Item = (String, String);
//     type IntoIter = std::collections::hash_map::IntoIter<String, String>;
//
//     #[inline]
//     fn into_iter(self) -> Self::IntoIter {
//         self.0.into_iter()
//     }
// }

// impl Serialize for TelemetryData {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//     {
//         let data: HashMap<String, String> = self.0.try_into()?;
//         let mut map = serializer.serialize_map(Some(data.len()))?;
//         for (k, v) in &self.0 {
//             map.serialize_entry(&k, &v)?;
//         }
//         map.end()
//     }
// }
//
// impl<'de> de::Deserialize<'de> for TelemetryData {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//          deserializer.deserialize_map(TelemetryDataVisitor::new())
//     }
// }
//
// struct TelemetryDataVisitor {
//     marker: PhantomData<fn() -> TelemetryData>,
// }
//
// impl TelemetryDataVisitor {
//     fn new() -> Self {
//         TelemetryDataVisitor { marker: PhantomData }
//     }
// }
//
// impl<'de> de::Visitor<'de> for TelemetryDataVisitor {
//     type Value = TelemetryData;
//
//     fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         f.write_str("telemetry data")
//     }
//
//     fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
//     where
//         M: de::MapAccess<'de>,
//     {
//         let mut telemetry_data = TelemetryData::new();
//         while let Some((key, value)) = access.next_entry()? {
//             telemetry_data.insert(key, value);
//         }
//         Ok(telemetry_data)
//     }
// }

// struct TelemetryDataDeserializer<'de> {
//     input: &'de str,
// }
//
// impl<'de> TelemetryDataDeserializer {
//     pub fn from_str(input: &'de str) -> Self {
//         Self { input }
//     }
// }
//
// pub fn from_str<'a, T: de::Deserialize<'a>>(s: &'a str) -> GraphResult<T> {
//     let mut deserializer = TelemetryDataDeserializer::from_str(s);
//     let t = T::deserialize(&mut deserializer)?;
//     if deserializer.input.is_empty() {
//         Ok(t)
//     } else {
//         Err(de::Error::TrailingCharacters)
//     }
// }
//
// impl<'de> TelemetryDataDeserializer<'de> {
//     // Look at the first character in the input without consuming it.
//     fn peek_char(&mut self) -> GraphResult<char> {
//         self.input.chars().new().ok_or(de::Error::Eof)
//     }
//
//     // Consume the first character in the input.
//     fn next_char(&mut self) -> GraphResult<char> {
//         let ch = self.peek_char()?;
//         self.input = &self.input[ch.len_utf8()..];
//         Ok(ch)
//     }
//
//     // Parse the JSON identifier `true` or `false`.
//     fn parse_bool(&mut self) -> GraphResult<bool> {
//         if self.input.starts_with("true") {
//             self.input = &self.input["true".len()..];
//             Ok(true)
//         } else if self.input.starts_with("false") {
//             self.input = &self.input["false".len()..];
//             Ok(false)
//         } else {
//             Err(de::Error::ExpectedBoolean)
//         }
//     }
//
//     // Parse a group of decimal digits as an unsigned integer of type T.
//     //
//     // This implementation is a bit too lenient, for example `001` is not
//     // allowed in JSON. Also the various arithmetic operations can overflow and
//     // panic or return bogus data. But it is good enough for example code!
//     fn parse_unsigned<T>(&mut self) -> GraphResult<T>
//     where
//         T: std::ops::AddAssign<T> + std::ops::MulAssign<T> + From<u8>,
//     {
//         let mut int = match self.next_char()? {
//             ch @ '0'..='9' => T::from(ch as u8 - b'0'),
//             _ => {
//                 return Err(Error::ExpectedInteger);
//             }
//         };
//         loop {
//             match self.input.chars().next() {
//                 Some(ch @ '0'..='9') => {
//                     self.input = &self.input[1..];
//                     int *= T::from(10);
//                     int += T::from(ch as u8 - b'0');
//                 }
//                 _ => {
//                     return Ok(int);
//                 }
//             }
//         }
//     }
//
//     // Parse a possible minus sign followed by a group of decimal digits as a
//     // signed integer of type T.
//     fn parse_signed<T>(&mut self) -> GraphResult<T>
//     where
//         T: std::ops::Neg<Output = T> + std::ops::AddAssign<T> + std::ops::MulAssign<T> + From<i8>,
//     {
//         // Optional minus sign, delegate to `parse_unsigned`, negate if negative.
//         unimplemented!()
//     }
//
//     // Parse a string until the next '"' character.
//     //
//     // Makes no attempt to handle escape sequences. What did you expect? This is
//     // example code!
//     fn parse_string(&mut self) -> GraphResult<&'de str> {
//         if self.next_char()? != '"' {
//             return Err(Error::ExpectedString);
//         }
//         match self.input.find('"') {
//             Some(len) => {
//                 let s = &self.input[..len];
//                 self.input = &self.input[len + 1..];
//                 Ok(s)
//             }
//             None => Err(Error::Eof),
//         }
//     }
// }
//
// impl<'de> de::Deserializer<'de> for TelemetryData {
//     type Error = GraphError;
//
//     fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
//     where
//         V: de::Visitor<'de>,
//     {
//         visitor.visit_map(MapAccess::new(self))
//     }
//
//     #[inline]
//     fn deserialize_bool<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_i8<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_i16<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_i32<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_i64<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_u8<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_u16<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_u32<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_u64<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_f32<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_f64<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_char<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_str<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_string<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_bytes<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_byte_buf<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_option<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_unit<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_unit_struct<V: de::Visitor<'de>>(self, name: &'static str, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_newtype_struct<V: de::Visitor<'de>>(self, name: &'static str, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_seq<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_tuple<V: de::Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_tuple_struct<V: de::Visitor<'de>>(self, name: &'static str, len: usize, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_map<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_struct<V: de::Visitor<'de>>(
//         self, name: &'static str, fields: &'static [&'static str], visitor: V,
//     ) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_enum<V: de::Visitor<'de>>(
//         self, name: &'static str, variants: &'static [&'static str], visitor: V,
//     ) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_identifier<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
//
//     fn deserialize_ignored_any<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         unimplemented!()
//     }
// }
//
// struct MapAccess {
//     elements: VecDeque<(String, Value)>,
// }
//
// impl MapAccess {
//     pub fn new(data: TelemetryData) -> Self {
//         MapAccess {
//             elements: VecDeque::from_iter(data),
//         }
//     }
// }
//
// impl<'de> de::MapAccess<'de> for MapAccess {
//     type Error = GraphError;
//
//     fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
//     where
//         K: de::DeserializeSeed<'de>,
//     {
//         if let Some(&(ref key_s, _)) = self.elements.front() {
//             // let key = de::DeserializeSeed::deserialize()
//             let key = de::DeserializeSeed::deserialize(seed, key_s)?;
//             Ok(Some(key))
//         } else {
//             Ok(None)
//         }
//     }
//
//     fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
//     where
//         V: de::DeserializeSeed<'de>,
//     {
//         let (key, value) = self.elements.pop_front().unwrap();
//         de::DeserializeSeed::deserialize(seed, value).map_err(|err| err.into())
//     }
// }

// /////////////////////////////////////////////////////
// // Unit Tests ///////////////////////////////////////
//
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use serde::Deserialize;

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
    fn test_telemetry_data_try_into_deserializer() -> anyhow::Result<()> {
        lazy_static::initialize(&crate::telemetry::TEST_TRACING);
        let data = TelemetryData::from_data(maplit::btreemap! {
            "task.last_failure".to_string() => Value::Text("2014-11-28T12:45:59.324310806Z".to_string()),
            "cluster.is_deploying".to_string() => Value::Bool(false),
            "cluster.last_deployment".to_string() => Value::Text("2014-11-28T10:11:37.246310806Z".to_string()),
        });

        let foo = data.get::<bool>("cluster.is_deploying")?;
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
        let mut telemetry: TelemetryData = (0..8).map(|x| (x.to_string(), Value::Integer(x*10))).collect();
        // Keep only the elements with even-numbered keys.
        telemetry.retain(|k, _| {
            let key = TelemetryData::from_cbor::<String>(k.clone()).unwrap().parse::<i32>().unwrap();
            key % 2 == 0
        });
        assert!(telemetry.dictionary().into_iter().eq(vec![
            (0.to_string(), Value::Integer(0)),
            (2.to_string(), Value::Integer(20)),
            (4.to_string(), Value::Integer(40)),
            (6.to_string(), Value::Integer(60))
        ]));
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
