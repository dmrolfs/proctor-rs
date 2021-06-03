//todo This file tried to define Telemetry as a Deserializer; meaning values could be directly
// deserialized from it. I ran into problems getting Enum Variants right and particular trouble with
// recursive structures; e.g., Seq of Tables, etc.

// use super::{Telemetry, TelemetryValue, FromTelemetry};
// use crate::elements::telemetry::value::Table;
// use crate::error::StageError;
// use serde::de;
// use serde::forward_to_deserialize_any;
// use std::collections::{HashMap, VecDeque};
// use std::convert::TryFrom;
// use std::iter::Enumerate;
// use crate::elements::ToTelemetry;
//
// impl<'de> de::Deserializer<'de> for TelemetryValue {
//     type Error = StageError;
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_any<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         match self {
//             TelemetryValue::Unit => visitor.visit_unit(),
//             TelemetryValue::Integer(i) => visitor.visit_i64(i),
//             TelemetryValue::Boolean(b) => visitor.visit_bool(b),
//             TelemetryValue::Float(f) => visitor.visit_f64(f),
//             TelemetryValue::Text(s) => visitor.visit_string(s),
//             TelemetryValue::Seq(values) => visitor.visit_seq(SeqAccess::new(values)),
//             TelemetryValue::Table(table) => visitor.visit_map(MapAccess::new(table)),
//         }
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_bool<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_bool(bool::from_telemetry(self)?)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_i8<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_i8(i8::from_telemetry(self)?)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_i16<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_i16(i16::from_telemetry(self)?)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_i32<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_i32(i32::from_telemetry(self)?)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_i64<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_i64(i64::from_telemetry(self)?)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_u8<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_u8(u8::from_telemetry(self)?)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_u16<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_u16(u16::from_telemetry(self)?)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_u32<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_u32(u32::from_telemetry(self)?)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_u64<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_u64(u32::from_telemetry(self)? as u64)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_f32<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_f32(f32::from_telemetry(self)?)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_f64<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_f64(f64::from_telemetry(self)?)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_str<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_string(String::from_telemetry(self)?)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_string<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_string(String::from_telemetry(self)?)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_option<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         match self {
//             TelemetryValue::Unit => visitor.visit_none(),
//             _ => visitor.visit_some(self),
//         }
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_newtype_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value, Self::Error>
//     where
//         V: de::Visitor<'de>,
//     {
//         visitor.visit_newtype_struct(self)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_enum<V>(
//         self, name: &'static str, variants: &'static [&'static str], visitor: V,
//     ) -> Result<V::Value, Self::Error>
//     where
//         V: de::Visitor<'de>,
//     {
//         visitor.visit_enum(EnumAccess {
//             value: self,
//             name,
//             variants,
//         })
//     }
//
//     forward_to_deserialize_any! {
//         char seq
//         bytes byte_buf map struct unit
//         identifier ignored_any unit_struct tuple_struct tuple
//     }
// }
//
// #[derive(Debug)]
// struct StrDeserializer<'a>(&'a str);
//
// impl<'a> StrDeserializer<'a> {
//     fn new(key: &'a str) -> Self {
//         StrDeserializer(key)
//     }
// }
//
// impl<'de, 'a> de::Deserializer<'de> for StrDeserializer<'a> {
//     type Error = StageError;
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_any<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_str(self.0)
//     }
//
//     forward_to_deserialize_any! {
//         bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string seq
//         bytes byte_buf map struct unit enum newtype_struct
//         identifier ignored_any unit_struct tuple_struct tuple option
//     }
// }
//
// #[derive(Debug)]
// struct SeqAccess {
//     elements: Enumerate<::std::vec::IntoIter<TelemetryValue>>,
// }
//
// impl SeqAccess {
//     #[tracing::instrument(level="trace", skip())]
//     fn new(elements: Vec<TelemetryValue>) -> Self {
//         SeqAccess {
//             elements: elements.into_iter().enumerate(),
//         }
//     }
// }
//
// impl<'de> de::SeqAccess<'de> for SeqAccess {
//     type Error = StageError;
//
//     #[tracing::instrument(level="trace", skip(seed))]
//     fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
//     where
//         T: de::DeserializeSeed<'de>,
//     {
//         match self.elements.next() {
//             Some((_idx, value)) => {
//                 seed
//                     .deserialize(value)
//                     .map(Some)
//                     // .map_err(|e| e.prepend_index(idx))
//             },
//             None => Ok(None),
//         }
//     }
//
//     #[tracing::instrument(level="trace", skip())]
//     fn size_hint(&self) -> Option<usize> {
//         match self.elements.size_hint() {
//             (lower, Some(upper)) if lower == upper => Some(upper),
//             _ => None,
//         }
//     }
// }
//
// #[derive(Debug)]
// struct MapAccess {
//     elements: VecDeque<(String, TelemetryValue)>,
// }
//
// impl MapAccess {
//     #[tracing::instrument(level="trace", skip())]
//     fn new(table: HashMap<String, TelemetryValue>) -> Self {
//         MapAccess {
//             elements: table.into_iter().collect(),
//         }
//     }
// }
//
// impl<'de> de::MapAccess<'de> for MapAccess {
//     type Error = StageError;
//
//     #[tracing::instrument(level="trace", skip(seed))]
//     fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
//     where
//         K: de::DeserializeSeed<'de>,
//     {
//         if let Some(&(ref key_s, _)) = self.elements.front() {
//             let key_de: TelemetryValue = key_s.clone().to_telemetry();
//             // let key_de = TelemetryValue::new(None, key_s as &str);
//             let key = de::DeserializeSeed::deserialize(seed, key_de)?;
//
//             Ok(Some(key))
//         } else {
//             Ok(None)
//         }
//     }
//
//     #[tracing::instrument(level="trace", skip(seed))]
//     fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
//     where
//         V: de::DeserializeSeed<'de>,
//     {
//         let (_key, value) = self.elements.pop_front().unwrap();
//         de::DeserializeSeed::deserialize(seed, value)
//             // .map_err(|e| e.prepend_key(key))
//     }
// }
//
// #[derive(Debug)]
// struct EnumAccess {
//     value: TelemetryValue,
//     name: &'static str,
//     variants: &'static [&'static str],
// }
//
// impl EnumAccess {
//     #[tracing::instrument(level="trace", skip())]
//     fn variant_deserializer(&self, name: &str) -> Result<StrDeserializer, StageError> {
//         self.variants
//             .iter()
//             .find(|&&s| s == name)
//             .map(|&s| StrDeserializer(s))
//             .ok_or_else(|| self.no_constructor_error(name))
//     }
//
//     #[tracing::instrument(level="trace", skip())]
//     fn table_deserializer(&self, table: &Table) -> Result<StrDeserializer, StageError> {
//         if table.len() == 1 {
//             self.variant_deserializer(table.iter().next().unwrap().0)
//         } else {
//             Err(self.structural_error())
//         }
//     }
//
//     #[tracing::instrument(level="trace", skip())]
//     fn no_constructor_error(&self, supposed_variant: &str) -> StageError {
//         StageError::GraphSerde(format!(
//             "enum {} does not have variant constructor {}",
//             self.name, supposed_variant
//         ))
//     }
//
//     #[tracing::instrument(level="trace", skip())]
//     fn structural_error(&self) -> StageError {
//         StageError::GraphSerde(format!(
//             "value of enum {} should be represented by either string or table with exactly one key",
//             self.name
//         ))
//     }
// }
//
// impl<'de> de::EnumAccess<'de> for EnumAccess {
//     type Error = StageError;
//     type Variant = Self;
//
//     #[tracing::instrument(level="trace", skip(seed))]
//     fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
//     where
//         V: de::DeserializeSeed<'de>,
//     {
//         let value = {
//             let deserializer = match self.value {
//                 TelemetryValue::Text(ref s) => self.variant_deserializer(s),
//                 TelemetryValue::Table(ref t) => self.table_deserializer(&t),
//                 _ => Err(self.structural_error()),
//             }?;
//             seed.deserialize(deserializer)?
//         };
//
//         Ok((value, self))
//     }
// }
//
// impl<'de> de::VariantAccess<'de> for EnumAccess {
//     type Error = StageError;
//
//     #[tracing::instrument(level="trace", skip())]
//     fn unit_variant(self) -> Result<(), Self::Error> {
//         Ok(())
//     }
//
//     #[tracing::instrument(level="trace", skip(seed))]
//     fn newtype_variant_seed<T: de::DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value, Self::Error> {
//         match self.value {
//             TelemetryValue::Table(t) => seed.deserialize(t.into_iter().next().unwrap().1),
//             _ => unreachable!(),
//         }
//     }
//
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn tuple_variant<V: de::Visitor<'de>>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error> {
//         match self.value {
//             TelemetryValue::Table(t) => de::Deserializer::deserialize_seq(t.into_iter().next().unwrap().1, visitor),
//             _ => unreachable!(),
//         }
//     }
//
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn struct_variant<V>(self, _fields: &'static [&'static str], visitor: V) -> Result<V::Value, Self::Error>
//     where
//         V: de::Visitor<'de>,
//     {
//         match self.value {
//             TelemetryValue::Table(t) => de::Deserializer::deserialize_map(t.into_iter().next().unwrap().1, visitor),
//             _ => unreachable!(),
//         }
//     }
// }
//
// impl<'de> de::Deserializer<'de> for Telemetry {
//     type Error = StageError;
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_any<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         // Deserialize based on the underlying type
//         match self.0 {
//             TelemetryValue::Unit => {
//                 tracing::trace!("visiting unit");
//                 visitor.visit_unit()
//             },
//             TelemetryValue::Integer(i) => {
//                 tracing::trace!(?i, "visiting int");
//                 visitor.visit_i64(i)
//             },
//             TelemetryValue::Boolean(b) => {
//                 tracing::trace!(?b, "visiting bool");
//                 visitor.visit_bool(b)
//             },
//             TelemetryValue::Float(f) => {
//                 tracing::trace!(?f, "visiting float");
//                 visitor.visit_f64(f)
//             },
//             TelemetryValue::Text(text) => {
//                 tracing::trace!(?text, "visiting text");
//                 visitor.visit_string(text)
//             },
//             TelemetryValue::Seq(seq) => {
//                 tracing::trace!(?seq, "visiting seq");
//                 visitor.visit_seq(SeqAccess::new(seq))
//             },
//             TelemetryValue::Table(table) => {
//                 tracing::trace!(?table, "visiting map");
//                 visitor.visit_map(MapAccess::new(table))
//             },
//         }
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_bool<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_bool(bool::try_from(self.0)?)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_i8<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_i8(i8::try_from(self.0)?)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_i16<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_i16(i16::try_from(self.0)?)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_i32<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_i32(i32::try_from(self.0)?)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_i64<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_i64(i64::try_from(self.0)?)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_u8<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_u8(u8::try_from(self.0)?)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_u16<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_u16(u16::try_from(self.0)?)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_u32<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_u32(u32::try_from(self.0)?)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_u64<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_u64(u32::try_from(self.0)? as u64)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_f32<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_f32(f32::try_from(self.0)?)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_f64<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_f64(f64::try_from(self.0)?)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_str<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_string(String::try_from(self.0)?)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_string<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         visitor.visit_string(String::try_from(self.0)?)
//     }
//
//     #[inline]
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_option<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
//         // Match an explicit nil as None and everything else as Some
//         match self.0 {
//             TelemetryValue::Unit => visitor.visit_none(),
//             _ => visitor.visit_some(self),
//         }
//     }
//
//     #[tracing::instrument(level="trace", skip(visitor))]
//     fn deserialize_enum<V: de::Visitor<'de>>(
//         self, name: &'static str, variants: &'static [&'static str], visitor: V,
//     ) -> Result<V::Value, Self::Error> {
//         visitor.visit_enum(EnumAccess {
//             value: self.0,
//             name,
//             variants,
//         })
//     }
//
//     forward_to_deserialize_any! {
//         char seq
//         bytes byte_buf map struct unit newtype_struct
//         identifier ignored_any unit_struct tuple_struct tuple
//     }
// }
