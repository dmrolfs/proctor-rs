use crate::graph::GraphResult;
use serde::ser;
use std::fmt::Display;

use super::{Telemetry, TelemetryValue};
use crate::error::GraphError;
use crate::elements::ToTelemetry;

#[derive(Default, Debug)]
pub struct TelemetrySerializer {
    keys: Vec<(String, Option<usize>)>,
    pub output: Telemetry,
}

impl TelemetrySerializer {
    #[tracing::instrument(level="trace", skip(value))]
    fn serialize_primitive<T>(&mut self, value: T) -> GraphResult<()>
    where
        T: Into<TelemetryValue> + Display,
    {
        let key = match self.last_key_index_pair() {
            Some((key, Some(index))) => Ok(format!("{}[{}]", key, index)),
            Some((key, None)) => Ok(key.to_string()),
            None => Err(GraphError::GraphSerde(format!("key is not found for value {}", value))),
        }?;

        let _ = self.output.insert(key, value.into());
        Ok(())
    }

    #[tracing::instrument(level="trace", skip())]
    fn last_key_index_pair(&self) -> Option<(&str, Option<usize>)> {
        let len = self.keys.len();
        if 0 < len {
            self.keys.get(len - 1).map(|&(ref key, opt)| (key.as_str(), opt))
        } else {
            None
        }
    }

    #[tracing::instrument(level="trace", skip())]
    fn inc_last_key_index(&mut self) -> GraphResult<()> {
        let len = self.keys.len();
        if 0 < len {
            self.keys
                .get_mut(len - 1)
                .map(|pair| pair.1 = pair.1.map(|i| i + 1).or(Some(0)))
                .ok_or_else(|| GraphError::GraphSerde(format!("last key is not found in {} keys", len)))
        } else {
            Err(GraphError::GraphSerde("keys is empty".to_string()))
        }
    }

    #[tracing::instrument(level="trace", skip())]
    fn make_full_key(&self, key: &str) -> String {
        let len = self.keys.len();
        if 0 < len {
            if let Some(&(ref prev_key, index)) = self.keys.get(len - 1) {
                let full_key = if let Some(index) = index {
                    format!("{}[{}].{}", prev_key, index, key)
                } else {
                    format!("{}.{}", prev_key, key)
                };
                full_key
            } else {
                key.to_string()
            }
        } else {
            key.to_string()
        }
    }

    #[tracing::instrument(level="trace", skip())]
    fn push_key(&mut self, key: &str) {
        let full_key = self.make_full_key(key);
        self.keys.push((full_key, None));
    }

    #[tracing::instrument()]
    fn pop_key(&mut self) -> Option<(String, Option<usize>)> {
        self.keys.pop()
    }
}

type Result<T> = GraphResult<T>;

impl<'a> ser::Serializer for &'a mut TelemetrySerializer {
    type Ok = ();
    type Error = GraphError;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    #[tracing::instrument(level="trace", skip())]
    fn serialize_bool(self, v: bool) -> Result<Self::Ok> {
        self.serialize_primitive(v.to_telemetry())
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_i8(self, v: i8) -> Result<Self::Ok> {
        self.serialize_i64(v as i64)
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_i16(self, v: i16) -> Result<Self::Ok> {
        self.serialize_i64(v as i64)
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_i32(self, v: i32) -> Result<Self::Ok> {
        self.serialize_i64(v as i64)
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_i64(self, v: i64) -> Result<Self::Ok> {
        self.serialize_primitive(v.to_telemetry())
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_u8(self, v: u8) -> Result<Self::Ok> {
        self.serialize_u64(v as u64)
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_u16(self, v: u16) -> Result<Self::Ok> {
        self.serialize_u64(v as u64)
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_u32(self, v: u32) -> Result<Self::Ok> {
        self.serialize_u64(v as u64)
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_u64(self, v: u64) -> Result<Self::Ok> {
        if v > (i64::MAX as u64) {
            Err(GraphError::GraphSerde(format!(
                "value {} is greater than the max {}",
                v,
                i64::MAX,
            )))
        } else {
            self.serialize_i64(v as i64)
        }
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_f32(self, v: f32) -> Result<Self::Ok> {
        self.serialize_f64(v as f64)
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_f64(self, v: f64) -> Result<Self::Ok> {
        self.serialize_primitive(v.to_telemetry())
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_char(self, v: char) -> Result<Self::Ok> {
        self.serialize_primitive(v.to_string().to_telemetry())
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_str(self, v: &str) -> Result<Self::Ok> {
        self.serialize_primitive(v.to_string().to_telemetry())
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok> {
        use serde::ser::SerializeSeq;
        let mut seq = self.serialize_seq(Some(v.len()))?;
        for byte in v {
            seq.serialize_element(byte)?;
        }
        seq.end()
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_none(self) -> Result<Self::Ok> {
        self.serialize_unit()
    }

    #[tracing::instrument(level="trace", skip(value))]
    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(self)
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_unit(self) -> Result<Self::Ok> {
        self.serialize_primitive(TelemetryValue::from(TelemetryValue::Unit))
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok> {
        self.serialize_unit()
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_unit_variant(
        self, _name: &'static str, _variant_index: u32, variant: &'static str,
    ) -> Result<Self::Ok> {
        self.serialize_str(&variant)
    }

    #[tracing::instrument(level="trace", skip(value))]
    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<Self::Ok>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(self)
    }

    #[tracing::instrument(level="trace", skip(value))]
    fn serialize_newtype_variant<T>(
        self, _name: &'static str, _variant_index: u32, variant: &'static str, value: &T,
    ) -> Result<Self::Ok>
    where
        T: ?Sized + ser::Serialize,
    {
        self.push_key(&variant);
        value.serialize(&mut *self)?;
        self.pop_key();
        Ok(())
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(self)
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        self.serialize_seq(Some(len))
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_tuple_struct(self, _name: &'static str, len: usize) -> Result<Self::SerializeTupleStruct> {
        self.serialize_seq(Some(len))
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_tuple_variant(
        self, _name: &'static str, _variant_index: u32, variant: &'static str, _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        self.push_key(&variant);
        Ok(self)
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        Ok(self)
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_struct(self, _name: &'static str, len: usize) -> Result<Self::SerializeStruct> {
        self.serialize_map(Some(len))
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_struct_variant(
        self, _name: &'static str, _variant_index: u32, variant: &'static str, _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        self.push_key(&variant);
        Ok(self)
    }
}

impl<'a> ser::SerializeSeq for &'a mut TelemetrySerializer {
    type Ok = ();
    type Error = GraphError;

    #[tracing::instrument(level="trace", skip(value))]
    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        self.inc_last_key_index()?;
        value.serialize(&mut **self)?;
        Ok(())
    }

    #[tracing::instrument(level="trace", skip())]
    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

impl<'a> ser::SerializeTuple for &'a mut TelemetrySerializer {
    type Ok = ();
    type Error = GraphError;

    #[tracing::instrument(level="trace", skip(value))]
    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        self.inc_last_key_index()?;
        value.serialize(&mut **self)?;
        Ok(())
    }

    #[tracing::instrument(level="trace", skip())]
    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

impl<'a> ser::SerializeTupleStruct for &'a mut TelemetrySerializer {
    type Ok = ();
    type Error = GraphError;

    #[tracing::instrument(level="trace", skip(value))]
    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        self.inc_last_key_index()?;
        value.serialize(&mut **self)?;
        Ok(())
    }

    #[tracing::instrument(level="trace", skip())]
    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

impl<'a> ser::SerializeTupleVariant for &'a mut TelemetrySerializer {
    type Ok = ();
    type Error = GraphError;

    #[tracing::instrument(level="trace", skip(value))]
    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        self.inc_last_key_index()?;
        value.serialize(&mut **self)?;
        Ok(())
    }

    #[tracing::instrument(level="trace", skip())]
    fn end(self) -> Result<Self::Ok> {
        self.pop_key();
        Ok(())
    }
}

impl<'a> ser::SerializeMap for &'a mut TelemetrySerializer {
    type Ok = ();
    type Error = GraphError;

    #[tracing::instrument(level="trace", skip(key))]
    fn serialize_key<T>(&mut self, key: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        let key_serializer = StringKeySerializer;
        let key = key.serialize(key_serializer)?;
        self.push_key(&key);
        Ok(())
    }

    #[tracing::instrument(level="trace", skip(value))]
    fn serialize_value<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(&mut **self)?;
        self.pop_key();
        Ok(())
    }

    #[tracing::instrument(level="trace", skip())]
    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

impl<'a> ser::SerializeStruct for &'a mut TelemetrySerializer {
    type Ok = ();
    type Error = GraphError;

    #[tracing::instrument(level="trace", skip(value))]
    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        self.push_key(key);
        value.serialize(&mut **self)?;
        self.pop_key();
        Ok(())
    }

    #[tracing::instrument(level="trace", skip())]
    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

impl<'a> ser::SerializeStructVariant for &'a mut TelemetrySerializer {
    type Ok = ();
    type Error = GraphError;

    #[tracing::instrument(level="trace", skip(value))]
    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        self.push_key(key);
        value.serialize(&mut **self)?;
        self.pop_key();
        Ok(())
    }

    #[tracing::instrument(level="trace", skip())]
    fn end(self) -> Result<Self::Ok> {
        self.pop_key();
        Ok(())
    }
}

#[derive(Debug)]
pub struct StringKeySerializer;

impl ser::Serializer for StringKeySerializer {
    type Ok = String;
    type Error = GraphError;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    #[tracing::instrument(level="trace", skip())]
    fn serialize_bool(self, v: bool) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_i8(self, v: i8) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_i16(self, v: i16) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_i32(self, v: i32) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_i64(self, v: i64) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_u8(self, v: u8) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_u16(self, v: u16) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_u32(self, v: u32) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_u64(self, v: u64) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_f32(self, v: f32) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_f64(self, v: f64) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_char(self, v: char) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_str(self, v: &str) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok> {
        Ok(String::from_utf8_lossy(v).to_string())
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_none(self) -> Result<Self::Ok> {
        self.serialize_unit()
    }

    #[tracing::instrument(level="trace", skip(value))]
    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(self)
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_unit(self) -> Result<Self::Ok> {
        Ok(String::new())
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_unit_struct(self, _name: &str) -> Result<Self::Ok> {
        self.serialize_unit()
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_unit_variant(self, _name: &str, _variant_index: u32, variant: &str) -> Result<Self::Ok> {
        Ok(variant.to_string())
    }

    #[tracing::instrument(level="trace", skip(value))]
    fn serialize_newtype_struct<T>(self, _name: &str, value: &T) -> Result<Self::Ok>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(self)
    }

    #[tracing::instrument(level="trace", skip(value))]
    fn serialize_newtype_variant<T>(
        self, _name: &str, _variant_index: u32, _variant: &str, value: &T,
    ) -> Result<Self::Ok>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(self)
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        Err(GraphError::GraphSerde("seq can't serialize to string key".to_string()))
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        Err(GraphError::GraphSerde(
            "tuple can't serialize to string key".to_string(),
        ))
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_tuple_struct(self, name: &str, _len: usize) -> Result<Self::SerializeTupleStruct> {
        Err(GraphError::GraphSerde(format!(
            "tuple struct {} can't serialize to string key",
            name
        )))
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_tuple_variant(
        self, name: &str, _variant_index: u32, variant: &str, _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Err(GraphError::GraphSerde(format!(
            "tuple variant {}::{} can't serialize to string key",
            name, variant
        )))
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        Err(GraphError::GraphSerde("map can't serialize to string key".to_string()))
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_struct(self, name: &str, _len: usize) -> Result<Self::SerializeStruct> {
        Err(GraphError::GraphSerde(format!(
            "struct {} can't serialize to string key",
            name
        )))
    }

    #[tracing::instrument(level="trace", skip())]
    fn serialize_struct_variant(
        self, name: &str, _variant_index: u32, variant: &str, _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Err(GraphError::GraphSerde(format!(
            "struct variant {}::{} can't serialize to string key",
            name, variant
        )))
    }
}

impl ser::SerializeSeq for StringKeySerializer {
    type Ok = String;
    type Error = GraphError;

    #[tracing::instrument(level="trace", skip(self, _value))]
    fn serialize_element<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        unreachable!()
    }

    #[tracing::instrument(level="trace", skip(self))]
    fn end(self) -> Result<Self::Ok> {
        unreachable!()
    }
}

impl ser::SerializeTuple for StringKeySerializer {
    type Ok = String;
    type Error = GraphError;

    #[tracing::instrument(level="trace", skip(self, _value))]
    fn serialize_element<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        unreachable!()
    }

    #[tracing::instrument(level="trace", skip(self))]
    fn end(self) -> Result<Self::Ok> {
        unreachable!()
    }
}

impl ser::SerializeTupleStruct for StringKeySerializer {
    type Ok = String;
    type Error = GraphError;

    #[tracing::instrument(level="trace", skip(self, _value))]
    fn serialize_field<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        unreachable!()
    }

    #[tracing::instrument(level="trace", skip(self))]
    fn end(self) -> Result<Self::Ok> {
        unreachable!()
    }
}

impl ser::SerializeTupleVariant for StringKeySerializer {
    type Ok = String;
    type Error = GraphError;

    #[tracing::instrument(level="trace", skip(self, _value))]
    fn serialize_field<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        unreachable!()
    }

    #[tracing::instrument(level="trace", skip(self))]
    fn end(self) -> Result<Self::Ok> {
        unreachable!()
    }
}

impl ser::SerializeMap for StringKeySerializer {
    type Ok = String;
    type Error = GraphError;

    #[tracing::instrument(level="trace", skip(self, _key))]
    fn serialize_key<T>(&mut self, _key: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        unreachable!()
    }

    #[tracing::instrument(level="trace", skip(self, _value))]
    fn serialize_value<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        unreachable!()
    }

    #[tracing::instrument(level="trace", skip(self))]
    fn end(self) -> Result<Self::Ok> {
        unreachable!()
    }
}

impl ser::SerializeStruct for StringKeySerializer {
    type Ok = String;
    type Error = GraphError;

    #[tracing::instrument(level="trace", skip(_value))]
    fn serialize_field<T>(&mut self, _key: &'static str, _value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        unreachable!()
    }

    #[tracing::instrument(level="trace", skip())]
    fn end(self) -> Result<Self::Ok> {
        unreachable!()
    }
}

impl ser::SerializeStructVariant for StringKeySerializer {
    type Ok = String;
    type Error = GraphError;

    #[tracing::instrument(level="trace", skip(_value))]
    fn serialize_field<T>(&mut self, _key: &'static str, _value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        unreachable!()
    }

    #[tracing::instrument(level="trace", skip(self))]
    fn end(self) -> Result<Self::Ok> {
        unreachable!()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde::{Serialize, Deserialize};

    #[test]
    fn test_telemetry_struct_serde() {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Test {
            int: u32,
            seq: Vec<String>,
        }

        let test = Test {
            int: 1,
            seq: vec!["a".to_string(), "b".to_string()],
        };
        let telemetry = Telemetry::try_from(&test).unwrap();
        tracing::info!(?telemetry, "after converting Test into telemetry.");

        let actual: Test = telemetry.try_into().unwrap();
        assert_eq!(test, actual);
    }
}
