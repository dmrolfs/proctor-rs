use serde::{de, Deserialize, Deserializer, Serializer};
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

pub use date_serde::*;

mod date_serde;

pub fn serialize_to_str<T, S>(that: T, serializer: S) -> Result<S::Ok, S::Error>
where
    T: AsRef<str>,
    S: Serializer,
{
    serializer.serialize_str(that.as_ref())
}

pub fn deserialize_from_str<'de, S, D>(deserializer: D) -> Result<S, D::Error>
where
    S: FromStr,
    S::Err: fmt::Display,
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    S::from_str(&s).map_err(de::Error::custom)
}

pub fn serialize_duration_secs<S: Serializer>(that: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_u64(that.as_secs())
}

pub fn deserialize_duration_secs<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    let secs: u64 = Deserialize::deserialize(deserializer)?;
    Ok(Duration::from_secs(secs))
}
