use chrono::{DateTime, TimeZone, Utc};
use serde::{self, de, Serializer};
use std::fmt;

const FORMAT: &'static str = "%+";

pub fn serialize_optional_datetime<S>(date: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let s = date.map(|d| format!("{}", d.format(FORMAT)));
    match s {
        None => serializer.serialize_none(),
        Some(s) => serializer.serialize_some(&s),
    }
}

pub fn deserialize_optional_datetime<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: de::Deserializer<'de>,
{
    deserializer.deserialize_option(OptionalDateTimeFromIso8601Rfc339FormatVisitor)
}

pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let s = format!("{}", date.format(FORMAT));
    serializer.serialize_str(&s)
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: de::Deserializer<'de>,
{
    deserializer.deserialize_str(DateTimeFromIso8601Rfc3339FormatVisitor)
}

struct DateTimeFromIso8601Rfc3339FormatVisitor;

impl<'de> de::Visitor<'de> for DateTimeFromIso8601Rfc3339FormatVisitor {
    type Value = DateTime<Utc>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "a datetime string")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Utc.datetime_from_str(value, FORMAT).map_err(serde::de::Error::custom)
    }
}

struct OptionalDateTimeFromIso8601Rfc339FormatVisitor;

impl<'de> de::Visitor<'de> for OptionalDateTimeFromIso8601Rfc339FormatVisitor {
    type Value = Option<DateTime<Utc>>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "null or a datetime string")
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(None)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        Ok(Some(
            deserializer.deserialize_str(DateTimeFromIso8601Rfc3339FormatVisitor)?,
        ))
    }
}

// mod date_format {
//     use chrono::{DateTime, Utc, TimeZone};
//     use serde::{self, Deserialize, Serializer, Deserializer};
//
//     const FORMAT: &'static str = "%+";
//
//     pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//     {
//         let s = format!("{}", date.format(FORMAT));
//         serializer.serialize_str(&s)
//     }
//
//     pub fn deserialize<'de, D>(deserializer: D,) -> Result<DateTime<Utc>, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         let s = String::deserialize(deserializer)?;
//         Utc.datetime_from_str(&s, FORMAT).map_err(serde::de::Error::custom)
//     }
//
//     pub mod optional {
//         use chrono::{DateTime, Utc, TimeZone};
//         use serde::{self, Deserialize, Deserializer, Serializer};
//
//         pub fn serialize<S>(date: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
//         where
//             S: Serializer,
//         {
//             let s = date.map(|d| format!("{}", d.format(super::FORMAT)));
//             match s {
//                 None => serializer.serialize_none(),
//                 Some(s) => serializer.serialize_some(&s),
//             }
//         }
//
//         pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
//         where
//             D: Deserializer<'de>,
//         {
//             let date = Option::<String>::deserialize(deserializer)?;
//             let foo= date
//                 .map(|d| {
//                     Utc.datetime_from_str(&d, super::FORMAT).map_err(serde::de::Error::custom)
//                 });
//
//             match foo {
//                 Some(date) => date.map(|d| Some(d)),
//                 None => Ok(None),
//             }
//         }
//     }
// }
//
