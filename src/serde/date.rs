use std::collections::HashMap;
use std::fmt;

use chrono::{DateTime, TimeZone, Utc};
use serde::de::MapAccess;
use serde::{self, de, ser::SerializeMap, Serializer};

pub const FORMAT: &str = "%+";
pub const SECS_KEY: &str = crate::elements::SECS_KEY;
pub const NANOS_KEY: &str = crate::elements::NANOS_KEY;

#[tracing::instrument(level = "debug")]
fn table_from_datetime(datetime: &DateTime<Utc>) -> HashMap<String, i64> {
    maplit::hashmap! {
        SECS_KEY.to_string() => datetime.timestamp(),
        NANOS_KEY.to_string() => datetime.timestamp_subsec_nanos() as i64,
    }
}

#[tracing::instrument(level = "debug")]
fn datetime_from_table(datetime: HashMap<String, i64>) -> DateTime<Utc> {
    Utc.timestamp(
        datetime.get(SECS_KEY).copied().unwrap_or(0),
        datetime.get(NANOS_KEY).copied().unwrap_or(0) as u32,
    )
}

#[tracing::instrument(level = "debug", skip(serializer))]
pub fn serialize_optional_datetime_map<S>(date: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let s = date.as_ref().map(table_from_datetime);

    match s {
        None => serializer.serialize_none(),
        Some(s) => serializer.serialize_some(&s),
    }
}

#[tracing::instrument(level = "debug", skip(serializer))]
pub fn serialize_optional_datetime_format<S>(date: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let s = date.map(|d| format!("{}", d.format(FORMAT)));

    match s {
        None => serializer.serialize_none(),
        Some(s) => serializer.serialize_some(&s),
    }
}

#[tracing::instrument(level = "debug", skip(deserializer))]
pub fn deserialize_optional_datetime<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: de::Deserializer<'de>,
{
    deserializer.deserialize_option(OptionalDateTimeMapVisitor)
}

// pub fn deserialize_optional_datetime_format<'de, D>(deserializer: D) ->
// Result<Option<DateTime<Utc>>, D::Error> where
//     D: de::Deserializer<'de>,
// {
//     deserializer.deserialize_option(OptionalDateTimeFormatVisitor)
// }

#[tracing::instrument(level = "debug", skip(serializer))]
pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    // todo changing DateTime<Utc> serde to match From<TelemetryValue>
    // let datetime_table = format!("{}", date.format(FORMAT));
    let datetime_table = table_from_datetime(date);
    let mut map = serializer.serialize_map(Some(datetime_table.len()))?;
    for (k, v) in datetime_table {
        map.serialize_entry(&k, &v)?;
    }
    map.end()
}

#[tracing::instrument(level = "debug", skip(serializer))]
pub fn serialize_format<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let datetime_rep = format!("{}", date.format(FORMAT));
    serializer.serialize_str(datetime_rep.as_str())
}

#[tracing::instrument(level = "debug", skip(deserializer))]
pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: de::Deserializer<'de>,
{
    // deserializer.deserialize_str(DateTimeFromIso8601Rfc3339FormatVisitor)
    deserializer.deserialize_any(DateTimeVisitor)
}

// pub fn deserialize_format<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
// where
//     D: de::Deserializer<'de>,
// {
//     deserializer.deserialize_str(DateTimeFromIso8601Rfc3339FormatVisitor)
// }

struct DateTimeVisitor;

impl<'de> de::Visitor<'de> for DateTimeVisitor {
    type Value = DateTime<Utc>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "a datetime table")
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Utc.datetime_from_str(value, FORMAT).map_err(serde::de::Error::custom)
    }

    #[tracing::instrument(level = "debug", skip(self, access))]
    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut table = access.size_hint().map(HashMap::with_capacity).unwrap_or_default();

        while let Some((k, v)) = access.next_entry()? {
            table.insert(k, v);
        }

        let datetime = datetime_from_table(table);
        Ok(datetime)
    }
}

// struct DateTimeFromIso8601Rfc3339FormatVisitor;
//
// impl<'de> de::Visitor<'de> for DateTimeFromIso8601Rfc3339FormatVisitor {
//     type Value = DateTime<Utc>;
//
//     fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "a datetime string")
//     }
//
//     fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
//     where
//         E: de::Error,
//     {
//         Utc.datetime_from_str(value, FORMAT).map_err(serde::de::Error::custom)
//     }
// }

struct OptionalDateTimeMapVisitor;

impl<'de> de::Visitor<'de> for OptionalDateTimeMapVisitor {
    type Value = Option<DateTime<Utc>>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "null or a datetime serialized value")
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(None)
    }

    #[tracing::instrument(level = "debug", skip(self, deserializer))]
    fn visit_some<D>(self, deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        Ok(Some(deserializer.deserialize_any(DateTimeVisitor)?))
    }
}

// struct OptionalDateTimeFormatVisitor;
//
// impl<'de> de::Visitor<'de> for OptionalDateTimeFormatVisitor {
//     type Value = Option<DateTime<Utc>>;
//
//     fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "null or a datetime serialized value")
//     }
//
//     fn visit_none<E>(self) -> Result<Self::Value, E>
//     where
//         E: de::Error,
//     {
//         Ok(None)
//     }
//
//     fn visit_some<D>(self, deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
//     where
//         D: de::Deserializer<'de>,
//     {
//         Ok(Some(
//             deserializer.deserialize_any(DateTimeVisitor)?
//             // deserializer.deserialize_str(DateTimeFromIso8601Rfc3339FormatVisitor)?,
//             // deserializer.deserialize_map(DateTimeFromMapVisitor)?,
//         ))
//     }
// }

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
//         pub fn serialize<S>(date: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok,
// S::Error>         where
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
