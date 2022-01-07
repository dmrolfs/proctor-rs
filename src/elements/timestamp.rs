use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::{self, Debug};
use std::str::FromStr;
use std::time::Duration;

use approx::{AbsDiffEq, RelativeEq};
use chrono::{DateTime, TimeZone, Utc};
use num_traits::cast::FromPrimitive;
use once_cell::sync::Lazy;
use oso::PolarClass;
use regex::Regex;
use serde::de::Unexpected;
use serde::{de, Deserialize, Deserializer, Serialize};

use super::{TelemetryType, TelemetryValue, ToTelemetry};
use crate::error::TelemetryError;

#[derive(PolarClass, Debug, Copy, Clone, Default, PartialEq, PartialOrd, Serialize)]
pub struct Timestamp(i64, u32);

impl Timestamp {
    const MILLIS_PER_SEC: i64 = 1_000;
    const NANOS_PER_MILLI: i64 = 1_000_000;
    const NANOS_PER_SEC: i64 = Self::NANOS_PER_MILLI * Self::MILLIS_PER_SEC;
    pub const ZERO: Timestamp = Timestamp(0, 0);

    pub fn now() -> Self {
        Self::from_datetime(&Utc::now())
    }

    pub fn from_datetime(datetime: &DateTime<Utc>) -> Self {
        Self::new(datetime.timestamp(), datetime.timestamp_subsec_nanos())
    }

    pub fn from_secs(secs: i64) -> Self {
        Self(secs, 0)
    }

    pub fn from_milliseconds(millis: i64) -> Self {
        let secs: i64 = millis / Self::MILLIS_PER_SEC;
        let frac_millis: u32 = (millis % Self::MILLIS_PER_SEC) as u32;
        let subsec_nanos: u32 = frac_millis * (Self::NANOS_PER_MILLI as u32);
        Self::new(secs, subsec_nanos)
    }

    pub fn new(secs: i64, subsec_nanos: u32) -> Self {
        if (f64::MAX as i64) < secs {
            panic!(
                "maximum timestamp seconds support is {} - cannot create with {} secs",
                f64::MAX as i64,
                secs
            );
        }

        Self(secs, subsec_nanos)
    }

    pub fn as_f64(&self) -> f64 {
        (self.0 as f64) + ((self.1 as f64) / (Self::NANOS_PER_SEC as f64))
    }

    pub fn as_secs(&self) -> i64 {
        self.0
    }

    pub fn as_millis(&self) -> i64 {
        let sec_millis = self.0 * Self::MILLIS_PER_SEC;
        let nsec_millis = (self.1 as i64) / Self::NANOS_PER_MILLI;
        sec_millis + nsec_millis
    }

    pub fn as_nanos(&self) -> i64 {
        self.0 * Self::NANOS_PER_SEC + self.1 as i64
    }

    pub fn as_utc(&self) -> DateTime<Utc> {
        Utc.timestamp(self.0, self.1)
    }

    pub fn as_pair(&self) -> (i64, u32) {
        (self.0, self.1)
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            write!(f, "({},{})", self.0, self.1)
        } else {
            write!(f, "{}", self.as_utc())
        }
    }
}

pub const FORMAT: &str = "%+";
pub const SECS_KEY: &str = "secs";
pub const NANOS_KEY: &str = "nanos";

static TUPLE_FORM: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^\((\d+),(\d+)\)$").expect("failed to create tuple timestamp regex"));

impl FromStr for Timestamp {
    type Err = TelemetryError;

    fn from_str(ts_rep: &str) -> Result<Self, Self::Err> {
        if let Some(cap) = TUPLE_FORM.captures(ts_rep) {
            let secs = i64::from_str(&cap[1]).map_err(|err| TelemetryError::ValueParseError(err.into()))?;
            let nanos = u32::from_str(&cap[2]).map_err(|err| TelemetryError::ValueParseError(err.into()))?;
            return Ok(Self(secs, nanos));
        }

        let dt = DateTime::parse_from_str(ts_rep, FORMAT)
            .map_err(|err| TelemetryError::ValueParseError(err.into()))?
            .with_timezone(&Utc);
        Ok(dt.into())
    }
}

impl TryFrom<TelemetryValue> for Timestamp {
    type Error = TelemetryError;

    fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
        match telemetry {
            TelemetryValue::Seq(mut seq) if seq.len() == 2 => {
                let nanos = seq.pop().map(u32::try_from).transpose()?.unwrap();
                let secs = seq.pop().map(i64::try_from).transpose()?.unwrap();
                Ok(Self::new(secs, nanos))
            }
            TelemetryValue::Float(f64) => Ok(f64.into()),
            TelemetryValue::Integer(i64) => Ok(i64.into()),
            TelemetryValue::Table(mut table) => {
                let secs = table.remove(SECS_KEY).map(i64::try_from).transpose()?.unwrap_or(0);
                let nanos = table.remove(NANOS_KEY).map(u32::try_from).transpose()?.unwrap_or(0);
                Ok(Self(secs, nanos))
            }
            TelemetryValue::Text(rep) => {
                let dt = DateTime::parse_from_str(rep.as_str(), FORMAT)
                    .map_err(|err| TelemetryError::ValueParseError(err.into()))?
                    .with_timezone(&Utc);
                Ok(dt.into())
            }
            value => Err(TelemetryError::TypeError {
                expected: TelemetryType::Float,
                actual: Some(format!("{:?}", value)),
            }),
        }
    }
}

// impl AsRef<f64> for Timestamp {
//     fn as_ref(&self) -> &f64 {
//         &self.as_f64()
//     }
// }

impl From<Timestamp> for DateTime<Utc> {
    fn from(ts: Timestamp) -> Self {
        Utc.timestamp(ts.0, ts.1)
    }
}

impl From<DateTime<Utc>> for Timestamp {
    fn from(that: DateTime<Utc>) -> Self {
        Self(that.timestamp(), that.timestamp_subsec_nanos())
    }
}

impl From<Timestamp> for f64 {
    fn from(ts: Timestamp) -> Self {
        ts.as_f64()
    }
}

impl From<f64> for Timestamp {
    fn from(timestamp_secs: f64) -> Self {
        let secs = timestamp_secs.floor() as i64;
        let nanos = (timestamp_secs.fract() * 10_f64.powi(9)) as u32;
        Self(secs, nanos)
    }
}

impl From<i64> for Timestamp {
    fn from(timestamp_secs: i64) -> Self {
        Self(timestamp_secs, 0)
    }
}

impl From<&Timestamp> for f64 {
    fn from(ts: &Timestamp) -> Self {
        ts.as_f64()
    }
}

impl From<Timestamp> for i64 {
    fn from(ts: Timestamp) -> Self {
        ts.0
    }
}

impl From<&Timestamp> for i64 {
    fn from(ts: &Timestamp) -> Self {
        ts.0
    }
}

impl From<Timestamp> for TelemetryValue {
    fn from(ts: Timestamp) -> Self {
        TelemetryValue::Table(
            maplit::hashmap! {
                SECS_KEY.to_string() => ts.0.to_telemetry(),
                NANOS_KEY.to_string() => ts.1.to_telemetry(),
            }
            .into(),
        )
    }
}

impl std::ops::Add<Duration> for Timestamp {
    type Output = Timestamp;

    fn add(self, rhs: Duration) -> Self::Output {
        let total = self.as_f64() + rhs.as_secs_f64();
        total.into()
    }
}

// impl std::ops::Add<chrono::Duration> for Timestamp {
//     type Output = Timestamp;
//
//     fn add(self, rhs: chrono::Duration) -> Self::Output {
//         let rhs_secs = rhs.num_seconds();
//         let rhs_nanos = if let Some(nanos) = rhs.num_nanoseconds() {
//             let n = nanos - rhs_secs * 10.powi(9);
//         }
//
//
//
//
//         // let total = if let Some(nanos) = rhs.num_nanoseconds() {
//         //     self.as_f64() + (nanos as f64) / 10.powi(9)
//         // } else if let Some(micros) = rhs.num_microseconds() {
//         //     let r = micros * 10.powi(3);
//         //     self.as_f64() + (micros as f64) * 10.powi(3);
//         // }
//         // Self(self.0 + rhs.num_seconds(), self.1 + rhs.nan)
//         // let nanos = rhs.num_nanoseconds()
//         // let millis = rhs.num_milliseconds() as f64;
//         // let secs = millis / 1_000.;
//         // Self(self.0 + secs)
//     }
// }

impl std::ops::Sub<Duration> for Timestamp {
    type Output = Timestamp;

    fn sub(self, rhs: Duration) -> Self::Output {
        (self.as_f64() - rhs.as_secs_f64()).into()
    }
}

// impl std::ops::Sub<chrono::Duration> for Timestamp {
//     type Output = Timestamp;
//
//     fn sub(self, rhs: chrono::Duration) -> Self::Output {
//         let millis = rhs.num_milliseconds() as f64;
//         let secs = millis / 1_000.;
//         Self(self.0 - secs)
//     }
// }

impl std::ops::Add<Duration> for &Timestamp {
    type Output = Timestamp;

    fn add(self, rhs: Duration) -> Self::Output {
        (self.as_f64() + rhs.as_secs_f64()).into()
    }
}

// impl std::ops::Add<chrono::Duration> for &Timestamp {
//     type Output = Timestamp;
//
//     fn add(self, rhs: chrono::Duration) -> Self::Output {
//         let millis = rhs.num_milliseconds() as f64;
//         let secs = millis / 1_000.;
//         Timestamp(self.0 + secs)
//     }
// }

impl std::ops::Sub<Duration> for &Timestamp {
    type Output = Timestamp;

    fn sub(self, rhs: Duration) -> Self::Output {
        (self.as_f64() - rhs.as_secs_f64()).into()
    }
}

// impl std::ops::Sub<chrono::Duration> for &Timestamp {
//     type Output = Timestamp;
//
//     fn sub(self, rhs: chrono::Duration) -> Self::Output {
//         let millis = rhs.num_milliseconds() as f64;
//         let secs = millis / 1_000.;
//         Timestamp(self.0 - secs)
//     }
// }

impl AbsDiffEq for Timestamp {
    type Epsilon = f64;

    fn default_epsilon() -> Self::Epsilon {
        f64::default_epsilon()
    }

    fn abs_diff_eq(&self, other: &Self, epsilon: Self::Epsilon) -> bool {
        f64::abs_diff_eq(&self.as_f64(), &other.as_f64(), epsilon)
    }
}

impl RelativeEq for Timestamp {
    fn default_max_relative() -> Self::Epsilon {
        f64::default_max_relative()
    }

    fn relative_eq(&self, other: &Self, epsilon: Self::Epsilon, max_relative: Self::Epsilon) -> bool {
        f64::relative_eq(&self.as_f64(), &other.as_f64(), epsilon, max_relative)
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    #[tracing::instrument(level = "debug", skip(deserializer))]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(TimestampVisitor)
    }
}

struct TimestampVisitor;

impl<'de> de::Visitor<'de> for TimestampVisitor {
    type Value = Timestamp;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a timestamp value in integer, float, sequence, map or string form")
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn visit_i8<E>(self, v: i8) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_i64(v as i64)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn visit_i16<E>(self, v: i16) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_i64(v as i64)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_i64(v as i64)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v.into())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_i64(v as i64)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_i64(v as i64)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_i64(v as i64)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match i64::from_u64(v) {
            Some(val) => self.visit_i64(val),
            None => Err(de::Error::invalid_value(Unexpected::Unsigned(v), &self)),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_f64(v as f64)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v.into())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let dt = DateTime::parse_from_str(v, FORMAT)
            .map_err(|err| {
                tracing::error!(error=?err, "failed to parse string {} for format {}", v, FORMAT);
                de::Error::invalid_value(Unexpected::Str(v), &self)
            })?
            .with_timezone(&Utc);

        Ok(Timestamp::from_datetime(&dt))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(v)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(v.as_str())
    }

    #[tracing::instrument(level = "debug", skip(self, access))]
    fn visit_seq<S>(self, mut access: S) -> Result<Self::Value, S::Error>
    where
        S: de::SeqAccess<'de>,
    {
        let secs = access.next_element()?.unwrap_or(0);
        let nanos = access.next_element()?.unwrap_or(0);
        Ok(Timestamp::new(secs, nanos))
    }

    #[tracing::instrument(level = "debug", skip(self, access))]
    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: de::MapAccess<'de>,
    {
        let mut map: HashMap<String, i64> = HashMap::with_capacity(access.size_hint().unwrap_or(0));
        while let Some((key, value)) = access.next_entry()? {
            map.insert(key, value);
        }

        let secs = map.get(SECS_KEY).copied().unwrap_or(0);
        let nanos = map.get(NANOS_KEY).copied().unwrap_or(0) as u32;
        Ok(Timestamp::new(secs, nanos))
    }
}

#[cfg(test)]
mod tests {
    use claim::*;
    use pretty_assertions::assert_eq;

    use super::*;
    //     use approx::assert_relative_eq;

    #[test]
    fn test_from_pair_string() {
        let ts = Timestamp::now();
        let ts_rep = format!("{:#}", ts);
        let actual = assert_ok!(Timestamp::from_str(&ts_rep));
        assert_eq!(actual, ts);
    }

    #[test]
    fn test_timestamp_from_millis() {
        let millis: i64 = 1_638_989_054_310;
        let secs = millis / 1_000;
        assert_eq!(secs, 1_638_989_054);
        let remaining_millis = millis as u32 - (secs * 1_000) as u32;
        assert_eq!(remaining_millis, 310);

        let actual = Timestamp::from_milliseconds(millis);
        assert_eq!(actual, Timestamp::new(1_638_989_054, 310_000_000));
    }
}
