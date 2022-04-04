use std::cmp::Ordering;
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
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use super::{TelemetryType, TelemetryValue, ToTelemetry};
use crate::error::TelemetryError;

#[derive(Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Interval(Timestamp, Timestamp);

impl Interval {
    pub const fn new(start: Timestamp, end: Timestamp) -> Self {
        Self(start, end)
    }
}

impl From<Interval> for (Timestamp, Timestamp) {
    fn from(value: Interval) -> Self {
        (value.0, value.1)
    }
}

impl From<(Timestamp, Timestamp)> for Interval {
    fn from(value: (Timestamp, Timestamp)) -> Self {
        Self::new(value.0, value.1)
    }
}

impl fmt::Debug for Interval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{} - {}]", self.0, self.1)
    }
}

impl Interval {
    pub fn duration(&self) -> Duration {
        self.1 - self.0
    }

    pub fn contains_timestamp(&self, timestamp: Timestamp) -> bool {
        self.0 <= timestamp && timestamp <= self.1
    }

    pub fn contains(&self, other: Self) -> bool {
        self.0 <= other.0 && other.0 <= self.1 && self.0 <= other.1 && other.1 <= self.1
    }

    pub fn abuts(&self, other: Self) -> bool {
        self.0 == other.1 || self.1 == other.0
    }

    pub fn overlap(&self, other: Self) -> Option<Self> {
        let start = if other.0 < self.0 { self.0 } else { other.0 };
        let end = if self.1 < other.1 { self.1 } else { other.1 };

        if start < end {
            Some(Self(start, end))
        } else {
            None
        }
    }

    pub fn gap(&self, other: Self) -> Option<Self> {
        if self.abuts(other) || self.overlap(other).is_some() {
            None
        } else if self.is_before(other) {
            Some(Self(self.1, other.0))
        } else {
            Some(Self(other.1, self.0))
        }
    }

    /// Intervals are inclusive of the start and exclusive
    pub fn is_before(&self, other: Self) -> bool {
        self.1 < other.0
    }

    /// Intervals are inclusive of the start and exclusive
    pub fn is_after(&self, other: Self) -> bool {
        other.1 < self.0
    }
}

#[derive(PolarClass, Debug, Copy, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Timestamp(i64, u32);

impl Timestamp {
    const MILLIS_PER_SEC: i64 = 1_000;
    const NANOS_PER_MILLI: i64 = 1_000_000;
    const NANOS_PER_SEC: i64 = Self::NANOS_PER_MILLI * Self::MILLIS_PER_SEC;
    pub const ZERO: Self = Self(0, 0);

    pub fn now() -> Self {
        Self::from_datetime(&Utc::now())
    }

    pub fn from_datetime(datetime: &DateTime<Utc>) -> Self {
        Self::new(datetime.timestamp(), datetime.timestamp_subsec_nanos())
    }

    pub const fn from_secs(secs: i64) -> Self {
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

    /// Returns seconds timestamp (including partial) as a floating point.
    pub fn as_f64(&self) -> f64 {
        (self.0 as f64) + ((self.1 as f64) / (Self::NANOS_PER_SEC as f64))
    }

    pub const fn as_secs(&self) -> i64 {
        self.0
    }

    pub const fn as_millis(&self) -> i64 {
        let sec_millis = self.0 * Self::MILLIS_PER_SEC;
        let nsec_millis = (self.1 as i64) / Self::NANOS_PER_MILLI;
        sec_millis + nsec_millis
    }

    pub const fn as_nanos(&self) -> i64 {
        self.0 * Self::NANOS_PER_SEC + self.1 as i64
    }

    pub fn as_utc(&self) -> DateTime<Utc> {
        Utc.timestamp(self.0, self.1)
    }

    pub const fn as_pair(&self) -> (i64, u32) {
        (self.0, self.1)
    }
}

impl Timestamp {
    pub fn serialize_as_secs_i64<S: Serializer>(ts: &Self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i64(ts.as_secs())
    }

    pub fn serialize_as_secs_f64<S: Serializer>(ts: &Self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_f64(ts.as_f64())
    }

    pub fn serialize_as_millis<S: Serializer>(ts: &Self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i64(ts.as_millis())
    }

    pub fn serialize_as_nanos<S: Serializer>(ts: &Self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i64(ts.as_nanos())
    }

    pub fn deserialize_secs_i64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let secs: i64 = Deserialize::deserialize(deserializer)?;
        Ok(Self::from_secs(secs))
    }

    pub fn deserialize_millis_i64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let millis: i64 = Deserialize::deserialize(deserializer)?;
        Ok(Self::from_milliseconds(millis))
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

impl PartialOrd for Timestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Timestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        let secs_cmp = self.0.cmp(&other.0);
        if secs_cmp == Ordering::Equal {
            self.1.cmp(&other.1)
        } else {
            secs_cmp
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
            let secs = i64::from_str(&cap[1]).map_err(|err| TelemetryError::ValueParse(err.into()))?;
            let nanos = u32::from_str(&cap[2]).map_err(|err| TelemetryError::ValueParse(err.into()))?;
            return Ok(Self(secs, nanos));
        }

        let dt = DateTime::parse_from_str(ts_rep, FORMAT)
            .map_err(|err| TelemetryError::ValueParse(err.into()))?
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
            },
            TelemetryValue::Float(f64) => Ok(f64.into()),
            TelemetryValue::Integer(i64) => Ok(i64.into()),
            TelemetryValue::Table(mut table) => {
                let secs = table.remove(SECS_KEY).map(i64::try_from).transpose()?.unwrap_or(0);
                let nanos = table.remove(NANOS_KEY).map(u32::try_from).transpose()?.unwrap_or(0);
                Ok(Self(secs, nanos))
            },
            TelemetryValue::Text(rep) => {
                let dt = DateTime::parse_from_str(rep.as_str(), FORMAT)
                    .map_err(|err| TelemetryError::ValueParse(err.into()))?
                    .with_timezone(&Utc);
                Ok(dt.into())
            },
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
        Self::Table(
            maplit::hashmap! {
                SECS_KEY.to_string() => ts.0.to_telemetry(),
                NANOS_KEY.to_string() => ts.1.to_telemetry(),
            }
            .into(),
        )
    }
}

impl std::ops::Add<Duration> for Timestamp {
    type Output = Self;

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
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self::Output {
        (self.as_f64() - rhs.as_secs_f64()).into()
    }
}

impl std::ops::Sub<Self> for Timestamp {
    type Output = Duration;

    fn sub(self, rhs: Self) -> Self::Output {
        let secs = self.as_f64() - rhs.as_f64();
        Duration::from_secs_f64(secs)
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
    #[tracing::instrument(level = "trace", skip(deserializer))]
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

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a timestamp value in integer, float, sequence, map or string form")
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn visit_i8<E>(self, v: i8) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_i64(v as i64)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn visit_i16<E>(self, v: i16) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_i64(v as i64)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_i64(v as i64)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v.into())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_i64(v as i64)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_i64(v as i64)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_i64(v as i64)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match i64::from_u64(v) {
            Some(val) => self.visit_i64(val),
            None => Err(de::Error::invalid_value(Unexpected::Unsigned(v), &self)),
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_f64(v as f64)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v.into())
    }

    #[tracing::instrument(level = "trace", skip(self))]
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

    #[tracing::instrument(level = "trace", skip(self))]
    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(v)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(v.as_str())
    }

    #[tracing::instrument(level = "trace", skip(self, access))]
    fn visit_seq<S>(self, mut access: S) -> Result<Self::Value, S::Error>
    where
        S: de::SeqAccess<'de>,
    {
        let secs = access.next_element()?.unwrap_or(0);
        let nanos = access.next_element()?.unwrap_or(0);
        Ok(Timestamp::new(secs, nanos))
    }

    #[tracing::instrument(level = "trace", skip(self, access))]
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
    use approx::assert_relative_eq;

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

    #[test]
    fn test_timestamp_difference() {
        let ts1 = Timestamp::new(1_638_989_054, 310_000_000);
        let ts2 = Timestamp::new(1_638_389_054, 0);
        let actual = (ts1 - ts2).as_secs_f64();
        assert_relative_eq!(actual, 600_000.31, epsilon = 0.001);
    }

    #[test]
    fn test_interval_contains() {
        let interval: Interval = (Timestamp::new(10, 33), Timestamp::new(15, 0)).into();
        assert!(interval.contains_timestamp(Timestamp::new(12, 0)));
        assert!(interval.contains_timestamp(Timestamp::new(10, 33)));
        assert!(interval.contains_timestamp(Timestamp::new(15, 0)));
        assert_eq!(interval.contains_timestamp(Timestamp::new(10, 0)), false);
        assert_eq!(interval.contains_timestamp(Timestamp::new(100, 0)), false);
    }

    #[allow(non_snake_case)]
    #[test]
    fn test_interval_abuts() {
        let i_9__10: Interval = Interval::new(Timestamp::new(9, 0), Timestamp::new(10, 0)).into();
        let i_9__9_01: Interval = Interval::new(Timestamp::new(9, 0), Timestamp::new(9, 1)).into();
        let i_9__9: Interval = Interval::new(Timestamp::new(9, 0), Timestamp::new(9, 0)).into();
        let i_8__8_30: Interval = Interval::new(Timestamp::new(8, 0), Timestamp::new(8, 30)).into();
        let i_8__9: Interval = Interval::new(Timestamp::new(8, 0), Timestamp::new(9, 0)).into();
        let i_8__9_01: Interval = Interval::new(Timestamp::new(8, 0), Timestamp::new(9, 1)).into();
        let i_10__10: Interval = Interval::new(Timestamp::new(10, 0), Timestamp::new(10, 0)).into();
        let i_10__10_30: Interval = Interval::new(Timestamp::new(10, 0), Timestamp::new(10, 30)).into();
        let i_10_30__11: Interval = Interval::new(Timestamp::new(10, 30), Timestamp::new(11, 0)).into();
        let i_14__14: Interval = Interval::new(Timestamp::new(14, 0), Timestamp::new(14, 0)).into();
        let i_14__15: Interval = Interval::new(Timestamp::new(14, 0), Timestamp::new(15, 0)).into();
        let i_13__14: Interval = Interval::new(Timestamp::new(13, 0), Timestamp::new(14, 0)).into();

        assert_eq!(i_9__10.abuts(i_8__8_30), false); // completely before
        assert_eq!(i_9__10.abuts(i_8__9), true);
        assert_eq!(i_9__10.abuts(i_8__9_01), false); // overlaps

        assert_eq!(i_9__10.abuts(i_9__9), true);
        assert_eq!(i_9__10.abuts(i_9__9_01), false); // overlaps

        assert_eq!(i_9__10.abuts(i_10__10), true);
        assert_eq!(i_9__10.abuts(i_10__10_30), true);

        assert_eq!(i_9__10.abuts(i_10_30__11), false); // completely after

        assert_eq!(i_14__14.abuts(i_14__14), true);
        assert_eq!(i_14__14.abuts(i_14__15), true);
        assert_eq!(i_14__14.abuts(i_13__14), true);
    }

    #[allow(non_snake_case)]
    #[test]
    fn test_interval_overlap() {
        let i_9__10: Interval = Interval::new(Timestamp::new(9, 0), Timestamp::new(10, 0)).into();
        let i_9__9_01: Interval = Interval::new(Timestamp::new(9, 0), Timestamp::new(9, 1)).into();
        let i_9__9: Interval = Interval::new(Timestamp::new(9, 0), Timestamp::new(9, 0)).into();
        let i_8__8_30: Interval = Interval::new(Timestamp::new(8, 0), Timestamp::new(8, 30)).into();
        let i_8__9: Interval = Interval::new(Timestamp::new(8, 0), Timestamp::new(9, 0)).into();
        let i_8__9_01: Interval = Interval::new(Timestamp::new(8, 0), Timestamp::new(9, 1)).into();
        let i_10__10: Interval = Interval::new(Timestamp::new(10, 0), Timestamp::new(10, 0)).into();
        let i_10__10_30: Interval = Interval::new(Timestamp::new(10, 0), Timestamp::new(10, 30)).into();
        let i_10_30__11: Interval = Interval::new(Timestamp::new(10, 30), Timestamp::new(11, 0)).into();
        let i_14__14: Interval = Interval::new(Timestamp::new(14, 0), Timestamp::new(14, 0)).into();
        let i_14__15: Interval = Interval::new(Timestamp::new(14, 0), Timestamp::new(15, 0)).into();
        let i_13__14: Interval = Interval::new(Timestamp::new(13, 0), Timestamp::new(14, 0)).into();

        assert_none!(i_9__10.overlap(i_8__8_30)); // completely before
        assert_none!(i_9__10.overlap(i_8__9)); // abuts
        assert_eq!(assert_some!(i_9__10.overlap(i_8__9_01)), i_9__9_01); // overlaps

        assert_none!(i_9__10.overlap(i_9__9)); // abuts
        assert_eq!(assert_some!(i_9__10.overlap(i_9__9_01)), i_9__9_01); // overlaps

        assert_none!(i_9__10.overlap(i_10__10)); // abuts
        assert_none!(i_9__10.overlap(i_10__10_30)); // abuts

        assert_none!(i_9__10.overlap(i_10_30__11)); // completely after

        assert_none!(i_14__14.overlap(i_14__14)); // abuts
        assert_none!(i_14__14.overlap(i_14__15)); // abuts
        assert_none!(i_14__14.overlap(i_13__14)); // abuts
    }

    #[allow(non_snake_case)]
    #[test]
    fn test_interval_gap() {
        let i_9__10: Interval = Interval::new(Timestamp::new(9, 0), Timestamp::new(10, 0)).into();
        let i_9__9_01: Interval = Interval::new(Timestamp::new(9, 0), Timestamp::new(9, 1)).into();
        let i_9__9: Interval = Interval::new(Timestamp::new(9, 0), Timestamp::new(9, 0)).into();
        let i_8__8_30: Interval = Interval::new(Timestamp::new(8, 0), Timestamp::new(8, 30)).into();
        let i_8__9: Interval = Interval::new(Timestamp::new(8, 0), Timestamp::new(9, 0)).into();
        let i_8__9_01: Interval = Interval::new(Timestamp::new(8, 0), Timestamp::new(9, 1)).into();
        let i_10__10: Interval = Interval::new(Timestamp::new(10, 0), Timestamp::new(10, 0)).into();
        let i_10__10_30: Interval = Interval::new(Timestamp::new(10, 0), Timestamp::new(10, 30)).into();
        let i_10_30__11: Interval = Interval::new(Timestamp::new(10, 30), Timestamp::new(11, 0)).into();
        let i_14__14: Interval = Interval::new(Timestamp::new(14, 0), Timestamp::new(14, 0)).into();
        let i_14__15: Interval = Interval::new(Timestamp::new(14, 0), Timestamp::new(15, 0)).into();
        let i_13__14: Interval = Interval::new(Timestamp::new(13, 0), Timestamp::new(14, 0)).into();

        assert_eq!(
            assert_some!(i_9__10.gap(i_8__8_30)),
            (Timestamp::new(8, 30), Timestamp::new(9, 0)).into()
        ); // completely before
        assert_none!(i_9__10.gap(i_8__9)); // abuts
        assert_none!(i_9__10.gap(i_8__9_01)); // overlaps

        assert_none!(i_9__10.gap(i_9__9)); // abuts
        assert_none!(i_9__10.gap(i_9__9_01)); // overlaps

        assert_none!(i_9__10.gap(i_10__10)); // abuts
        assert_none!(i_9__10.gap(i_10__10_30)); // abuts

        assert_eq!(assert_some!(i_9__10.gap(i_10_30__11)), i_10__10_30); // completely after

        assert_none!(i_14__14.gap(i_14__14)); // abuts
        assert_none!(i_14__14.gap(i_14__15)); // abuts
        assert_none!(i_14__14.gap(i_13__14)); // abuts
    }
}
