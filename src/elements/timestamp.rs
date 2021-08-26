use super::{TelemetryValue, ToTelemetry};
use crate::error::{TelemetryError, TypeExpectation};
use approx::{AbsDiffEq, RelativeEq};
use chrono::{DateTime, TimeZone, Utc};
use oso::PolarClass;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fmt::{self, Debug};
use std::time::Duration;

#[derive(PolarClass, Debug, Copy, Clone, Default, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Timestamp(i64, u32);

impl Timestamp {
    pub fn now() -> Self {
        let now = Utc::now();
        Self::new(now.timestamp(), now.timestamp_subsec_nanos())
    }

    pub fn new_secs(secs: i64) -> Self {
        Self(secs, 0)
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
        (self.0 as f64) + (self.1 as f64) / 10_f64.powi(9)
    }

    pub fn as_secs(&self) -> i64 {
        self.0
    }

    pub fn as_millis(&self) -> i64 {
        let sec_millis = self.0 * 1_000;
        let nsec_millis = (self.1 as i64) / 10_i64.pow(6);
        sec_millis + nsec_millis
    }

    pub fn as_nanos(&self) -> i64 {
        self.0 * 10_i64.pow(9) + self.1 as i64
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
        f.write_fmt(format_args!("{}_s", self.0))
    }
}

pub const FORMAT: &'static str = "%+";
pub const SECS_KEY: &'static str = "secs";
pub const NSECS_KEY: &'static str = "nsecs";

impl TryFrom<TelemetryValue> for Timestamp {
    type Error = TelemetryError;

    fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
        match telemetry {
            TelemetryValue::Seq(mut seq) if seq.len() == 2 => {
                let nsecs = seq.pop().map(|v| u32::try_from(v)).transpose()?.unwrap();
                let secs = seq.pop().map(|v| i64::try_from(v)).transpose()?.unwrap();
                Ok(Self::new(secs, nsecs))
            }
            TelemetryValue::Float(f64) => Ok(f64.into()),
            TelemetryValue::Integer(i64) => Ok(i64.into()),
            TelemetryValue::Table(mut table) => {
                let secs = table
                    .remove(SECS_KEY)
                    .map(|val| i64::try_from(val))
                    .transpose()?
                    .unwrap_or(0);
                let nsecs = table
                    .remove(NSECS_KEY)
                    .map(|val| u32::try_from(val))
                    .transpose()?
                    .unwrap_or(0);
                Ok(Self(secs, nsecs))
            }
            TelemetryValue::Text(rep) => {
                let dt = DateTime::parse_from_str(rep.as_str(), FORMAT)
                    .map_err(|err| TelemetryError::ValueParseError(err.into()))?
                    .with_timezone(&Utc);
                Ok(dt.into())
            }
            value => Err(TelemetryError::TypeError {
                expected: format!("a telemetry {}", TypeExpectation::Float),
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

impl From<DateTime<Utc>> for Timestamp {
    fn from(that: DateTime<Utc>) -> Self {
        Self(that.timestamp(), that.timestamp_subsec_nanos())
    }
}

impl From<f64> for Timestamp {
    fn from(timestamp_secs: f64) -> Self {
        let secs = timestamp_secs.floor() as i64;
        let nsecs = (timestamp_secs.fract() * 10_f64.powi(9)) as u32;
        Self(secs, nsecs)
    }
}

impl From<i64> for Timestamp {
    fn from(timestamp_secs: i64) -> Self {
        Self(timestamp_secs, 0)
    }
}

impl Into<f64> for Timestamp {
    fn into(self) -> f64 {
        self.as_f64()
    }
}

impl Into<f64> for &Timestamp {
    fn into(self) -> f64 {
        self.as_f64()
    }
}

impl Into<i64> for Timestamp {
    fn into(self) -> i64 {
        self.0
    }
}

impl Into<i64> for &Timestamp {
    fn into(self) -> i64 {
        self.0
    }
}

impl From<Timestamp> for TelemetryValue {
    fn from(that: Timestamp) -> Self {
        Self::Table(maplit::hashmap! {
            SECS_KEY.to_string() => that.0.to_telemetry(),
            NSECS_KEY.to_string() => that.1.to_telemetry(),
        })
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

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use pretty_assertions::assert_eq;
//     use claim::*;
//     use approx::assert_relative_eq;
//
//     #[test]
//     fn test_timestamp_secs_FOO_BAR() -> anyhow::Result<()> {
//         todo!()
//     }
// }
