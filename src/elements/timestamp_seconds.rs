use super::TelemetryValue;
use crate::error::{TelemetryError, TypeExpectation};
use approx::{AbsDiffEq, RelativeEq};
use chrono::{DateTime, TimeZone, Utc};
use oso::PolarClass;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fmt::{self, Debug};
use std::time::Duration;

#[derive(PolarClass, Debug, Copy, Clone, Default, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct TimestampSeconds(f64);

impl TimestampSeconds {
    pub fn now() -> Self {
        Self::new_secs(Utc::now().timestamp())
    }

    pub fn new_secs(ts_secs: i64) -> Self {
        if (f64::MAX as i64) < ts_secs {
            panic!(
                "maximum timestamp seconds support is {} - cannot create with {}",
                f64::MAX as i64,
                ts_secs
            );
        }

        Self(ts_secs as f64)
    }

    pub fn new_precise(ts_secs: f64) -> Self {
        Self(ts_secs)
    }

    pub fn as_f64(&self) -> f64 {
        self.0
    }

    pub fn as_i64(&self) -> i64 {
        self.0 as i64
    }

    pub fn as_utc(&self) -> DateTime<Utc> {
        let secs: i64 = self.0.trunc() as i64;
        let nanos: u32 = (self.0.fract() * 1_000_000_000.) as u32;
        Utc.timestamp(secs, nanos)
    }
}

impl fmt::Display for TimestampSeconds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{}_s", self.0))
    }
}

impl TryFrom<TelemetryValue> for TimestampSeconds {
    type Error = TelemetryError;

    fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
        match telemetry {
            TelemetryValue::Float(f64) => Ok(f64.into()),
            TelemetryValue::Integer(i64) => Ok(i64.into()),
            value => Err(TelemetryError::TypeError {
                expected: format!("a telemetry {}", TypeExpectation::Float),
                actual: Some(format!("{:?}", value)),
            }),
        }
    }
}

impl AsRef<f64> for TimestampSeconds {
    fn as_ref(&self) -> &f64 {
        &self.0
    }
}

impl From<DateTime<Utc>> for TimestampSeconds {
    fn from(that: DateTime<Utc>) -> Self {
        let ts = (that.timestamp_millis() as f64) / 1_000.;
        ts.into()
    }
}

impl From<f64> for TimestampSeconds {
    fn from(timestamp_secs: f64) -> Self {
        Self::new_precise(timestamp_secs)
    }
}

impl From<i64> for TimestampSeconds {
    fn from(timestamp_secs: i64) -> Self {
        Self::new_secs(timestamp_secs)
    }
}

impl Into<f64> for TimestampSeconds {
    fn into(self) -> f64 {
        self.0
    }
}

impl Into<f64> for &TimestampSeconds {
    fn into(self) -> f64 {
        self.0
    }
}

impl Into<i64> for TimestampSeconds {
    fn into(self) -> i64 {
        self.0 as i64
    }
}

impl Into<i64> for &TimestampSeconds {
    fn into(self) -> i64 {
        self.0 as i64
    }
}

impl From<TimestampSeconds> for TelemetryValue {
    fn from(that: TimestampSeconds) -> Self {
        Self::Float(that.0)
    }
}

impl std::ops::Add<Duration> for TimestampSeconds {
    type Output = TimestampSeconds;

    fn add(self, rhs: Duration) -> Self::Output {
        Self(self.0 + rhs.as_secs_f64())
    }
}

impl std::ops::Add<chrono::Duration> for TimestampSeconds {
    type Output = TimestampSeconds;

    fn add(self, rhs: chrono::Duration) -> Self::Output {
        let millis = rhs.num_milliseconds() as f64;
        let secs = millis / 1_000.;
        Self(self.0 + secs)
    }
}

impl std::ops::Sub<Duration> for TimestampSeconds {
    type Output = TimestampSeconds;

    fn sub(self, rhs: Duration) -> Self::Output {
        Self(self.0 - rhs.as_secs_f64())
    }
}

impl std::ops::Sub<chrono::Duration> for TimestampSeconds {
    type Output = TimestampSeconds;

    fn sub(self, rhs: chrono::Duration) -> Self::Output {
        let millis = rhs.num_milliseconds() as f64;
        let secs = millis / 1_000.;
        Self(self.0 - secs)
    }
}

impl std::ops::Add<Duration> for &TimestampSeconds {
    type Output = TimestampSeconds;

    fn add(self, rhs: Duration) -> Self::Output {
        TimestampSeconds(self.0 + rhs.as_secs_f64())
    }
}

impl std::ops::Add<chrono::Duration> for &TimestampSeconds {
    type Output = TimestampSeconds;

    fn add(self, rhs: chrono::Duration) -> Self::Output {
        let millis = rhs.num_milliseconds() as f64;
        let secs = millis / 1_000.;
        TimestampSeconds(self.0 + secs)
    }
}

impl std::ops::Sub<Duration> for &TimestampSeconds {
    type Output = TimestampSeconds;

    fn sub(self, rhs: Duration) -> Self::Output {
        TimestampSeconds(self.0 - rhs.as_secs_f64())
    }
}

impl std::ops::Sub<chrono::Duration> for &TimestampSeconds {
    type Output = TimestampSeconds;

    fn sub(self, rhs: chrono::Duration) -> Self::Output {
        let millis = rhs.num_milliseconds() as f64;
        let secs = millis / 1_000.;
        TimestampSeconds(self.0 - secs)
    }
}

impl AbsDiffEq for TimestampSeconds {
    type Epsilon = f64;

    fn default_epsilon() -> Self::Epsilon {
        f64::default_epsilon()
    }

    fn abs_diff_eq(&self, other: &Self, epsilon: Self::Epsilon) -> bool {
        f64::abs_diff_eq(&self.0, &other.0, epsilon)
    }
}

impl RelativeEq for TimestampSeconds {
    fn default_max_relative() -> Self::Epsilon {
        f64::default_max_relative()
    }

    fn relative_eq(&self, other: &Self, epsilon: Self::Epsilon, max_relative: Self::Epsilon) -> bool {
        f64::relative_eq(&self.0, &other.0, epsilon, max_relative)
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
