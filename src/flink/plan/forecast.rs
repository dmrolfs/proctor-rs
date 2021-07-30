use crate::error::PlanError;
use crate::flink::MetricCatalog;

mod calculator;
mod least_squares;
mod regression;
mod ridge_regression;
mod signal;

use std::cmp::Ordering;
use std::fmt::{self, Debug};
use std::time::Duration;

use approx::{AbsDiffEq, RelativeEq};
pub use calculator::ForecastCalculator;
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use crate::elements::TelemetryValue;

#[cfg(test)]
use mockall::{automock, predicate::*};


#[cfg_attr(test, automock)]
pub trait WorkloadForecastBuilder: Debug + Sync + Send {
    fn observations_needed(&self) -> (usize, usize);
    fn add_observation(&mut self, measurement: WorkloadMeasurement);
    fn clear(&mut self);
    fn build_forecast(&mut self) -> Result<Box<dyn WorkloadForecast>, PlanError>;
}

#[cfg_attr(test, automock)]
pub trait WorkloadForecast: Debug {
    fn name(&self) -> &'static str;
    fn workload_at(&self, timestamp: TimestampSeconds) -> Result<RecordsPerSecond, PlanError>;
    fn total_records_between(&self, start: TimestampSeconds, end: TimestampSeconds) -> Result<f64, PlanError>;
    fn correlation_coefficient(&self) -> f64;
}

#[derive(Debug, Copy, Clone, Default, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct TimestampSeconds(f64);

impl TimestampSeconds {
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


#[derive(Debug, Copy, Clone, Default, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct RecordsPerSecond(f64);

impl RecordsPerSecond {
    pub fn new(recs_per_sec: f64) -> Self {
        Self(recs_per_sec)
    }

    pub fn max(lhs: RecordsPerSecond, rhs: RecordsPerSecond) -> RecordsPerSecond {
        f64::max(lhs.0, rhs.0).into()
    }

    pub fn min(lhs: RecordsPerSecond, rhs: RecordsPerSecond) -> RecordsPerSecond {
        f64::min(lhs.0, rhs.0).into()
    }
}

impl fmt::Display for RecordsPerSecond {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{:.5?} out_records / s", self.0))
    }
}

impl AsRef<f64> for RecordsPerSecond {
    fn as_ref(&self) -> &f64 {
        &self.0
    }
}

impl From<f64> for RecordsPerSecond {
    fn from(rate: f64) -> Self {
        Self(rate)
    }
}

impl Into<f64> for RecordsPerSecond {
    fn into(self) -> f64 {
        self.0
    }
}

impl Into<f64> for &RecordsPerSecond {
    fn into(self) -> f64 {
        self.0
    }
}

impl From<RecordsPerSecond> for TelemetryValue {
    fn from(that: RecordsPerSecond) -> Self {
        Self::Float(that.0)
    }
}

impl AbsDiffEq for RecordsPerSecond {
    type Epsilon = f64;

    fn default_epsilon() -> Self::Epsilon {
        f64::default_epsilon()
    }

    fn abs_diff_eq(&self, other: &Self, epsilon: Self::Epsilon) -> bool {
        f64::abs_diff_eq(&self.0, &other.0, epsilon)
    }
}

impl RelativeEq for RecordsPerSecond {
    fn default_max_relative() -> Self::Epsilon {
        f64::default_max_relative()
    }

    fn relative_eq(&self, other: &Self, epsilon: Self::Epsilon, max_relative: Self::Epsilon) -> bool {
        f64::relative_eq(&self.0, &other.0, epsilon, max_relative)
    }
}

pub type Point = (f64, f64);

impl From<WorkloadMeasurement> for Point {
    fn from(measurement: WorkloadMeasurement) -> Self {
        (
            measurement.timestamp_secs as f64,
            measurement.task_nr_records_in_per_sec,
        )
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkloadMeasurement {
    pub timestamp_secs: i64,
    pub task_nr_records_in_per_sec: f64,
}

impl PartialOrd for WorkloadMeasurement {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.timestamp_secs.partial_cmp(&other.timestamp_secs)
    }
}

impl From<MetricCatalog> for WorkloadMeasurement {
    fn from(metrics: MetricCatalog) -> Self {
        Self {
            timestamp_secs: metrics.timestamp.timestamp(),
            task_nr_records_in_per_sec: metrics.flow.task_nr_records_in_per_sec,
        }
    }
}
