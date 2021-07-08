use crate::error::PlanError;
use crate::flink::MetricCatalog;

mod least_squares;
mod regression;
mod ridge_regression;
mod signal;

use std::cmp::Ordering;
use std::fmt::{self, Debug};

use approx::{AbsDiffEq, RelativeEq};
use serde::{Deserialize, Serialize};

use crate::elements::TelemetryValue;
use crate::flink::plan::Benchmark;

#[derive(Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Workload {
    RecordsInPerSecond(RecordsPerSecond),
    NotEnoughData,
    HeuristicsExceedThreshold {},
}

#[derive(Debug, Copy, Clone, Default, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct RecordsPerSecond(pub f64);

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

impl From<RecordsPerSecond> for Workload {
    fn from(that: RecordsPerSecond) -> Self {
        Workload::RecordsInPerSecond(that)
    }
}

impl From<RecordsPerSecond> for TelemetryValue {
    fn from(that: RecordsPerSecond) -> Self {
        Self::Float(that.0)
    }
}

impl AbsDiffEq for RecordsPerSecond {
    type Epsilon = f64;

    #[inline]
    fn default_epsilon() -> Self::Epsilon {
        f64::default_epsilon()
    }

    #[inline]
    fn abs_diff_eq(&self, other: &Self, epsilon: Self::Epsilon) -> bool {
        f64::abs_diff_eq(&self.0, &other.0, epsilon)
    }
}

impl RelativeEq for RecordsPerSecond {
    #[inline]
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
        (measurement.timestamp_secs, measurement.task_nr_records_in_per_sec)
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkloadMeasurement {
    pub timestamp_secs: f64,
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
            timestamp_secs: metrics.timestamp.timestamp() as f64,
            task_nr_records_in_per_sec: metrics.flow.task_nr_records_in_per_sec,
        }
    }
}

pub trait WorkloadForecast: Debug + Sync + Send {
    fn observations_needed(&self) -> (usize, usize);
    fn add_observation(&mut self, measurement: WorkloadMeasurement);
    fn clear(&mut self);
    fn predict_next_workload(&mut self) -> Result<Workload, PlanError>;
}
