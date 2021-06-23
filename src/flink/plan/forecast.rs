use crate::error::PlanError;
use crate::flink::MetricCatalog;

mod least_squares;
mod regression;
mod ridge_regression;
mod signal;

use std::cmp::Ordering;
use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::flink::plan::Benchmark;

#[derive(Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Workload {
    RecordsInPerSecond(f64),
    NotEnoughData,
    HeuristicsExceedThreshold {},
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
    fn add_observation(&mut self, measurement: WorkloadMeasurement);
    fn clear(&mut self);
    fn predict_next_workload(&mut self) -> Result<Workload, PlanError>;
}
