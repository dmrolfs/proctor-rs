use std::cmp::Ordering;
use std::convert::TryFrom;

use ::serde_with::{serde_as, TimestampMilliSeconds};
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use crate::elements::{TelemetryValue, ToTelemetry};
use crate::error::{PlanError, TelemetryError, TypeExpectation};
use crate::flink::plan::RecordsPerSecond;
use crate::flink::MetricCatalog;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkRange {
    pub nr_task_managers: u8,
    pub lo_rate: Option<RecordsPerSecond>,
    pub hi_rate: Option<RecordsPerSecond>,
}

impl BenchmarkRange {
    pub fn lo_from(b: Benchmark) -> Self {
        Self::new(b.nr_task_managers, Some(b.records_out_per_sec), None)
    }

    pub fn hi_from(b: Benchmark) -> Self {
        Self::new(b.nr_task_managers, None, Some(b.records_out_per_sec))
    }

    pub fn new(nr_task_managers: u8, lo_rate: Option<RecordsPerSecond>, hi_rate: Option<RecordsPerSecond>) -> Self {
        Self { nr_task_managers, lo_rate, hi_rate }
    }
}

impl BenchmarkRange {
    pub fn hi_mark(&self) -> Option<Benchmark> {
        self.hi_rate.map(|records_out_per_sec| Benchmark {
            nr_task_managers: self.nr_task_managers,
            records_out_per_sec,
        })
    }

    pub fn lo_mark(&self) -> Option<Benchmark> {
        self.lo_rate.map(|records_out_per_sec| Benchmark {
            nr_task_managers: self.nr_task_managers,
            records_out_per_sec,
        })
    }
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Benchmark {
    pub nr_task_managers: u8,

    pub records_out_per_sec: RecordsPerSecond,
    /* #[serde_as(as = "TimestampMilliSeconds")]
     * pub timestamp: DateTime<Utc>, */
}

impl From<MetricCatalog> for Benchmark {
    fn from(that: MetricCatalog) -> Self {
        Self::from(&that)
    }
}

impl From<&MetricCatalog> for Benchmark {
    fn from(that: &MetricCatalog) -> Self {
        Self {
            nr_task_managers: that.cluster.nr_task_managers,
            records_out_per_sec: RecordsPerSecond(that.flow.records_out_per_sec),
        }
    }
}

impl PartialOrd for Benchmark {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.nr_task_managers.cmp(&other.nr_task_managers))
    }
}

// impl From<&MetricCatalog> for Benchmark {
//     fn from(that: &MetricCatalog) -> Self {
//         Self {
//             nr_task_managers: that.cluster.nr_task_managers,
//             records_out_per_sec: RecordsPerSecond(that.flow.records_out_per_sec),
// //             timestamp: that.timestamp,
// }
// }
// }

// const T_TIMESTAMP: &'static str = "timestamp";
const T_NR_TASK_MANAGERS: &'static str = "nr_task_managers";
const T_RECORDS_OUT_PER_SEC: &'static str = "records_out_per_sec";

impl From<Benchmark> for TelemetryValue {
    fn from(that: Benchmark) -> Self {
        TelemetryValue::Table(maplit::hashmap! {
            // T_TIMESTAMP.to_string() => that.timestamp.timestamp().to_telemetry(),
            T_NR_TASK_MANAGERS.to_string() => that.nr_task_managers.to_telemetry(),
            T_RECORDS_OUT_PER_SEC.to_string() => that.records_out_per_sec.to_telemetry(),
        })
    }
}

impl TryFrom<TelemetryValue> for Benchmark {
    type Error = PlanError;

    fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
        if let TelemetryValue::Table(rep) = telemetry {
            // let timestamp = rep
            //     .get(T_TIMESTAMP)
            //     .map(|v| i64::try_from(v.clone()).map(|secs| Utc.timestamp(secs, 0)))
            //     .ok_or(PlanError::DataNotFound(T_NR_TASK_MANAGERS.to_string()))??;

            let nr_task_managers = rep
                .get(T_NR_TASK_MANAGERS)
                .map(|v| u8::try_from(v.clone()))
                .ok_or(PlanError::DataNotFound(T_NR_TASK_MANAGERS.to_string()))??;

            let records_out_per_sec = rep
                .get(T_RECORDS_OUT_PER_SEC)
                .map(|v| f64::try_from(v.clone()))
                .ok_or(PlanError::DataNotFound(T_RECORDS_OUT_PER_SEC.to_string()))??;

            // Ok(Benchmark { timestamp, nr_task_managers, records_out_per_sec })
            Ok(Benchmark {
                nr_task_managers,
                records_out_per_sec: RecordsPerSecond(records_out_per_sec),
            })
        } else {
            Err(TelemetryError::TypeError {
                expected: format!("{}", TypeExpectation::Table),
                actual: Some(format!("{:?}", telemetry)),
            }
            .into())
        }
    }
}
