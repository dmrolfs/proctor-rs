use crate::elements::{TelemetryValue, ToTelemetry};
use crate::error::{PlanError, TelemetryError, TypeExpectation};
use ::serde_with::{serde_as, TimestampMilliSeconds};
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::convert::TryFrom;

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Benchmark {
    pub nr_task_managers: usize,

    pub records_out_per_sec: f64,

    #[serde_as(as = "TimestampMilliSeconds")]
    pub timestamp: DateTime<Utc>,
}

impl PartialOrd for Benchmark {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.nr_task_managers.cmp(&other.nr_task_managers))
    }
}

const T_TIMESTAMP: &'static str = "timestamp";
const T_NR_TASK_MANAGERS: &'static str = "nr_task_managers";
const T_RECORDS_OUT_PER_SEC: &'static str = "records_out_per_sec";

impl From<Benchmark> for TelemetryValue {
    fn from(that: Benchmark) -> Self {
        TelemetryValue::Table(maplit::hashmap! {
            T_TIMESTAMP.to_string() => that.timestamp.timestamp_millis().to_telemetry(),
            T_NR_TASK_MANAGERS.to_string() => that.nr_task_managers.to_telemetry(),
            T_RECORDS_OUT_PER_SEC.to_string() => that.records_out_per_sec.to_telemetry(),
        })
    }
}

impl TryFrom<TelemetryValue> for Benchmark {
    type Error = PlanError;

    fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
        if let TelemetryValue::Table(rep) = telemetry {
            let timestamp = rep
                .get(T_TIMESTAMP)
                .map(|v| {
                    i64::try_from(v.clone())
                        // .map(|millis| Utc::timestamp_millis(&millis))
                        .map(|millis| Utc.timestamp_millis(millis))
                })
                .ok_or(PlanError::DataNotFound(T_NR_TASK_MANAGERS.to_string()))??;

            let nr_task_managers = rep
                .get(T_NR_TASK_MANAGERS)
                .map(|v| usize::try_from(v.clone()))
                .ok_or(PlanError::DataNotFound(T_NR_TASK_MANAGERS.to_string()))??;

            let records_out_per_sec = rep
                .get(T_RECORDS_OUT_PER_SEC)
                .map(|v| f64::try_from(v.clone()))
                .ok_or(PlanError::DataNotFound(T_RECORDS_OUT_PER_SEC.to_string()))??;

            Ok(Benchmark {
                timestamp,
                nr_task_managers,
                records_out_per_sec,
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
