use crate::elements::{TelemetryValue, ToTelemetry};
use crate::error::{PlanError, TelemetryError, TypeExpectation};
use oso::PolarClass;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::convert::TryFrom;

#[derive(Debug, PolarClass, Clone, PartialEq, Serialize, Deserialize)]
pub struct Benchmark {
    #[polar(attribute)]
    pub nr_task_managers: i32,

    #[polar(attribute)]
    pub records_out_per_sec: f32,
}

impl PartialOrd for Benchmark {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.nr_task_managers.cmp(&other.nr_task_managers))
    }
}

const T_NR_TASK_MANAGERS: &'static str = "nr_task_managers";
const T_RECORDS_OUT_PER_SEC: &'static str = "records_out_per_sec";

impl From<Benchmark> for TelemetryValue {
    fn from(that: Benchmark) -> Self {
        TelemetryValue::Table(maplit::hashmap! {
            T_NR_TASK_MANAGERS.to_string() => that.nr_task_managers.to_telemetry(),
            T_RECORDS_OUT_PER_SEC.to_string() => that.records_out_per_sec.to_telemetry(),
        })
    }
}

impl TryFrom<TelemetryValue> for Benchmark {
    type Error = PlanError;

    fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
        if let TelemetryValue::Table(rep) = telemetry {
            let nr_task_managers = rep
                .get(T_NR_TASK_MANAGERS)
                .map(|v| i32::try_from(v.clone()))
                .ok_or(PlanError::DataNotFound(T_NR_TASK_MANAGERS.to_string()))??;

            let records_out_per_sec = rep
                .get(T_RECORDS_OUT_PER_SEC)
                .map(|v| f32::try_from(v.clone()))
                .ok_or(PlanError::DataNotFound(T_RECORDS_OUT_PER_SEC.to_string()))??;

            Ok(Benchmark {
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
