use crate::elements::{TelemetryValue, ToTelemetry};
use crate::error::GraphError;
use oso::PolarClass;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fmt::Debug;

pub struct PerformanceHistory {
    pub benchmarks: Vec<Benchmark>,
}

#[derive(Debug, PolarClass, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct Benchmark {
    #[polar(attribute)]
    pub nr_task_managers: i32,

    #[polar(attribute)]
    pub records_out_per_sec: f32,
}

impl Benchmark {
    pub const ZERO: Benchmark = Benchmark {
        nr_task_managers: 0,
        records_out_per_sec: 0.0,
    };
}

impl From<Benchmark> for TelemetryValue {
    fn from(that: Benchmark) -> Self {
        TelemetryValue::Table(maplit::hashmap! {
            "nr_task_managers".to_string() => that.nr_task_managers.to_telemetry(),
            "records_out_per_sec".to_string() => that.records_out_per_sec.to_telemetry(),
        })
    }
}

impl TryFrom<TelemetryValue> for Benchmark {
    type Error = GraphError;

    fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
        if let TelemetryValue::Table(rep) = telemetry {
            let nr_task_managers = rep
                .get("nr_task_managers")
                .map(|v| i32::try_from(v.clone()))
                .ok_or(GraphError::TypeError("i32".to_string(), "not found".to_string()))??;

            let records_out_per_sec = rep
                .get("records_out_per_sec")
                .map(|v| f32::try_from(v.clone()))
                .ok_or(GraphError::TypeError("f32".to_string(), "not found".to_string()))??;

            Ok(Benchmark {
                nr_task_managers,
                records_out_per_sec,
            })
        } else {
            Err(GraphError::TypeError(
                "a telemetry Table".to_string(),
                format!("{:?}", telemetry),
            ))
        }
    }
}

