use crate::elements::{PolicyResult, TelemetryValue, ToTelemetry};
use crate::error::GraphError;
use crate::graph::stage::{self, ThroughStage};
use crate::{AppData, ProctorContext};
use std::convert::TryFrom;
use std::fmt::Debug;
use oso::{PolarClass, ToPolar};
use serde::{Deserialize, Serialize};

pub const DECISION_BINDING: &'static str = "direction";
pub const SCALE_UP: &'static str = "up";
pub const SCALE_DOWN: &'static str = "down";

pub fn make_decision_transform<T, C, S, F>(
    name: S,
    mut extract_benchmark: F
) -> impl ThroughStage<PolicyResult<T, C>, DecisionResult<T>>
where
    T: AppData + Clone + PartialEq,
    C: ProctorContext,
    S: Into<String>,
    F: FnMut(&PolicyResult<T, C>) -> Benchmark + Send + Sync + 'static,
{
    stage::Map::new(name, move |policy_result: PolicyResult<T, C>| {
        if let Some(TelemetryValue::Text(direction)) = policy_result.bindings.get(DECISION_BINDING) {
            let benchmark = extract_benchmark(&policy_result);
            DecisionResult::new(policy_result.item, benchmark, direction)
        } else {
            DecisionResult::None
        }
    })
}

#[derive(Debug, PolarClass, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct Benchmark {
    #[polar(attribute)]
    pub nr_task_managers: u16,

    #[polar(attribute)]
    pub records_out_per_sec: f32,
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
            let nr_task_managers = rep.get("nr_task_managers")
                .map(|v| u16::try_from(v.clone()))
                .ok_or(GraphError::TypeError("u16".to_string(), "not found".to_string()))??;

            let records_out_per_sec = rep.get("records_out_per_sec")
                .map(|v| f32::try_from(v.clone()))
                .ok_or(GraphError::TypeError("f32".to_string(), "not found".to_string()))??;

            Ok(Benchmark { nr_task_managers, records_out_per_sec })
        } else {
            Err(GraphError::TypeError(
                "a telemetry Table".to_string(),
                format!("{:?}", telemetry),
            ))
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum DecisionResult<T>
where
    T: Debug + Clone + PartialEq,
{
    ScaleUp(T, Benchmark),
    ScaleDown(T, Benchmark),
    None,
}

impl<T> DecisionResult<T>
where
    T: Debug + Clone + PartialEq,
{
    pub fn new(item: T, benchmark: Benchmark, decision_rep: &str) -> Self {
        match decision_rep {
            SCALE_UP => DecisionResult::ScaleUp(item, benchmark),
            SCALE_DOWN => DecisionResult::ScaleDown(item, benchmark),
            _ => DecisionResult::None,
        }
    }
}

const T_ITEM: &'static str = "item";
const T_BENCHMARK: &'static str = "benchmark";
const T_SCALE_DECISION: &'static str = "scale_decision";

impl<T> Into<TelemetryValue> for DecisionResult<T>
where
    T: Into<TelemetryValue> + Debug + Clone + PartialEq,
{
    fn into(self) -> TelemetryValue {
        match self {
            DecisionResult::ScaleUp(item, benchmark) => TelemetryValue::Table(maplit::hashmap! {
                T_ITEM.to_string() => item.to_telemetry(),
                T_BENCHMARK.to_string() => benchmark.to_telemetry(),
                T_SCALE_DECISION.to_string() => SCALE_UP.to_telemetry(),
            }),
            DecisionResult::ScaleDown(item, benchmark) => TelemetryValue::Table(maplit::hashmap! {
                T_ITEM.to_string() => item.to_telemetry(),
                T_BENCHMARK.to_string() => benchmark.to_telemetry(),
                T_SCALE_DECISION.to_string() => SCALE_DOWN.to_telemetry(),
            }),
            DecisionResult::None => TelemetryValue::Unit,
        }
    }
}

impl<T> TryFrom<TelemetryValue> for DecisionResult<T>
where
    T: TryFrom<TelemetryValue> + Debug + Clone + PartialEq,
    <T as TryFrom<TelemetryValue>>::Error: Into<GraphError>,
{
    type Error = GraphError;

    fn try_from(value: TelemetryValue) -> Result<Self, Self::Error> {
        if let TelemetryValue::Table(ref table) = value {
            let item = if let Some(i) = table.get(T_ITEM) {
                T::try_from(i.clone()).map_err(|err| err.into())
            } else {
                Err(GraphError::GraphPrecondition(format!(
                    "failed to find `{}` in Table",
                    T_ITEM
                )))
            }?;

            let benchmark = if let Some(b) = table.get(T_BENCHMARK) {
                Benchmark::try_from(b.clone()).map_err(|err| err.into())
            } else {
                Err(GraphError::GraphPrecondition(format!(
                    "failed to find `{}` in Table",
                    T_BENCHMARK
                )))
            }?;

            let decision = if let Some(d) = table.get(T_SCALE_DECISION) {
                String::try_from(d.clone()).map_err(|err| err.into())
            } else {
                Err(GraphError::GraphPrecondition(format!(
                    "failed to find `{}` in Table",
                    T_SCALE_DECISION
                )))
            }?;

            let result = match decision.as_str() {
                SCALE_UP => DecisionResult::ScaleUp(item, benchmark),
                SCALE_DOWN => DecisionResult::ScaleDown(item, benchmark),
                _ => DecisionResult::None,
            };

            Ok(result)
        } else if let TelemetryValue::Unit = value {
            Ok(DecisionResult::None)
        } else {
            Err(GraphError::TypeError("Table|Unit".to_string(), format!("{:?}", value)))
        }
    }
}
