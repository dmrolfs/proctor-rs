use crate::elements::{PolicyResult, TelemetryValue, ToTelemetry};
use crate::error::GraphError;
use crate::graph::stage::{self, ThroughStage};
use crate::{AppData, ProctorContext};
use std::convert::TryFrom;
use std::fmt::Debug;

pub const DECISION_BINDING: &'static str = "direction";
pub const SCALE_UP: &'static str = "up";
pub const SCALE_DOWN: &'static str = "down";

pub fn make_decision_transform<T, C, S: Into<String>>(
    name: S,
) -> impl ThroughStage<PolicyResult<T, C>, DecisionResult<T>>
where
    T: AppData + Clone + PartialEq,
    C: ProctorContext,
{
    stage::Map::new(name, |policy_result: PolicyResult<T, C>| {
        if let Some(TelemetryValue::Text(direction)) = policy_result.bindings.get(DECISION_BINDING) {
            //todo: what else goes into DecisionResult? performance info
            DecisionResult::new(policy_result.item, direction)
        } else {
            DecisionResult::None
        }
    })
}

#[derive(Debug, Clone, PartialEq)]
pub enum DecisionResult<T>
where
    T: Debug + Clone + PartialEq,
{
    ScaleUp(T),
    ScaleDown(T),
    None,
}

impl<T> DecisionResult<T>
where
    T: Debug + Clone + PartialEq,
{
    pub fn new(item: T, decision_rep: &str) -> Self {
        match decision_rep {
            SCALE_UP => DecisionResult::ScaleUp(item),
            SCALE_DOWN => DecisionResult::ScaleDown(item),
            _ => DecisionResult::None,
        }
    }
}

const T_ITEM_LABEL: &'static str = "item";
const T_SCALE_DECISION_LABEL: &'static str = "scale_decision";

impl<T> Into<TelemetryValue> for DecisionResult<T>
where
    T: Into<TelemetryValue> + Debug + Clone + PartialEq,
{
    fn into(self) -> TelemetryValue {
        match self {
            DecisionResult::ScaleUp(item) => TelemetryValue::Table(maplit::hashmap! {
                T_ITEM_LABEL.to_string() => item.to_telemetry(),
                T_SCALE_DECISION_LABEL.to_string() => SCALE_UP.to_telemetry(),
            }),
            DecisionResult::ScaleDown(item) => TelemetryValue::Table(maplit::hashmap! {
                T_ITEM_LABEL.to_string() => item.to_telemetry(),
                T_SCALE_DECISION_LABEL.to_string() => SCALE_DOWN.to_telemetry(),
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
            let item = if let Some(i) = table.get(T_ITEM_LABEL) {
                T::try_from(i.clone()).map_err(|err| err.into())
            } else {
                Err(GraphError::GraphPrecondition(format!(
                    "failed to find `{}` in Table",
                    T_ITEM_LABEL
                )))
            }?;

            let decision = if let Some(d) = table.get(T_SCALE_DECISION_LABEL) {
                String::try_from(d.clone()).map_err(|err| err.into())
            } else {
                Err(GraphError::GraphPrecondition(format!(
                    "failed to find `{}` in Table",
                    T_SCALE_DECISION_LABEL
                )))
            }?;

            let result = match decision.as_str() {
                SCALE_UP => DecisionResult::ScaleUp(item),
                SCALE_DOWN => DecisionResult::ScaleDown(item),
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
