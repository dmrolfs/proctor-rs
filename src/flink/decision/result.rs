use crate::elements::{PolicyOutcome, TelemetryValue, ToTelemetry};
use crate::error::{DecisionError, TelemetryError, TypeExpectation};
use crate::graph::stage::{self, ThroughStage};
use crate::{AppData, ProctorContext};
use std::convert::TryFrom;
use std::fmt::Debug;

pub const DECISION_BINDING: &'static str = "direction";
pub const SCALE_UP: &'static str = "up";
pub const SCALE_DOWN: &'static str = "down";
pub const NO_ACTION: &'static str = "no action";

pub fn make_decision_transform<T, C, S>(name: S) -> impl ThroughStage<PolicyOutcome<T, C>, DecisionResult<T>>
where
    T: AppData + Clone + PartialEq,
    C: ProctorContext,
    S: Into<String>,
{
    stage::Map::new(name, move |policy_result: PolicyOutcome<T, C>| {
        if let Some(TelemetryValue::Text(direction)) = policy_result.bindings.get(DECISION_BINDING) {
            DecisionResult::new(policy_result.item, direction)
        } else {
            DecisionResult::NoAction(policy_result.item)
        }
    })
}

const T_ITEM: &'static str = "item";
const T_SCALE_DECISION: &'static str = "scale_decision";

#[derive(Debug, Clone, PartialEq)]
pub enum DecisionResult<T>
where
    T: Debug + Clone + PartialEq,
{
    ScaleUp(T),
    ScaleDown(T),
    NoAction(T),
}

impl<T> DecisionResult<T>
where
    T: Debug + Clone + PartialEq,
{
    pub fn new(item: T, decision_rep: &str) -> Self {
        match decision_rep {
            SCALE_UP => DecisionResult::ScaleUp(item),
            SCALE_DOWN => DecisionResult::ScaleDown(item),
            _ => DecisionResult::NoAction(item),
        }
    }
}

impl<T> Into<TelemetryValue> for DecisionResult<T>
where
    T: Into<TelemetryValue> + Debug + Clone + PartialEq,
{
    fn into(self) -> TelemetryValue {
        match self {
            DecisionResult::ScaleUp(item) => TelemetryValue::Table(maplit::hashmap! {
                T_ITEM.to_string() => item.to_telemetry(),
                T_SCALE_DECISION.to_string() => SCALE_UP.to_telemetry(),
            }),
            DecisionResult::ScaleDown(item) => TelemetryValue::Table(maplit::hashmap! {
                T_ITEM.to_string() => item.to_telemetry(),
                T_SCALE_DECISION.to_string() => SCALE_DOWN.to_telemetry(),
            }),
            DecisionResult::NoAction(item) => TelemetryValue::Table(maplit::hashmap! {
                T_ITEM.to_string() => item.to_telemetry(),
                T_SCALE_DECISION.to_string() => NO_ACTION.to_telemetry(),
            }),
        }
    }
}

impl<T> TryFrom<TelemetryValue> for DecisionResult<T>
where
    T: TryFrom<TelemetryValue> + Debug + Clone + PartialEq,
    <T as TryFrom<TelemetryValue>>::Error: Into<TelemetryError>,
{
    type Error = DecisionError;

    fn try_from(value: TelemetryValue) -> Result<Self, Self::Error> {
        if let TelemetryValue::Table(ref table) = value {
            let item = if let Some(i) = table.get(T_ITEM) {
                T::try_from(i.clone()).map_err(|err| {
                    let t_err: TelemetryError = err.into();
                    t_err.into()
                })
            } else {
                Err(DecisionError::DataNotFound(T_ITEM.to_string()))
            }?;

            let decision = if let Some(d) = table.get(T_SCALE_DECISION) {
                String::try_from(d.clone()).map_err(|err| err.into())
            } else {
                Err(DecisionError::DataNotFound(T_SCALE_DECISION.to_string()))
            }?;

            match decision.as_str() {
                SCALE_UP => Ok(DecisionResult::ScaleUp(item)),
                SCALE_DOWN => Ok(DecisionResult::ScaleDown(item)),
                rep => Err(DecisionError::ParseError(rep.to_string())),
            }
        } else if let TelemetryValue::Unit = value {
            Err(crate::error::TelemetryError::TypeError {
                expected: format!("telemetry {} value", TypeExpectation::Table),
                actual: Some(format!("{:?}", value)),
            }
            .into())
        } else {
            //todo resolves into DecisionError::Other. Improve precision?
            Err(crate::error::TelemetryError::TypeError {
                expected: format!("telemetry {} value", TypeExpectation::Table),
                actual: Some(format!("{:?}", value)),
            }
            .into())
        }
    }
}
