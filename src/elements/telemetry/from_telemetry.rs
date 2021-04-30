use super::TelemetryValue;
use crate::error::GraphError;
use crate::graph::GraphResult;
use std::convert::TryFrom;

pub trait FromTelemetry: Sized {
    fn from_telemetry(val: TelemetryValue) -> GraphResult<Self>;
}

impl<T> FromTelemetry for T
where
    T: TryFrom<TelemetryValue>,
    <T as TryFrom<TelemetryValue>>::Error: Into<GraphError>,
{
    fn from_telemetry(telemetry: TelemetryValue) -> GraphResult<Self> {
        T::try_from(telemetry).map_err(|err| err.into())
    }
}
