use super::TelemetryValue;
use crate::error::TelemetryError;
use std::convert::TryFrom;

pub trait FromTelemetry: Sized {
    fn from_telemetry(val: TelemetryValue) -> Result<Self, TelemetryError>;
}

impl<T> FromTelemetry for T
where
    T: TryFrom<TelemetryValue>,
    <T as TryFrom<TelemetryValue>>::Error: Into<TelemetryError>,
{
    fn from_telemetry(telemetry: TelemetryValue) -> Result<Self, TelemetryError> {
        T::try_from(telemetry).map_err(|err| err.into())
    }
}
