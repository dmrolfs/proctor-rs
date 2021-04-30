use super::TelemetryValue;

pub trait ToTelemetry {
    #[allow(clippy::wrong_self_convention)]
    fn to_telemetry(self) -> TelemetryValue;
}

impl<T: Into<TelemetryValue>> ToTelemetry for T {
    fn to_telemetry(self) -> TelemetryValue {
        self.into()
    }
}
