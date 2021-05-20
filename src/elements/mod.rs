pub use collection::Collect;
pub use from_telemetry::*;
pub use policy_filter::*;
pub use telemetry::{FromTelemetry, Telemetry, TelemetryValue, ToTelemetry};

mod collection;
mod from_telemetry;
mod performance_history;
mod policy_filter;
pub mod telemetry;

use crate::AppData;
use oso::PolarClass;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashSet;

pub trait ProctorContext: AppData + Clone + PolarClass + Serialize + DeserializeOwned {
    fn required_context_fields() -> HashSet<&'static str>;
    fn optional_context_fields() -> HashSet<&'static str> {
        HashSet::default()
    }
    fn custom(&self) -> telemetry::Table;
}
