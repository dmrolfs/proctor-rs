use std::collections::HashSet;

use oso::PolarClass;
use serde::{de::DeserializeOwned, Serialize};

pub use collection::Collect;
pub use from_telemetry::*;
pub use policy_filter::*;
pub use telemetry::{FromTelemetry, Telemetry, TelemetryValue, ToTelemetry};

use crate::AppData;

mod collection;
mod from_telemetry;
mod policy_filter;
pub mod telemetry;

pub trait ProctorContext: AppData + Clone + PolarClass + Serialize + DeserializeOwned {
    fn required_context_fields() -> HashSet<&'static str>;
    fn optional_context_fields() -> HashSet<&'static str> {
        HashSet::default()
    }
    fn custom(&self) -> telemetry::Table;
}
