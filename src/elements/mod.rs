pub use collection::Collect;
pub use from_telemetry::*;
pub use policy_filter::*;
pub use telemetry::TelemetryData;

mod collection;
mod from_telemetry;
mod performance_history;
mod policy_filter;
mod telemetry;

use crate::AppData;
use oso::PolarClass;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;

pub trait ProctorContext: AppData + PolarClass + Clone + PartialEq + Serialize + DeserializeOwned {
    fn custom(&self) -> HashMap<String, String>;
}
