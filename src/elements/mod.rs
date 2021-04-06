pub use collection::Collect;
pub use from_telemetry::*;
pub use policy_filter::*;
pub use telemetry::TelemetryData;

mod collection;
mod from_telemetry;
mod performance_history;
mod policy_filter;
mod telemetry;

use oso::PolarClass;
use std::fmt::Debug;
use serde::{Serialize, de::DeserializeOwned};
use std::collections::HashMap;

pub trait ProctorContext: PolarClass + Debug + Clone + PartialEq + Serialize + DeserializeOwned {
    fn custom(&self) -> HashMap<String, String>;
}

