pub use collection::Collect;
pub use from_telemetry::*;
pub use policy_filter::*;
pub use telemetry::*;

mod collection;
mod from_telemetry;
mod performance_history;
mod policy_filter;
mod telemetry;

use crate::AppData;
use oso::PolarClass;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::{HashMap, HashSet};

pub trait ProctorContext: AppData + PolarClass + Clone + PartialEq + Serialize + DeserializeOwned + Sync {
    fn required_context_fields() -> HashSet<String>;
    fn optional_context_fields() -> HashSet<String> {
        HashSet::default()
    }
    fn custom(&self) -> HashMap<String, String>;
}
