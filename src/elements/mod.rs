pub use collection::Collect;
pub use from_telemetry::*;
use oso::PolarClass;
pub use policy_filter::*;
pub use records_per_second::*;
use serde::{de::DeserializeOwned, Serialize};
pub use signal::*;
pub use telemetry::{FromTelemetry, Telemetry, TelemetryValue, ToTelemetry};
pub use timestamp::*;

use crate::AppData;
use crate::phases::collection::SubscriptionRequirements;

mod collection;
mod from_telemetry;
mod policy_filter;
pub mod records_per_second;
pub mod signal;
pub mod telemetry;
pub mod timestamp;

pub type Point = (f64, f64);

pub trait ProctorContext: AppData + SubscriptionRequirements + Clone + PolarClass + Serialize + DeserializeOwned {
    fn custom(&self) -> telemetry::Table;
}
