// #[macro_use]
extern crate proctor_derive;

pub mod app_data;
pub mod elements;
pub mod error;
pub mod flink;
pub mod graph;
pub mod phases;
pub mod serde;
pub mod settings;
pub mod telemetry;

pub use app_data::AppData;
pub use elements::ProctorContext;
pub use error::ProctorResult;

pub type Ack = ();
