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

// #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
// pub struct UtcDateTime(
//     #[serde(with = "crate::serde")]
//     pub chrono::DateTime<chrono::Utc>
// );
//
// impl oso::PolarClass for UtcDateTime {}
//
// impl From<chrono::DateTime<chrono::Utc>> for UtcDateTime {
//     fn from(that: chrono::DateTime<chrono::Utc>) -> Self { UtcDateTime(that) }
// }
//
// impl Into<chrono::DateTime<chrono::Utc>> for UtcDateTime {
//     fn into(self) -> chrono::DateTime<chrono::Utc> { self.0 }
// }
