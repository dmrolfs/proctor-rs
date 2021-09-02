#[cfg(test)]
#[macro_use]
extern crate static_assertions;

// #[macro_use]
extern crate proctor_derive;

// #[macro_use]
extern crate enum_display_derive;

pub mod app_data;
pub mod elements;
pub mod error;
pub mod graph;
pub mod phases;
pub mod serde;
pub mod tracing;

pub use app_data::AppData;
pub use elements::ProctorContext;

pub type ProctorResult<T> = Result<T, error::ProctorError>;

pub type Ack = ();
