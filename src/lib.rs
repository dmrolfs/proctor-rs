#![warn(clippy::all)]
#![forbid(unsafe_code)]
#![warn(
clippy::cargo,
// missing_docs,
clippy::nursery,
// clippy::pedantic,
future_incompatible,
rust_2018_idioms
)]

#[cfg(test)]
#[macro_use]
extern crate static_assertions;

pub mod app_data;
pub mod elements;
pub mod error;
pub mod graph;
pub mod metrics;
pub mod phases;
pub mod serde;
pub mod tracing;

pub use app_data::AppData;
pub use elements::ProctorContext;
pub use graph::track_errors;
use pretty_snowflake::{Id, LabeledRealtimeIdGenerator};

pub type ProctorResult<T> = Result<T, error::ProctorError>;

pub type ProctorIdGenerator<T> = LabeledRealtimeIdGenerator<T>;

pub type Ack = ();

pub trait Correlation {
    type Correlated: Sized;
    fn correlation(&self) -> &Id<Self::Correlated>;
}

/// An allocation-optimized string.
///
/// We specify `SharedString` to attempt to get the best of both worlds: flexibility to provide a
/// static or dynamic (owned) string, while retaining the performance benefits of being able to
/// take ownership of owned strings and borrows of completely static strings.
///
/// `SharedString` can be converted to from either `&'static str` or `String`, with a method,
/// `const_str`, from constructing `SharedString` from `&'static str` in a `const` fashion.
pub type SharedString = std::borrow::Cow<'static, str>;
