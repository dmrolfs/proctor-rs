#![warn(clippy::all)]
#![forbid(unsafe_code)]
#![warn(
clippy::cargo,
clippy::suspicious,
// missing_docs,
clippy::nursery,
// clippy::pedantic,
future_incompatible,
rust_2018_idioms
)]

#[cfg(test)]
#[macro_use]
extern crate static_assertions;

use crate::elements::Timestamp;
use pretty_snowflake::{Id, LabeledRealtimeIdGenerator};

pub mod app_data;
pub mod elements;
pub mod envelope;
pub mod error;
pub mod graph;
pub mod metrics;
pub mod phases;
pub mod serde;
pub mod tracing;

pub use app_data::AppData;
pub use elements::ProctorContext;
pub use envelope::{Envelope, IntoEnvelope, MetaData};
pub use graph::track_errors;

pub type ProctorResult<T> = Result<T, error::ProctorError>;
pub type ProctorIdGenerator<T> = LabeledRealtimeIdGenerator<T>;

pub type Ack = ();
pub type Env<T> = Envelope<T>;

pub trait Correlation {
    type Correlated: Sized + Sync;
    fn correlation(&self) -> &Id<Self::Correlated>;
}

pub trait ReceivedAt {
    fn recv_timestamp(&self) -> Timestamp;
}

impl<T> ReceivedAt for (T, Timestamp) {
    fn recv_timestamp(&self) -> Timestamp {
        self.1
    }
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
