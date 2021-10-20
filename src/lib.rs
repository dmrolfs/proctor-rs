#[cfg(test)]
#[macro_use]
extern crate static_assertions;

extern crate either;
extern crate enum_display_derive;
extern crate proctor_derive;

pub mod app_data;
pub mod elements;
pub mod error;
pub mod graph;
pub mod metrics;
pub mod phases;
pub mod serde;
pub mod tracing;
// pub mod metrics;

pub use app_data::AppData;
pub use elements::ProctorContext;
pub use graph::track_errors;
use pretty_snowflake::{AlphabetCodec, PrettyIdGenerator, RealTimeGenerator};
use std::borrow::Cow;

pub type ProctorResult<T> = Result<T, error::ProctorError>;

pub type IdGenerator = PrettyIdGenerator<RealTimeGenerator, AlphabetCodec>;

pub type Ack = ();

/// An allocation-optimized string.
///
/// We specify `SharedString` to attempt to get the best of both worlds: flexibility to provide a
/// static or dynamic (owned) string, while retaining the performance benefits of being able to
/// take ownership of owned strings and borrows of completely static strings.
///
/// `SharedString` can be converted to from either `&'static str` or `String`, with a method,
/// `const_str`, from constructing `SharedString` from `&'static str` in a `const` fashion.
pub type SharedString = Cow<'static, str>;
