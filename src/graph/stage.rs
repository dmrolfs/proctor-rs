mod sink;
mod source;
mod through;

pub use self::sink::*;
pub use self::source::*;
pub use self::through::*;

use anyhow::Result;
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use std::fmt;

/// Behavior driving graph stage lifecycle.
///
/// macro dyn_upcast enables the upcast conversion of concrete stages into the base Stage type when
/// placed in a graph. See https://github.com/Lej77/cast_trait_object README for background.
#[dyn_upcast]
#[async_trait]
pub trait Stage: fmt::Debug + Send + Sync {
    fn name(&self) -> &str;
    async fn check(&self) -> Result<()>;
    async fn run(&mut self) -> Result<()>;
    async fn close(self: Box<Self>) -> Result<()>;
}

pub trait WithApi {
    type Sender;
    fn tx_api(&self) -> Self::Sender;
}

pub trait WithMonitor {
    type Receiver;
    fn rx_monitor(&self) -> Self::Receiver;
}
