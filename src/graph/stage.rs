mod sink;
mod source;
mod through;

use std::fmt;

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;

pub use self::sink::*;
pub use self::source::*;
pub use self::through::*;
use super::{SinkShape, SourceShape, ThroughShape};
use crate::{ProctorResult, SharedString};

pub trait SourceStage<Out>: Stage + SourceShape<Out = Out> + 'static {}
impl<Out, T: 'static + Stage + SourceShape<Out = Out>> SourceStage<Out> for T {}

pub trait SinkStage<In>: Stage + SinkShape<In = In> + 'static {}
impl<In, T: 'static + Stage + SinkShape<In = In>> SinkStage<In> for T {}

pub trait ThroughStage<In, Out>: Stage + ThroughShape<In = In, Out = Out> + 'static {}
impl<In, Out, T: 'static + Stage + ThroughShape<In = In, Out = Out>> ThroughStage<In, Out> for T {}

/// Behavior driving graph stage lifecycle.
///
/// macro dyn_upcast enables the upcast conversion of concrete stages into the base Stage type when
/// placed in a graph. See https://github.com/Lej77/cast_trait_object README for background.
#[dyn_upcast]
#[async_trait]
pub trait Stage: fmt::Debug + Send + Sync {
    fn name(&self) -> SharedString;
    async fn check(&self) -> ProctorResult<()>;
    async fn run(&mut self) -> ProctorResult<()>;
    async fn close(self: Box<Self>) -> ProctorResult<()>;
}

pub trait WithApi {
    type Sender;
    fn tx_api(&self) -> Self::Sender;
}

pub trait WithMonitor {
    type Receiver;
    fn rx_monitor(&self) -> Self::Receiver;
}
