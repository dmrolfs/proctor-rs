mod sink;
mod source;
mod through;

use std::fmt;

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use once_cell::sync::Lazy;
use prometheus::{HistogramOpts, HistogramTimer, HistogramVec};

pub use self::sink::*;
pub use self::source::*;
pub use self::through::*;
use super::{SinkShape, SourceShape, ThroughShape};
use crate::ProctorResult;

pub trait SourceStage<Out>: Stage + SourceShape<Out = Out> + 'static {}
impl<Out, T: 'static + Stage + SourceShape<Out = Out>> SourceStage<Out> for T {}

pub trait SinkStage<In>: Stage + SinkShape<In = In> + 'static {}
impl<In, T: 'static + Stage + SinkShape<In = In>> SinkStage<In> for T {}

pub trait ThroughStage<In, Out>: Stage + ThroughShape<In = In, Out = Out> + 'static {}
impl<In, Out, T: 'static + Stage + ThroughShape<In = In, Out = Out>> ThroughStage<In, Out> for T {}

pub static STAGE_EVAL_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "stage_eval_time",
            "Time spent in a stage's event evaluation cycle in seconds",
        )
        .buckets(vec![
            0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
        ]),
        &["stage"],
    )
    .expect("failed creating stage_eval_time metric")
});

#[inline]
pub fn start_stage_eval_time(stage: &str) -> HistogramTimer {
    STAGE_EVAL_TIME.with_label_values(&[stage]).start_timer()
}

/// Behavior driving graph stage lifecycle.
///
/// macro dyn_upcast enables the upcast conversion of concrete stages into the base Stage type when
/// placed in a graph. See https://github.com/Lej77/cast_trait_object README for background.
#[dyn_upcast]
#[async_trait]
pub trait Stage: fmt::Debug + Send + Sync {
    fn name(&self) -> &str;
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
