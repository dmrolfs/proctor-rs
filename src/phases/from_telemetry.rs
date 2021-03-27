// use crate::graph::{ThroughShape};
// use crate::graph::stage::{self, Stage};
// use crate::phases::collection::TelemetryData;
// use crate::AppData;
// use serde::Deserialize;
// use crate::ProctorResult;
//
// pub type FromTelemetryShape<'de, Out: AppData + Deserialize<'de>> = Box<dyn FromTelemetryStage<'de, Out>>;
// pub trait FromTelemetryStage<'de, Out: AppData + Deserialize<'de>>: Stage + ThroughShape<In = TelemetryData, Out = Out> + 'static {}
// impl<
//     'de,
//     Out: AppData + Deserialize<'de>,
//     T: 'static + Stage + ThroughShape<In = TelemetryData, Out = Out>
// > FromTelemetryStage<'de, Out> for T {}
//
// #[tracing::instrument(
//     level="info",
//     skip(name),
//     fields(name=%name.into()),
// )]
// pub fn make_from_telemetry_stage<'de, Out, S>(name: S) -> ProctorResult<FromTelemetryShape<'de, Out>>
// where
//     Out: AppData + Deserialize<'de>,
//     S: Into<String>,
// {
//     let conv_op = |data: TelemetryData| { data.try_into::<Out>() };
//     let from = stage::Map::new(name, conv_op);
//     filter
// }
