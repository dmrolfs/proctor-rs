use crate::graph::stage::{self, Stage};
use crate::graph::{Connect, Graph, GraphResult, SinkShape, SourceShape, ThroughShape};
use crate::phases::collection::TelemetryData;
use crate::AppData;
use crate::ProctorResult;
use serde::Deserialize;

pub type FromTelemetryShape<'de, Out: AppData + Deserialize<'de>> = Box<dyn FromTelemetryStage<'de, Out>>;
pub trait FromTelemetryStage<'de, Out: AppData + Deserialize<'de>>: Stage + ThroughShape<In = TelemetryData, Out = Out> + 'static {}
impl<'de, Out: AppData + Deserialize<'de>, T: 'static + Stage + ThroughShape<In = TelemetryData, Out = Out>> FromTelemetryStage<'de, Out> for T {}

#[tracing::instrument(
    level="info",
    skip(name),
    fields(stage=%name.as_ref()),
)]
pub async fn make_from_telemetry<'de, Out, S>(name: S) -> ProctorResult<FromTelemetryShape<'de, Out>>
where
    Out: AppData + Deserialize<'de>,
    S: AsRef<str>,
{
    let mut from_telemetry =
        stage::Map::<_, TelemetryData, GraphResult<Out>>::new(format!("{}_from_telemetry", name.as_ref()), |data| data.try_into::<Out>());

    let mut filter_failures = stage::FilterMap::new(format!("{}_filter_ok_items", name.as_ref()), |item: GraphResult<Out>| item.ok());

    let cg_inlet = from_telemetry.inlet().clone();
    (from_telemetry.outlet(), filter_failures.inlet()).connect().await;
    let cg_outlet = filter_failures.outlet().clone();
    let mut cg = Graph::default();
    cg.push_back(Box::new(from_telemetry)).await;
    cg.push_back(Box::new(filter_failures)).await;
    let composite = stage::CompositeThrough::<TelemetryData, Out>::new(name.as_ref(), cg, cg_inlet, cg_outlet).await;

    let result: FromTelemetryShape<'de, Out> = Box::new(composite);
    Ok(result)
}
