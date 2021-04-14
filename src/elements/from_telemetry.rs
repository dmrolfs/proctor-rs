use super::TelemetryData;
use crate::graph::stage::{self, Stage};
use crate::graph::{Connect, Graph, GraphResult, SinkShape, SourceShape, ThroughShape};
use crate::AppData;
use serde::de::DeserializeOwned;

pub type FromTelemetryShape<Out> = Box<dyn FromTelemetryStage<Out>>;

pub trait FromTelemetryStage<Out>: Stage + ThroughShape<In = TelemetryData, Out = Out> + 'static {}

impl<Out, T> FromTelemetryStage<Out> for T where T: Stage + ThroughShape<In = TelemetryData, Out = Out> + 'static {}

#[tracing::instrument(level = "info", skip(name))]
pub async fn make_from_telemetry<Out, S>(name: S, log_conversion_failure: bool) -> GraphResult<FromTelemetryShape<Out>>
where
    Out: AppData + Sync + DeserializeOwned,
    S: AsRef<str>,
{
    let from_telemetry =
        stage::Map::<_, TelemetryData, GraphResult<Out>>::new(format!("{}_from_telemetry", name.as_ref()), |data| {
            tracing::trace!("data item: {:?}", data);
            data.try_into::<Out>()
        });

    let mut filter_failures = stage::FilterMap::new(
        format!("{}_filter_ok_items", name.as_ref()),
        |item: GraphResult<Out>| item.ok(),
    );
    if log_conversion_failure {
        filter_failures = filter_failures.with_block_logging();
    }

    let cg_inlet = from_telemetry.inlet().clone();
    (from_telemetry.outlet(), filter_failures.inlet()).connect().await;
    let cg_outlet = filter_failures.outlet().clone();
    let mut cg = Graph::default();
    cg.push_back(Box::new(from_telemetry)).await;
    cg.push_back(Box::new(filter_failures)).await;
    let composite = stage::CompositeThrough::<TelemetryData, Out>::new(name.as_ref(), cg, cg_inlet, cg_outlet).await;

    let result: FromTelemetryShape<Out> = Box::new(composite);
    Ok(result)
}
