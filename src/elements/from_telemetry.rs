use super::Telemetry;
use crate::graph::stage::{self, Stage};
use crate::graph::{Connect, Graph, GraphResult, SinkShape, SourceShape, ThroughShape};
use crate::AppData;
use serde::de::DeserializeOwned;

pub type FromTelemetryShape<Out> = Box<dyn FromTelemetryStage<Out>>;

pub trait FromTelemetryStage<Out>: Stage + ThroughShape<In = Telemetry, Out = Out> + 'static {}

impl<Out, T> FromTelemetryStage<Out> for T where T: Stage + ThroughShape<In = Telemetry, Out = Out> + 'static {}

#[tracing::instrument(level = "info", skip(name))]
pub async fn make_from_telemetry<Out, S>(name: S, log_conversion_failure: bool) -> GraphResult<FromTelemetryShape<Out>>
where
    Out: AppData + DeserializeOwned,
    S: AsRef<str>,
{
    let from_telemetry =
        stage::Map::<_, Telemetry, GraphResult<Out>>::new(format!("{}_from_telemetry", name.as_ref()), |telemetry| {
            tracing::trace!(?telemetry, "converting telemetry into data item...");
            let converted = telemetry.try_into::<Out>();
            if let Err(ref err) = converted {
                tracing::error!(error=?err, "failed to convert an entity from telemetry data");
            }
            tracing::trace!(?converted, "data item conversion from telemetry.");
            converted
        });

    let mut filter_failures = stage::FilterMap::new(
        format!("{}_filter_ok_conversions", name.as_ref()),
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
    let composite = stage::CompositeThrough::<Telemetry, Out>::new(name.as_ref(), cg, cg_inlet, cg_outlet).await;

    let result: FromTelemetryShape<Out> = Box::new(composite);
    Ok(result)
}
