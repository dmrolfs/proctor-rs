use serde::de::DeserializeOwned;

use super::Telemetry;
use crate::graph::stage::{self, Stage};
use crate::graph::{Graph, SinkShape, SourceShape, ThroughShape};
use crate::AppData;

pub type FromTelemetryShape<Out> = Box<dyn FromTelemetryStage<Out>>;

pub trait FromTelemetryStage<Out>: Stage + ThroughShape<In = Telemetry, Out = Out> + 'static {}

impl<Out, T> FromTelemetryStage<Out> for T where T: Stage + ThroughShape<In = Telemetry, Out = Out> + 'static {}

#[tracing::instrument(level = "info", skip(name))]
pub async fn make_from_telemetry<Out>(name: impl Into<String>, log_conversion_failure: bool) -> FromTelemetryShape<Out>
where
    Out: AppData + DeserializeOwned,
{
    let name = name.into();
    let from_telemetry =
        stage::FilterMap::<_, Telemetry, Out>::new(format!("{}_from_telemetry", name), move |telemetry| {
            let span = tracing::info_span!("converting telemetry into data item", ?telemetry);
            let _ = span.enter();

            let converted = telemetry.try_into::<Out>();
            if log_conversion_failure {
                if let Err(ref err) = converted {
                    tracing::error!(error=?err, "failed to convert an entity from telemetry data");
                }
            }
            tracing::trace!(?converted, "data item derived from telemetry.");
            converted.ok()
        });

    let cg_inlet = from_telemetry.inlet().clone();
    let cg_outlet = from_telemetry.outlet().clone();
    let mut cg = Graph::default();
    cg.push_back(Box::new(from_telemetry)).await;

    Box::new(stage::CompositeThrough::new(name, cg, cg_inlet, cg_outlet).await)
}
