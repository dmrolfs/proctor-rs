use serde::de::DeserializeOwned;

use super::Telemetry;
use crate::graph::stage::{self, Stage};
use crate::graph::{Graph, SinkShape, SourceShape, ThroughShape};
use crate::AppData;

pub type FromTelemetryShape<Out> = Box<dyn FromTelemetryStage<Out>>;

pub trait FromTelemetryStage<Out>: Stage + ThroughShape<In = Telemetry, Out = Out> + 'static {}

impl<Out, T> FromTelemetryStage<Out> for T where T: Stage + ThroughShape<In = Telemetry, Out = Out> + 'static {}

#[tracing::instrument(level = "info", skip(name), fields(stage_name=%name.as_ref()))]
pub async fn make_from_telemetry<Out>(name: impl AsRef<str>, log_conversion_failure: bool) -> FromTelemetryShape<Out>
where
    Out: AppData + DeserializeOwned,
{
    // let from_telemetry = stage::Map::<_, Telemetry, Result<Out, TelemetryError>>::new(
    //     format!("{}_convert_telemetry", name.as_ref()),
    //     |telemetry| {
    //         tracing::trace!(?telemetry, "converting telemetry into data item...");
    //         let converted = telemetry.try_into::<Out>();
    //         if let Err(ref err) = converted {
    //             tracing::error!(error=?err, "failed to convert an entity from telemetry data");
    //         }
    //         tracing::trace!(?converted, "data item conversion from telemetry.");
    //         converted
    //     },
    // );
    //
    // let mut filter_failures = stage::FilterMap::new(
    //     format!("{}_filter_ok_conversions", name.as_ref()),
    //     |item: Result<Out, TelemetryError>| item.ok(),
    // );
    // if log_conversion_failure {
    //     filter_failures = filter_failures.with_block_logging();
    // }

    let from_telemetry_2 =
        stage::FilterMap::<_, Telemetry, Out>::new(format!("{}_from_telemetry", name.as_ref()), move |telemetry| {
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

    // let cg_inlet = from_telemetry.inlet().clone();
    // (from_telemetry.outlet(), filter_failures.inlet()).connect().await;
    // let cg_outlet = filter_failures.outlet().clone();
    let cg_inlet = from_telemetry_2.inlet().clone();
    // (from_telemetry.outlet(), filter_failures.inlet()).connect().await;
    let cg_outlet = from_telemetry_2.outlet().clone();
    let mut cg = Graph::default();
    // cg.push_back(Box::new(from_telemetry)).await;
    // cg.push_back(Box::new(filter_failures)).await;
    cg.push_back(Box::new(from_telemetry_2)).await;

    Box::new(stage::CompositeThrough::new(name.as_ref(), cg, cg_inlet, cg_outlet).await)
}
