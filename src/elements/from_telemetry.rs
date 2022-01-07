use serde::de::DeserializeOwned;

use super::Telemetry;
use crate::error::ProctorError;
use crate::graph::stage::{self, Stage};
use crate::graph::{self, Graph, SinkShape, SourceShape, ThroughShape};
use crate::{AppData, SharedString};

pub type FromTelemetryShape<Out> = Box<dyn FromTelemetryStage<Out>>;

pub trait FromTelemetryStage<Out>: Stage + ThroughShape<In = Telemetry, Out = Out> + 'static {}

impl<Out, T> FromTelemetryStage<Out> for T where T: Stage + ThroughShape<In = Telemetry, Out = Out> + 'static {}

#[tracing::instrument(level = "info", skip(name))]
pub async fn make_from_telemetry<Out>(name: impl Into<String>, log_conversion_failure: bool) -> FromTelemetryShape<Out>
where
    Out: AppData + DeserializeOwned,
{
    let name: SharedString = SharedString::Owned(name.into());
    let stage_name = name.clone();
    let from_telemetry =
        stage::FilterMap::<_, Telemetry, Out>::new(format!("{}_from_telemetry", name), move |telemetry| {
            let span = tracing::info_span!("converting telemetry into data item", ?telemetry);
            let _ = span.enter();

            match telemetry.try_into() {
                Ok(converted) => {
                    tracing::trace!(?converted, "data item derived from telemetry.");
                    Some(converted)
                }
                Err(err) => {
                    if log_conversion_failure {
                        tracing::error!(error=?err, "failed to convert an entity from telemetry data");
                    }

                    graph::track_errors(stage_name.as_ref(), &ProctorError::CollectionError(err.into()));
                    None
                }
            }
        });

    let cg_inlet = from_telemetry.inlet().clone();
    let cg_outlet = from_telemetry.outlet().clone();
    let mut cg = Graph::default();
    cg.push_back(Box::new(from_telemetry)).await;

    Box::new(stage::CompositeThrough::new(name.into_owned(), cg, cg_inlet, cg_outlet).await)
}
