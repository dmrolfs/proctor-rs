use pretty_snowflake::Label;
use serde::de::DeserializeOwned;

use super::Telemetry;
use crate::error::ProctorError;
use crate::graph::stage::{self, Stage};
use crate::graph::{self, Graph, SinkShape, SourceShape, ThroughShape};
use crate::AppData;
use crate::DataSet;

pub type FromTelemetryShape<Out> = Box<dyn FromTelemetryStage<Out>>;

pub trait FromTelemetryStage<Out>: Stage + ThroughShape<In = DataSet<Telemetry>, Out = DataSet<Out>> + 'static
where
    Out: Label,
{
}

impl<Out, T> FromTelemetryStage<Out> for T
where
    T: Stage + ThroughShape<In = DataSet<Telemetry>, Out = DataSet<Out>> + 'static,
    Out: Label,
{
}

#[tracing::instrument(level = "trace", skip(name))]
pub async fn make_from_telemetry<Out>(name: &str, log_conversion_failure: bool) -> FromTelemetryShape<Out>
where
    Out: AppData + Label + DeserializeOwned,
{
    let name_2 = name.to_string();
    let err_name = name.to_string();
    let from_telemetry = stage::FilterMap::<_, DataSet<Telemetry>, DataSet<Out>>::new(
        format!("{}_from_telemetry", name),
        move |telemetry| {
            let span = tracing::trace_span!("converting telemetry into data item", ?telemetry);
            let _ = span.enter();

            match telemetry.map(|t| t.try_into()).transpose() {
                Ok(converted) => {
                    tracing::trace!(?converted, "data item derived from telemetry.");
                    Some(converted)
                },
                Err(err) => {
                    if log_conversion_failure {
                        tracing::error!(error=?err, "failed to convert telemetry data into entity in: {}", err_name);
                    }

                    graph::track_errors(&err_name, &ProctorError::SensePhase(err.into()));
                    None
                },
            }
        },
    );

    let cg_inlet = from_telemetry.inlet().clone();
    let cg_outlet = from_telemetry.outlet().clone();
    let mut cg = Graph::default();
    cg.push_back(Box::new(from_telemetry)).await;

    Box::new(stage::CompositeThrough::new(&name_2, cg, cg_inlet, cg_outlet).await)
}
