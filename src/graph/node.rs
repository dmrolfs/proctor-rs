use crate::error::{MetricLabel, ProctorError};
use tokio::task::JoinHandle;
use tracing::Instrument;

use super::stage::Stage;
use crate::ProctorResult;
use lazy_static::lazy_static;
use prometheus::{IntCounterVec, Opts};

lazy_static! {
    pub static ref GRAPH_ERRORS: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "proctor_graph_errors",
            "Number of recoverable errors occurring in graph processing"
        ),
        &["stage", "error_type"]
    )
    .expect("failed creating proctor_graph_errors metric");
}

#[inline]
fn track_errors(stage: &str, error: &ProctorError) {
    GRAPH_ERRORS.with_label_values(&[stage, error.label().as_ref()]).inc()
}

#[derive(Debug)]
pub struct Node {
    pub name: String,
    stage: Box<dyn Stage>,
}

impl Node {
    pub fn new(stage: Box<dyn Stage>) -> Self {
        let name = stage.name().to_string();
        Self { name, stage }
    }
}

impl Node {
    pub async fn check(&self) -> ProctorResult<()> {
        self.stage
            .check()
            .instrument(tracing::info_span!("check graph node", node=%self.stage.name()))
            .await?;

        Ok(())
    }

    pub fn run(mut self) -> JoinHandle<ProctorResult<()>> {
        let node_name = self.name.clone();
        tokio::spawn(
            async move {
                let run_result: ProctorResult<()> = loop {
                    match self.stage.run().await {
                        Ok(()) => {
                            tracing::info!("{} node completed and stopped", self.name);
                            break Ok(());
                        }
                        Err(ProctorError::GraphError(err)) => {
                            tracing::error!(error=?err, "Graph error in {} node - stopping", self.name);
                            break Err(err.into());
                        }
                        Err(err) => {
                            track_errors(self.stage.name(), &err);
                            tracing::error!(error=?err, "{} node failed on item - skipping", self.name);
                        }
                    }
                };

                let close_result = self
                    .stage
                    .close()
                    .instrument(tracing::info_span!("close graph node"))
                    .await;

                if let Err(err) = &close_result {
                    tracing::error!(error=?err, "node close failed.");
                }

                run_result.and(close_result)?;
                Ok(())
            }
            .instrument(tracing::info_span!("spawn node", node=%node_name)),
        )
    }
}

#[cfg(test)]
mod tests {
    use claim::*;
    use pretty_assertions::assert_eq;
    use super::*;
    use prometheus::Registry;
    use crate::error::{GraphError, PortError, EligibilityError};

    #[test]
    fn test_track_error_metric() {
        let registry_name = "test_track_error_metric";
        let registry = assert_ok!(Registry::new_custom(Some(registry_name.to_string()), None));
        assert_ok!(registry.register(Box::new(GRAPH_ERRORS.clone())));
        track_errors("foo", &GraphError::PortError(PortError::Detached("detached foo".to_string())).into());
        track_errors("foo", &EligibilityError::DataNotFound("no foo".to_string()).into());
        track_errors("bar", &EligibilityError::ParseError("bad smell".to_string()).into());

        let metric_family = registry.gather();
        assert_eq!(metric_family.len(), 1);
        assert_eq!(metric_family[0].get_name(), &format!("{}_{}", registry_name, "proctor_graph_errors"));
        let metrics = metric_family[0].get_metric();
        assert_eq!(metrics.len(), 3);
        let error_types: Vec<&str> = metrics
            .iter()
            .flat_map(|m| {
                m.get_label()
                    .iter()
                    .filter(|l| l.get_name() == "error_type" )
                    .map(|l| l.get_value())
            })
            // .sorted()
            .collect();
        assert_eq!(
            error_types,
            vec![
                "proctor::eligibility::data_not_found",
                "proctor::eligibility::parse",
                "proctor::graph::port::detached",
            ]
        );
    }
}