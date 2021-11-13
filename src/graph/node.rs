use tokio::task::JoinHandle;
use tracing::Instrument;

use super::stage::Stage;
use crate::error::ProctorError;
use crate::graph;
use crate::ProctorResult;

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
                        },
                        Err(ProctorError::GraphError(err)) => {
                            tracing::error!(error=?err, "Graph error in {} node - stopping", self.name);
                            break Err(err.into());
                        },
                        Err(err) => {
                            graph::track_errors(self.stage.name(), &err);
                            tracing::error!(error=?err, "{} node failed on item - skipping", self.name);
                        },
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
