use super::{stage::Stage, GraphResult};
use tokio::task::JoinHandle;
use tracing::Instrument;

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
    #[tracing::instrument(
    level="info",
    name="run node",
    skip(self),
    fields(node=%self.name),
    )]
    pub fn run(mut self) -> JoinHandle<GraphResult<()>> {
        tokio::spawn(
            async move {
                let run_result = self.stage.run().instrument(tracing::info_span!("run graph node")).await;
                if let Err(err) = &run_result {
                    tracing::error!(error=?err, "node run failed.");
                }

                let close_result = self
                    .stage
                    .close()
                    .instrument(tracing::info_span!("close graph node"))
                    .await;
                if let Err(err) = &close_result {
                    tracing::error!(error=?err, "node close failed.");
                }

                run_result.and(close_result)
            }
            .instrument(tracing::info_span!("spawn-run-graph-node",)),
        )
    }
}
