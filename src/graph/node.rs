use super::stage::Stage;
use anyhow::Result;
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
    // #[tracing::instrument( level="info", skip(self), fields(node=%self.name),)]
    // pub fn prepare_for_run(mut self) -> JoinHandle<GraphResult<Self>> {
    //     tokio::spawn(
    //         async move {
    //             self.stage
    //                 .prepare_for_run()
    //                 .instrument(tracing::info_span!("prepare graph node for run"))
    //                 .await
    //                 .map_err(|err| {
    //                     tracing::error!(error=?err, "node failed to prepare for run.");
    //                     err
    //                 })
    //                 .map(|_| self)
    //         }
    //         .instrument(tracing::info_span!("spawn prepare graph node for run",)),
    //     )
    // }

    pub async fn check(&self) -> Result<()> {
        self.stage
            .check()
            .instrument(tracing::info_span!("check graph node", node=%self.stage.name()))
            .await
    }

    pub fn run(mut self) -> JoinHandle<Result<()>> {
        let name = self.name.clone();
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
            .instrument(tracing::info_span!("spawn node", node=%name)),
        )
    }
}
