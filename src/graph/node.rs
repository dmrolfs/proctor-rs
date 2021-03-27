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
    fields(name=%self.name),
    )]
    pub fn run(mut self) -> JoinHandle<GraphResult<()>> {
        tokio::spawn(
            async move {
                let run_result = self.stage.run().instrument(tracing::info_span!("run graph node")).await;
                let close_result = self.stage.close().instrument(tracing::info_span!("close graph node")).await;
                run_result.and(close_result)
            }
            .instrument(tracing::info_span!("spawn-run-graph-node",)),
        )
    }

    // pub fn abort(&mut self) -> GraphResult<()> {
    //     self.handle.take().map_or(Ok(()), |h| Ok(h.abort()))
    // }

    // #[tracing::instrument(
    //     level="info",
    //     name="complete node",
    //     skip(self),
    //     fields(name=%self.name),
    // )]
    // pub async fn complete(mut self) -> GraphResult<()> {
    //     match self.handle.take() {
    //         None => Ok(()),
    //         Some(h) => {
    //             tracing::info!(handle=?h, "awaiting on JoinHandle...");
    //             // drop(h);
    //             h.await?; //.expect( format!( "failed to complete node: {}", self.name ).as_str(), )
    //             self.stage.close().await
    //         }
    //     }
    // }
}
