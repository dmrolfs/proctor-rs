use proctor::graph::stage::{self, Stage};
use proctor::graph::Connect;
use proctor::graph::{SinkShape, SourceShape};
use proctor::tracing::{get_subscriber, init_subscriber};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing_futures::Instrument;

#[tracing::instrument(name = "performing concurrent operation")]
async fn action(input: Option<i32>) -> Option<String> {
    let i = match input {
        Some(input) => input,
        None => return None,
    };

    tokio::time::sleep(Duration::from_millis(100))
        .instrument(tracing::info_span!("waiting with input", i))
        .await;

    Some(i.to_string())
}

#[tokio::main]
async fn main() {
    let subscriber = get_subscriber("proctor", "info");
    init_subscriber(subscriber);

    let main_span = tracing::info_span!("main");
    let _main_span_guard = main_span.enter();

    let (tx, rx) = mpsc::channel(128);

    tokio::spawn(async move {
        for i in 1..=6 {
            tokio::time::sleep(Duration::from_millis(47)).await;
            let _ = tx.send(i).await;
        }
    });

    let result = do_via_stage(rx).await.recv().await.unwrap();

    tracing::warn!("MAIN FINAL RESULT = {}", result);
}

#[tracing::instrument(name = "stage based example")]
async fn do_via_stage(rx_control: mpsc::Receiver<i32>) -> mpsc::Receiver<String> {
    let mut source = stage::RefreshableSource::new("src", action, rx_control);
    let mut stage = stage::Map::new("mapped-suffix", |x| format!("{}__MAPPED", x));
    (source.outlet(), stage.inlet()).connect().await;

    let (result_tx, result_rx) = mpsc::channel(1);
    stage.outlet().attach(result_tx).await;

    tokio::spawn(async move { stage.run().await });

    tokio::spawn(async move { source.run().instrument(tracing::info_span!("graph source run")).await });

    tracing::info!("after graph source spawn...");
    result_rx
}
