use anyhow::Result;
use cast_trait_object::DynCastExt;
use proctor::elements;
use proctor::graph::stage;
use proctor::graph::{Connect, Graph};
use proctor::graph::{SinkShape};
use proctor::phases::collection;
use proctor::settings::SourceSetting;
use proctor::telemetry::{get_subscriber, init_subscriber};
use std::collections::HashSet;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq)]
struct Data {
    pub pos: Option<usize>,
    pub value: Option<f64>,
    pub cat: String,
}

impl Default for Data {
    fn default() -> Self {
        Self {
            pos: None,
            value: None,
            cat: "".to_string(),
        }
    }
}

const POS_FIELD: &str = "pos";
const _VALUE_FIELD: &str = "value";
const _CAT_FIELD: &str = "cat";

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() -> Result<()> {
    let subscriber = get_subscriber("sandbox", "trace");
    init_subscriber(subscriber);

    let main_span = tracing::info_span!("main");
    let _main_span_guard = main_span.enter();

    let base_path = std::env::current_dir()?;
    let cvs_path = base_path.join(PathBuf::from("tests/data/cats.csv"));
    let cvs_setting = SourceSetting::Csv { path: cvs_path };
    let mut cvs = collection::make_telemetry_cvs_source("cvs", &cvs_setting)?;

    let mut clearinghouse = collection::Clearinghouse::new("clearinghouse");

    let pos_stats_fields = maplit::hashset! { POS_FIELD.to_string() };
    let mut pos_stats = stage::Fold::<_, elements::TelemetryData, (usize, usize)>::new("pos_stats", (0, 0), move |(count, sum), data| {
        let delivered = data.keys().cloned().collect::<HashSet<_>>();
        let allowed = &pos_stats_fields;
        let unexpected = delivered.difference(allowed).collect::<HashSet<_>>();
        if !unexpected.is_empty() {
            tracing::error!(?unexpected, "fields delivered beyond allowed.");
            panic!("fields fields beyond allowed: {:?}", unexpected);
        }
        let pos = data.get(POS_FIELD).unwrap().parse::<usize>().expect("failed to parse pos field");
        (count + 1, sum + pos)
    });
    let rx_pos_stats = pos_stats.take_final_rx().unwrap();

    clearinghouse
        .add_subscription("pos", maplit::hashset! { POS_FIELD.to_string() }, pos_stats.inlet())
        .await;

    // let (mut sink, _, rx_acc) =
    //     stage::Fold::<_, collection::TelemetryData, (Data, usize)>::new("sink", (Data::default(), 0), |(acc, count), rec: TelemetryData| {
    //         let dt_format = "%+";
    //         let rec_last_failure = rec.get("last_failure").and_then(|r| {
    //             if r.is_empty() {
    //                 None
    //             } else {
    //                 let lf = DateTime::parse_from_str(r.as_str(), dt_format).unwrap().with_timezone(&Utc);
    //                 Some(lf)
    //             }
    //         });
    //
    //         let is_deploying = rec.get("is_deploying").unwrap().as_str().parse::<bool>().unwrap();
    //
    //         let rec_latest_deployment = DateTime::parse_from_str(rec.get("last_deployment").unwrap().as_str(), dt_format)
    //             .unwrap()
    //             .with_timezone(&Utc);
    //
    //         let last_failure = match (acc.last_failure, rec_last_failure) {
    //             (None, None) => None,
    //             (Some(a), None) => Some(a),
    //             (None, Some(r)) => Some(r),
    //             (Some(a), Some(r)) if a < r => Some(r),
    //             (Some(a), _) => Some(a),
    //         };
    //
    //         let latest_deployment = if acc.latest_deployment < rec_latest_deployment {
    //             rec_latest_deployment
    //         } else {
    //             acc.latest_deployment
    //         };
    //
    //         (
    //             Data {
    //                 last_failure,
    //                 is_deploying,
    //                 latest_deployment,
    //             },
    //             count + 1,
    //         )
    //     });

    (cvs.outlet(), clearinghouse.inlet()).connect().await;

    let mut g = Graph::default();
    g.push_back(cvs.dyn_upcast()).await;
    g.push_back(Box::new(clearinghouse)).await;
    g.push_back(Box::new(pos_stats)).await;

    // let (tx_stop, rx_stop) = oneshot::channel();
    // let stop_handle = tokio::spawn(async move {
    //     tracing::warn!("waiting 51ms to stop...");
    //     tokio::time::sleep(Duration::from_millis(51)).await;
    //
    //     tracing::warn!("stopping tick source...");
    //     tx_tick.send(tick::TickMsg::Stop { tx: tx_stop }).expect("failed to send tick stop");
    // });

    g.run().await?;

    // stop_handle.await?;
    // let _ = rx_stop.await??;
    // tracing::warn!("tick source stop acknowledged");

    // g.complete().await?;

    let (actual_count, actual_sum) = rx_pos_stats.await?;
    assert_eq!(actual_count, 3);
    assert_eq!(actual_sum, 8);

    Ok(())
}
