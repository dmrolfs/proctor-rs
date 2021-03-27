mod fixtures;

use cast_trait_object::DynCastExt;
use proctor::graph::stage::WithApi;
use proctor::graph::{stage, Connect, Graph, SinkShape};
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
const CAT_FIELD: &str = "cat";

/// in this scenario the clearinghouse will skip publishing the second iteration because the
/// subscriber only care about `pos` which is not included.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_basic_1_clearinghouse_subscription() -> anyhow::Result<()> {
    // fixtures::init_tracing("test_basic_1_clearinghouse_subscription");
    let main_span = tracing::info_span!("main");
    let _main_span_guard = main_span.enter();

    let test_focus = maplit::hashset! { POS_FIELD.to_string() };
    let (actual_count, actual_sum) = test_scenario(test_focus).await?;
    assert_eq!(actual_count, 3);
    assert_eq!(actual_sum, 8);
    Ok(())
}

/// in this scenario the clearinghouse will include publishing the second iteration because the
/// subscriber care about `pos` *and* `cat` which is included in the iteration, so both
/// count and sum are greater as a result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_basic_2_clearinghouse_subscription() -> anyhow::Result<()> {
    // fixtures::init_tracing("test_basic_2_clearinghouse_subscription");
    let main_span = tracing::info_span!("main");
    let _main_span_guard = main_span.enter();

    let test_focus = maplit::hashset! { POS_FIELD.to_string(), CAT_FIELD.to_string() };
    let (actual_count, actual_sum) = test_scenario(test_focus).await?;
    assert_eq!(actual_count, 4);
    assert_eq!(actual_sum, 9);
    Ok(())
}

/// returns pos field (count, sum)
async fn test_scenario(focus: HashSet<String>) -> anyhow::Result<(usize, usize)> {
    let base_path = std::env::current_dir()?;
    let cvs_path = base_path.join(PathBuf::from("tests/data/cats.csv"));
    let cvs_setting = SourceSetting::Csv { path: cvs_path };
    let mut cvs = collection::make_telemetry_cvs_source("cvs", &cvs_setting)?;

    let mut clearinghouse = collection::Clearinghouse::new("clearinghouse");

    let pos_stats_fields = focus.clone();
    let mut pos_stats = stage::Fold::<_, collection::TelemetryData, (usize, usize)>::new("pos_stats", (0, 0), move |(count, sum), data| {
        let delivered = data.keys().cloned().collect::<HashSet<_>>();
        let unexpected = delivered.difference(&pos_stats_fields).collect::<HashSet<_>>();
        assert!(unexpected.is_empty());
        let pos = data.get(POS_FIELD).map_or(0, |pos| {
            tracing::warn!(?pos, "parsing pos...");
            pos.parse::<usize>().expect("failed to parse pos field")
        });
        (count + 1, sum + pos)
    });
    let rx_pos_stats = pos_stats.take_final_rx().unwrap();

    clearinghouse.add_subscription("pos", focus, pos_stats.inlet()).await;

    (cvs.outlet(), clearinghouse.inlet()).connect().await;

    let mut g = Graph::default();
    g.push_back(cvs.dyn_upcast()).await;
    g.push_back(Box::new(clearinghouse)).await;
    g.push_back(Box::new(pos_stats)).await;
    g.run().await?;

    rx_pos_stats.await.map_err(|err| err.into())
}
