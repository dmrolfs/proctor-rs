mod fixtures;

use cast_trait_object::DynCastExt;
use pretty_assertions::assert_eq;
use proctor::elements::Telemetry;
use proctor::graph::{stage, Connect, Graph, SinkShape, SourceShape};
use proctor::phases::collection;
use proctor::phases::collection::TelemetrySubscription;
use proctor::settings::SourceSetting;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Data {
    #[serde(default)]
    pub pos: Option<i64>,
    #[serde(default)]
    pub value: Option<f64>,
    #[serde(default)]
    pub cat: String,
}

impl Default for Data {
    fn default() -> Self {
        Self { pos: None, value: None, cat: "".to_string() }
    }
}

const POS_FIELD: &str = "pos";
const _VALUE_FIELD: &str = "value";
const CAT_FIELD: &str = "cat";

/// in this scenario the clearinghouse will skip publishing the second iteration because the
/// subscriber only care about `pos` which is not included.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_basic_1_clearinghouse_subscription() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_basic_1_clearinghouse_subscription");
    let _main_span_guard = main_span.enter();

    let d = Data {
        pos: Some(3),
        value: Some(2.71828),
        cat: "Apollo".to_string(),
    };
    let t = Telemetry::try_from(&d);
    tracing::info!(?d, ?t, "telemetry try_from data result.");

    let test_focus = maplit::hashset! { POS_FIELD.to_string() };
    let (actual_count, actual_sum) = test_scenario(test_focus).await?;
    tracing::info!(%actual_count, %actual_sum, "Results are in!");
    assert_eq!(actual_count, 3);
    assert_eq!(actual_sum, 8);
    Ok(())
}

/// in this scenario the clearinghouse will include publishing the second iteration because the
/// subscriber care about `pos` *and* `cat` which is included in the iteration, so both
/// count and sum are greater as a result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_basic_2_clearinghouse_subscription() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_basic_2_clearinghouse_subscription");
    let _main_span_guard = main_span.enter();

    let test_focus = maplit::hashset! { POS_FIELD.to_string(), CAT_FIELD.to_string() };
    let (actual_count, actual_sum) = test_scenario(test_focus).await?;
    assert_eq!(actual_count, 4);
    assert_eq!(actual_sum, 9);
    Ok(())
}

/// returns pos field (count, sum)
async fn test_scenario(focus: HashSet<String>) -> anyhow::Result<(i64, i64)> {
    let base_path = std::env::current_dir()?;
    let cvs_path = base_path.join(PathBuf::from("tests/data/cats.csv"));
    let cvs_setting = SourceSetting::Csv { path: cvs_path };
    let cvs = collection::make_telemetry_cvs_source::<Data, _>("cvs", &cvs_setting)?;

    let mut clearinghouse = collection::Clearinghouse::new("clearinghouse");
    let pos_channel = collection::SubscriptionChannel::<Data>::new("pos_channel").await?;

    let mut pos_stats = stage::Fold::<_, Data, (i64, i64)>::new("pos_stats", (0, 0), move |(count, sum), data| {
        let pos = data.pos.unwrap_or(0);
        (count + 1, sum + pos)
    });
    let rx_pos_stats = pos_stats.take_final_rx().unwrap();

    (cvs.outlet(), clearinghouse.inlet()).connect().await;
    clearinghouse
        .add_subscription(
            TelemetrySubscription::new("pos").with_required_fields(focus),
            &pos_channel.subscription_receiver,
        )
        .await;

    (pos_channel.outlet(), pos_stats.inlet()).connect().await;

    let mut g = Graph::default();
    g.push_back(cvs.dyn_upcast()).await;
    g.push_back(Box::new(clearinghouse)).await;
    g.push_back(Box::new(pos_channel)).await;
    g.push_back(Box::new(pos_stats)).await;
    g.run().await?;

    rx_pos_stats.await.map_err(|err| err.into())
}
