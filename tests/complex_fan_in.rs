mod fixtures;

use anyhow::Result;
use proctor::graph::stage::{self, tick};
use proctor::graph::{Connect, Graph, SinkShape, SourceShape, UniformFanInShape};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_complex_multi_stage_merge_5() -> Result<()> {
    lazy_static::initialize(&proctor::telemetry::TEST_TRACING);
    // fixtures::init_tracing("test_complex_multi_stage_merge_5");
    let main_span = tracing::info_span!("test_complex_multi_stage_merge_5");
    let _main_span_guard = main_span.enter();

    let mut src_0 = stage::Tick::with_constraint(
        "src_ONES",
        Duration::from_nanos(0),
        Duration::from_nanos(1),
        1,
        tick::Constraint::by_count(10),
    );

    let mut tick_1 = stage::Tick::with_constraint(
        "src_TENS",
        Duration::from_nanos(0),
        Duration::from_nanos(1),
        "10",
        tick::Constraint::by_count(100),
    );
    let mut map_1 = stage::Map::new("map_1", |s: &str| s.parse::<i32>().unwrap());

    let mut tick_2 = stage::Tick::with_constraint(
        "src_HUNDREDS",
        Duration::from_nanos(0),
        Duration::from_nanos(1),
        Duration::from_millis(100),
        tick::Constraint::by_count(1000),
    );
    let mut map_2 = stage::Map::new("map_2", |d: Duration| d.as_millis() as i32);

    let mut tick_3 = stage::Tick::with_constraint(
        "src_SEVENS",
        Duration::from_nanos(0),
        Duration::from_nanos(1),
        3,
        tick::Constraint::by_count(777),
    );
    let mut map_3 = stage::Map::new("map_3", |o| o + 4);

    let mut tick_4 = stage::Tick::with_constraint(
        "src_13",
        Duration::from_nanos(0),
        Duration::from_nanos(1),
        (),
        tick::Constraint::by_count(13),
    );
    let mut map_4 = stage::Map::new("map_4", |()| 13);

    let mut merge = stage::MergeN::new("merge", 5);

    (tick_1.outlet(), map_1.inlet()).connect().await;
    (tick_2.outlet(), map_2.inlet()).connect().await;
    (tick_3.outlet(), map_3.inlet()).connect().await;
    (tick_4.outlet(), map_4.inlet()).connect().await;

    (src_0.outlet(), &merge.inlets().get(0).await.unwrap()).connect().await;
    (map_1.outlet(), &merge.inlets().get(1).await.unwrap()).connect().await;
    (map_2.outlet(), &merge.inlets().get(2).await.unwrap()).connect().await;
    (map_3.outlet(), &merge.inlets().get(3).await.unwrap()).connect().await;
    (map_4.outlet(), &merge.inlets().get(4).await.unwrap()).connect().await;

    let mut sum = stage::Fold::new("sum", 0, |acc, x| acc + x);
    let rx_sum = sum.take_final_rx().unwrap();
    (merge.outlet(), sum.inlet()).connect().await;

    let mut g = Graph::default();
    g.push_back(Box::new(src_0)).await;
    g.push_back(Box::new(tick_1)).await;
    g.push_back(Box::new(tick_2)).await;
    g.push_back(Box::new(tick_3)).await;
    g.push_back(Box::new(tick_4)).await;
    g.push_back(Box::new(map_1)).await;
    g.push_back(Box::new(map_2)).await;
    g.push_back(Box::new(map_3)).await;
    g.push_back(Box::new(map_4)).await;
    g.push_back(Box::new(merge)).await;
    g.push_back(Box::new(sum)).await;
    g.run().await?;

    match rx_sum.await {
        Ok(actual) => {
            let e1 = 1 * 10;
            let e10 = 10 * 100;
            let e100 = 100 * 1000;
            let e7 = 7 * 777;
            let e13 = 13 * 13;
            assert_eq!(actual, e1 + e10 + e100 + e7 + e13);
            Ok(())
        }
        Err(_err) => panic!("failed to receive final sum"),
    }
}
