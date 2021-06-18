use anyhow::Result;
use criterion::{black_box, criterion_group, Criterion};
use proctor::graph::stage::{self, tick};
use proctor::graph::{Connect, Graph, SinkShape, SourceShape, UniformFanInShape};
use std::time::Duration;
use tokio::sync::oneshot;

async fn make_graph() -> (Graph, oneshot::Receiver<i32>) {
    let src_0 = stage::Tick::with_constraint(
        "src_ONES",
        Duration::from_nanos(0),
        Duration::from_nanos(1),
        1,
        tick::Constraint::by_count(10),
    );

    let tick_1 = stage::Tick::with_constraint(
        "src_TENS",
        Duration::from_nanos(0),
        Duration::from_nanos(1),
        "10",
        tick::Constraint::by_count(100),
    );
    let map_1 = stage::Map::new("map_1", |s: &str| s.parse::<i32>().unwrap());

    let tick_2 = stage::Tick::with_constraint(
        "src_HUNDREDS",
        Duration::from_nanos(0),
        Duration::from_nanos(1),
        Duration::from_millis(100),
        tick::Constraint::by_count(1000),
    );
    let map_2 = stage::Map::new("map_2", |d: Duration| d.as_millis() as i32);

    let tick_3 = stage::Tick::with_constraint(
        "src_SEVENS",
        Duration::from_nanos(0),
        Duration::from_nanos(1),
        3,
        tick::Constraint::by_count(777),
    );
    let map_3 = stage::Map::new("map_3", |o| o + 4);

    let tick_4 = stage::Tick::with_constraint(
        "src_13",
        Duration::from_nanos(0),
        Duration::from_nanos(1),
        (),
        tick::Constraint::by_count(13),
    );
    let map_4 = stage::Map::new("map_4", |()| 13);

    let merge = stage::MergeN::new("merge", 5);

    (tick_1.outlet(), map_1.inlet()).connect().await;
    (tick_2.outlet(), map_2.inlet()).connect().await;
    (tick_3.outlet(), map_3.inlet()).connect().await;
    (tick_4.outlet(), map_4.inlet()).connect().await;

    (&src_0.outlet(), &merge.inlets().get(0).await.unwrap()).connect().await;
    (&map_1.outlet(), &merge.inlets().get(1).await.unwrap()).connect().await;
    (&map_2.outlet(), &merge.inlets().get(2).await.unwrap()).connect().await;
    (&map_3.outlet(), &merge.inlets().get(3).await.unwrap()).connect().await;
    (&map_4.outlet(), &merge.inlets().get(4).await.unwrap()).connect().await;

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

    (g, rx_sum)
}

async fn run_scenario(g: Graph, rx_sum: oneshot::Receiver<i32>) -> Result<i32> {
    g.run().await?;
    let actual = rx_sum.await?;

    let e1 = 1 * 10;
    let e10 = 10 * 100;
    let e100 = 100 * 1000;
    let e7 = 7 * 777;
    let e13 = 13 * 13;
    assert_eq!(actual, e1 + e10 + e100 + e7 + e13);
    Ok(actual)
}

//todo: iter_batched() requires synchronous setup closure, so can't use
// fn benchmark_merge_5(c: &mut Criterion) {
//     c.bench_function("merge_5_fan_in", |b| {
//         let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
//         b.to_async(rt).iter_batched(
//             || {
//                 make_graph() },
//             |(g, rx_sum)| async {
//                 black_box( run_scenario(g, rx_sum).await.expect("scenario failed") );
//             },
//             criterion::BatchSize::SmallInput,
//         )
//     });
// }

fn benchmark_merge_5(c: &mut Criterion) {
    c.bench_function("merge_5_fan_in", move |b| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        b.to_async(rt).iter(|| async {
            let (g, rx_sum) = make_graph().await;
            black_box(run_scenario(g, rx_sum).await.expect("scenario failed"));
        })
    });
}

// criterion_group!(merge, benchmark_merge_5);

criterion_group! {
    name = merge;
    config = Criterion::default().with_profiler(super::super::profiler::FlamegraphProfiler::new(100));
    targets = benchmark_merge_5
}
