use anyhow::Result;
use proctor::graph::{stage, Connect, Graph, SinkShape, SourceShape, UniformFanInShape};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_basic_sequence_3_fan_in_merge() -> Result<()> {
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    // fixtures::init_tracing("test_basic_sequence_3_fan_in_merge");
    let main_span = tracing::info_span!("test_basic_sequence_3_fan_in_merge");
    let _main_span_guard = main_span.enter();

    let src_0 = stage::Sequence::new("src_ONES", 1..=9);
    let src_1 = stage::Sequence::new("src_TENS", 11..=99);
    let src_2 = stage::Sequence::new("src_HUNDREDS", 101..=999);

    let merge = stage::MergeN::new("merge", 3);

    (&src_0.outlet(), &merge.inlets().get(0).await.unwrap()).connect().await;
    (&src_1.outlet(), &merge.inlets().get(1).await.unwrap()).connect().await;
    (&src_2.outlet(), &merge.inlets().get(2).await.unwrap()).connect().await;

    let mut sum = stage::Fold::new("sum", 0, |acc, x| acc + x);
    let rx_sum = sum.take_final_rx().unwrap();
    (merge.outlet(), sum.inlet()).connect().await;

    let mut g = Graph::default();
    g.push_back(Box::new(src_0)).await;
    g.push_back(Box::new(src_1)).await;
    g.push_back(Box::new(src_2)).await;
    g.push_back(Box::new(merge)).await;
    g.push_back(Box::new(sum)).await;
    g.run().await?;

    match rx_sum.await {
        Ok(actual) => {
            let e1: i32 = (1..=9).into_iter().sum();
            let e10: i32 = (11..=99).into_iter().sum();
            let e100: i32 = (101..=999).into_iter().sum();
            assert_eq!(actual, e1 + e10 + e100);
            Ok(())
        },
        Err(_err) => panic!("failed to receive final sum"),
    }
}
