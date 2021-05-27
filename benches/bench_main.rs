use criterion::criterion_main;

mod benchmarks;
pub mod profiler;

criterion_main! {
    benchmarks::merge::merge,
}
