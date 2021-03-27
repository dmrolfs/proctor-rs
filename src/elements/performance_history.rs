pub struct PerformanceHistory {
    pub benchmarks: Vec<Benchmark>,
}

pub struct Benchmark {
    task_managers: u16,
    records_out_per_sec: f32,
}
