pub use benchmark::Benchmark;

mod benchmark;
mod forecast;
mod performance;
mod planning;

#[derive(Debug, Clone, PartialEq)]
pub struct FlinkScalePlan {}
