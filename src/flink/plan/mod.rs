pub use benchmark::Benchmark;

mod appraisal;
mod benchmark;
mod forecast;
mod planning;

#[derive(Debug, Clone, PartialEq)]
pub struct FlinkScalePlan {}
