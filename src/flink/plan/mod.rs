pub use benchmark::Benchmark;
pub use forecast::*;
pub use performance_history::PerformanceHistory;
pub use performance_repository::{
    PerformanceMemoryRepository, PerformanceRepository, PerformanceRepositorySettings, PerformanceRepositoryType,
};
pub use planning::FlinkScalePlanning;

mod benchmark;
pub mod forecast;
mod performance_history;
mod performance_repository;
mod planning;

#[derive(Debug, Clone, PartialEq)]
pub struct FlinkScalePlan {
    target_nr_task_managers: u8,
    current_nr_task_managers: u8,
}
