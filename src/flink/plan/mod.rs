pub use performance_history::PerformanceHistory;
pub use performance_repository::{
    PerformanceRepository, PerformanceRepositoryType, PerformanceRepositorySettings, PerformanceMemoryRepository,
};
pub use benchmark::Benchmark;
pub use forecast::*;
pub use planning::FlinkScalePlanning;

mod performance_history;
mod performance_repository;
mod benchmark;
mod forecast;
mod planning;

#[derive(Debug, Clone, PartialEq)]
pub struct FlinkScalePlan {
    target_nr_task_managers: u8,
    current_nr_task_managers: u8,
}
