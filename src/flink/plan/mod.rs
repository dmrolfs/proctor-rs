pub use benchmark::Benchmark;
pub use forecast::*;
pub use performance_history::PerformanceHistory;
pub use performance_repository::{
    PerformanceMemoryRepository, PerformanceRepository, PerformanceRepositorySettings, PerformanceRepositoryType,
};
pub use planning::FlinkScalePlanning;

use crate::flink::decision::result::DecisionResult;
use crate::flink::MetricCatalog;

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

impl FlinkScalePlan {
    pub fn new(
        decision: DecisionResult<MetricCatalog>, required_nr_task_managers: u8, min_scaling_step: u8,
    ) -> Option<Self> {
        use DecisionResult as DR;

        let current_nr_task_managers = decision.item().cluster.nr_task_managers;
        let scale_plan_for =
            |target_nr_task_managers: u8| Some(FlinkScalePlan { target_nr_task_managers, current_nr_task_managers });

        match decision {
            DR::ScaleUp(_) if current_nr_task_managers < required_nr_task_managers => {
                scale_plan_for(required_nr_task_managers)
            },

            DR::ScaleUp(_) => {
                let corrected_nr_task_managers = current_nr_task_managers + min_scaling_step;

                tracing::warn!(
                    calculated_nr_task_managers=%required_nr_task_managers,
                    %current_nr_task_managers,
                    %corrected_nr_task_managers,
                    %min_scaling_step,
                    "scale up calculation was not sufficient - applying minimal scaling step."
                );

                scale_plan_for(corrected_nr_task_managers)
            },

            DR::ScaleDown(_) if required_nr_task_managers < current_nr_task_managers => {
                scale_plan_for(required_nr_task_managers)
            },

            DR::ScaleDown(_) => {
                let corrected_nr_task_managers = if min_scaling_step < current_nr_task_managers {
                    current_nr_task_managers - min_scaling_step
                } else {
                    1
                };

                tracing::warn!(
                    calculated_nr_task_managers=%required_nr_task_managers,
                    %current_nr_task_managers,
                    %min_scaling_step,
                    %corrected_nr_task_managers,
                    "scale up calculation was not sufficient - applying minimal scaling step."
                );

                scale_plan_for(corrected_nr_task_managers)
            },

            DR::NoAction(_) => None,
        }
    }
}
