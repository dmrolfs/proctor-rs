use std::fmt::{self, Debug};
use std::time::Duration;

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;

use crate::error::PlanError;
use crate::flink::decision::result::DecisionResult;
use crate::flink::plan::{
    PerformanceHistory, PerformanceRepository, FlinkScalePlan, ForecastCalculator, RecordsPerSecond, TimestampSeconds,
    WorkloadForecast, WorkloadForecastBuilder, WorkloadMeasurement,
};
use crate::flink::MetricCatalog;
use crate::graph::stage::Stage;
use crate::graph::{Inlet, Outlet, Port, SinkShape, SourceShape};
use crate::phases::plan::DataDecisionStage;
use crate::ProctorResult;

pub struct FlinkScalePlanning<F: WorkloadForecastBuilder> {
    name: String,
    inlet: Inlet<MetricCatalog>,
    decision_inlet: Inlet<DecisionResult<MetricCatalog>>,
    outlet: Outlet<FlinkScalePlan>,
    forecast_calculator: ForecastCalculator<F>,
    performance_repository: Box<dyn PerformanceRepository>,
}

impl<F: WorkloadForecastBuilder> FlinkScalePlanning<F> {
    #[tracing::instrument(level = "info", skip(name))]
    pub fn new(
        name: &str, restart_duration: Duration, max_catch_up_duration: Duration, recovery_valid_offset: Duration,
        forecast_builder: F, performance_repository: Box<dyn PerformanceRepository>,
    ) -> Result<Self, PlanError> {
        let name = name.to_string();
        let inlet = Inlet::new(name.clone());
        let decision_inlet = Inlet::new(format!("decision_{}", name.clone()));
        let outlet = Outlet::new(name.clone());
        let forecast_calculator = ForecastCalculator::new(
            forecast_builder,
            restart_duration,
            max_catch_up_duration,
            recovery_valid_offset,
        )?;

        Ok(Self {
            name,
            inlet,
            decision_inlet,
            outlet,
            forecast_calculator,
            performance_repository,
        })
    }
}

impl<F: WorkloadForecastBuilder> Debug for FlinkScalePlanning<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlinkScalePlanning")
            .field("name", &self.name)
            .field("inlet", &self.inlet)
            .field("decision_inlet", &self.decision_inlet)
            .field("outlet", &self.outlet)
            .field("forecast_calculator", &self.forecast_calculator)
            .field("performance_repository", &self.performance_repository)
            .finish()
    }
}

impl<F: WorkloadForecastBuilder> SourceShape for FlinkScalePlanning<F> {
    type Out = FlinkScalePlan;

    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

impl<F: WorkloadForecastBuilder> SinkShape for FlinkScalePlanning<F> {
    type In = MetricCatalog;

    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

impl<F: 'static + WorkloadForecastBuilder> DataDecisionStage for FlinkScalePlanning<F> {
    type Decision = DecisionResult<MetricCatalog>;

    fn decision_inlet(&self) -> Inlet<Self::Decision> {
        self.decision_inlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<F: 'static + WorkloadForecastBuilder> Stage for FlinkScalePlanning<F> {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run Flink planning phase", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        self.do_run().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        self.do_close().await?;
        Ok(())
    }
}

impl<F: WorkloadForecastBuilder> FlinkScalePlanning<F> {
    #[inline]
    async fn do_check(&self) -> Result<(), PlanError> {
        self.inlet.check_attachment().await?;
        self.decision_inlet.check_attachment().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    #[inline]
    async fn do_run(&mut self) -> Result<(), PlanError> {
        use crate::flink::decision::result::DecisionResult as DR;

        let name = self.name.clone();
        let outlet = &self.outlet;
        let rx_data = &mut self.inlet;
        let rx_decision = &mut self.decision_inlet;
        let calculator = &mut self.forecast_calculator;
        let performance_repository = &mut self.performance_repository;

        let mut performance_history = performance_repository
            .load(name.as_str())
            .await?
            .unwrap_or(PerformanceHistory::default());

        loop {
            tokio::select! {
                Some(data) = rx_data.recv() => {
                    calculator.add_observation(data.into());
                },

                Some(decision) = rx_decision.recv() => {
                    Self::update_performance_history(&decision, name.as_str(), &mut performance_history, performance_repository).await?;

                    let _foo: () = if let DR::NoAction(ref metrics) = decision {
                        Self::handle_do_not_scale_decision(metrics)?
                    } else {
                        Self::handle_scale_decision(decision, calculator, &mut performance_history, outlet).await?
                    };
                },

                else => {
                    tracing::info!("Flink scale planning done - breaking...");
                    break;
                },
            }
        }

        Ok(())
    }

    // #[tracing::instrument(level = "info", skip(data))]
    // fn predict_anticipated_workload(
    //     data: MetricCatalog, forecast: &mut ForecastCalculator<F>,
    // ) -> Result<Option<RecordsPerSecond>, PlanError> {
    //     forecast.add_observation(data.into());
    //     let result = match forecast..predict_next_workload()? {
    //         Workload::RecordsInPerSecond(rips) => Some(rips),
    //
    //         Workload::NotEnoughData => {
    //             let (nr_required, nr_needed) = forecast.observations_needed();
    //             tracing::debug!(
    //                 %nr_required, %nr_needed,
    //                 "not enough data points to make workload prediction - need {} more observations
    // to make predictions.",                 nr_needed
    //             );
    //             None
    //         },
    //
    //         Workload::HeuristicsExceedThreshold {} => {
    //             tracing::warn!("forecast algorithm exceeded heuristics threshold - clearing");
    //             forecast.clear();
    //             None
    //         },
    //     };
    //
    //     Ok(result)
    // }

    #[tracing::instrument(level = "info", skip())]
    async fn update_performance_history(
        decision: &DecisionResult<MetricCatalog>, name: &str, performance_history: &mut PerformanceHistory,
        repository: &mut Box<dyn PerformanceRepository>,
    ) -> Result<(), PlanError> {
        use crate::flink::decision::result::DecisionResult as DR;

        let update_repository = match decision {
            DR::NoAction(_) => false,
            DR::ScaleUp(metrics) => {
                performance_history.add_upper_benchmark(metrics.into());
                true
            },
            DR::ScaleDown(metrics) => {
                performance_history.add_lower_benchmark(metrics.into());
                true
            },
        };

        if update_repository {
            repository.save(name, performance_history).await?;
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(_metrics))]
    fn handle_do_not_scale_decision(_metrics: &MetricCatalog) -> Result<(), PlanError> {
        tracing::debug!("Decision made not scale cluster up or down.");
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(outlet))]
    async fn handle_scale_decision(
        decision: DecisionResult<MetricCatalog>,
        calculator: &mut ForecastCalculator<F>,
        performance_history: &mut PerformanceHistory,
        outlet: &Outlet<FlinkScalePlan>,
    ) -> Result<(), PlanError> {
        let current_nr_task_managers = decision.item().cluster.nr_task_managers;
        let buffered_records = decision.item().flow.input_consumer_lag; //todo: how to support other options
        let anticipated_workload = calculator.calculate_target_rate(buffered_records)?;
        let required_nr_task_managers = performance_history.cluster_size_for_workload(anticipated_workload);
        if required_nr_task_managers != current_nr_task_managers {
            tracing::info!(%required_nr_task_managers, %current_nr_task_managers, "scaling plan is to adjust nr task managers to {}", required_nr_task_managers);
            let plan = FlinkScalePlan {
                target_nr_task_managers: required_nr_task_managers,
                current_nr_task_managers,
            };

            outlet.send(plan).await?;
        } else {
            tracing::warn!(%required_nr_task_managers, %current_nr_task_managers, "performance history suggests no change in cluster size needed.");
            //todo: should we clear some of the history????
        }

        Ok(())
    }

    #[inline]
    async fn do_close(mut self: Box<Self>) -> Result<(), PlanError> {
        tracing::trace!("closing flink scale planning ports.");
        self.inlet.close().await;
        self.decision_inlet.close().await;
        self.outlet.close().await;
        self.performance_repository.close().await?;
        Ok(())
    }
}
