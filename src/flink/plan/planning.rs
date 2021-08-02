use std::fmt::{self, Debug};
use std::time::Duration;

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;

use crate::error::PlanError;
use crate::flink::decision::result::DecisionResult;
use crate::flink::plan::{
    FlinkScalePlan, ForecastCalculator, PerformanceHistory, PerformanceRepository, WorkloadForecastBuilder,
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
    min_scaling_step: u8,
}

impl<F: WorkloadForecastBuilder> FlinkScalePlanning<F> {
    #[tracing::instrument(level = "info", skip(name))]
    pub fn new(
        name: &str, restart_duration: Duration, max_catch_up_duration: Duration, recovery_valid_offset: Duration,
        forecast_builder: F, performance_repository: Box<dyn PerformanceRepository>, min_scaling_step: u8,
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
            min_scaling_step,
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
            .field("min_scaling_step", &self.min_scaling_step)
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

impl<F: 'static + WorkloadForecastBuilder> FlinkScalePlanning<F> {
    async fn do_check(&self) -> Result<(), PlanError> {
        self.inlet.check_attachment().await?;
        self.decision_inlet.check_attachment().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

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

                    if let DR::NoAction(ref metrics) = decision {
                        Self::handle_do_not_scale_decision(metrics)?
                    } else {
                        Self::handle_scale_decision(decision, calculator, &mut performance_history, outlet, self.min_scaling_step).await?
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
        decision: DecisionResult<MetricCatalog>, calculator: &mut ForecastCalculator<F>,
        performance_history: &mut PerformanceHistory, outlet: &Outlet<FlinkScalePlan>, min_scaling_step: u8,
    ) -> Result<(), PlanError> {
        if calculator.have_enough_data() {
            let current_nr_task_managers = decision.item().cluster.nr_task_managers;
            let buffered_records = decision.item().flow.input_consumer_lag; // todo: how to support other options
            let anticipated_workload = calculator.calculate_target_rate(decision.item().timestamp, buffered_records)?;
            let required_nr_task_managers = performance_history.cluster_size_for_workload(anticipated_workload);
            if let Some(plan) = FlinkScalePlan::new(decision, required_nr_task_managers, min_scaling_step) {
                tracing::info!(?plan, "pushing scale plan.");
                outlet.send(plan).await?;
            } else {
                tracing::warn!(
                    %required_nr_task_managers,
                    %current_nr_task_managers,
                    "performance history suggests no change in cluster size needed."
                );
                // todo: should we clear some of the history????
            }
        } else {
            tracing::info!(
                needed=%calculator.observations_needed().0,
                required=%calculator.observations_needed().1,
                "passing on planning decision since more observations are required to forecast workflow."
            )
        }

        Ok(())
    }

    async fn do_close(mut self: Box<Self>) -> Result<(), PlanError> {
        tracing::trace!("closing flink scale planning ports.");
        self.inlet.close().await;
        self.decision_inlet.close().await;
        self.outlet.close().await;
        self.performance_repository.close().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use approx::assert_relative_eq;
    use chrono::{TimeZone, Utc};
    use claim::*;
    use lazy_static::lazy_static;
    // use mockall::predicate::*;
    // use mockall::*;
    use pretty_assertions::assert_eq;
    use tokio::sync::mpsc;
    use tokio::sync::Mutex;
    use tokio_test::block_on;

    use super::*;
    use crate::elements::telemetry;
    use crate::flink::plan::benchmark::BenchmarkRange;
    use crate::flink::plan::forecast::*;
    use crate::flink::{ClusterMetrics, FlowMetrics};

    const STEP: i64 = 15;
    const NOW: i64 = 1624061766 + (30 * STEP);

    lazy_static! {
        static ref METRICS: MetricCatalog = MetricCatalog {
            timestamp: Utc.timestamp(NOW, 0),
            flow: FlowMetrics {
                input_consumer_lag: 314.15926535897932384264,
                ..FlowMetrics::default()
            },
            cluster: ClusterMetrics { nr_task_managers: 4, ..ClusterMetrics::default() },
            custom: telemetry::Table::default(),
        };
        static ref SCALE_UP: DecisionResult<MetricCatalog> = DecisionResult::ScaleUp(METRICS.clone());
        static ref SCALE_DOWN: DecisionResult<MetricCatalog> = DecisionResult::ScaleDown(METRICS.clone());
        static ref NO_SCALE: DecisionResult<MetricCatalog> = DecisionResult::NoAction(METRICS.clone());
    }

    fn setup_calculator() -> Arc<Mutex<ForecastCalculator<LeastSquaresWorkloadForecastBuilder>>> {
        let mut calc = ForecastCalculator::new(
            LeastSquaresWorkloadForecastBuilder::new(20, SpikeSettings { influence: 0.25, ..SpikeSettings::default() }),
            Duration::from_secs(2 * 60),  // restart
            Duration::from_secs(13 * 60), // max_catch_up
            Duration::from_secs(5 * 60),  // valid_offset
        )
        .unwrap();

        (1..=30).into_iter().for_each(|tick| {
            let ts = Utc.timestamp(NOW - (30 - tick) * STEP, 0);
            calc.add_observation(WorkloadMeasurement {
                timestamp_secs: ts.timestamp(),
                workload: (tick as f64).into(),
            });
        });

        Arc::new(Mutex::new(calc))
    }

    #[test]
    fn test_flink_planning_handle_scale_decision() {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_flink_planning_handle_scale_decision");
        let _main_span_guard = main_span.enter();

        let (probe_tx, mut probe_rx) = mpsc::channel(8);
        let mut outlet = Outlet::new("plan outlet");

        let mut calc = setup_calculator();
        let min_step = 2;

        block_on(async move {
            outlet.attach("plan_outlet", probe_tx).await;
            let calc_2 = Arc::clone(&calc);

            let history = Arc::new(Mutex::new(PerformanceHistory::default()));
            let history_2 = Arc::clone(&history);

            tracing::warn!("DMR - testing scale-up with empty history...");
            let handle = tokio::spawn(async move {
                let c = &mut *calc_2.lock().await;
                let h = &mut *history_2.lock().await;
                assert_ok!(FlinkScalePlanning::handle_scale_decision(SCALE_UP.clone(), c, h, &outlet, min_step).await);
            });

            assert_ok!(handle.await);

            let expected = FlinkScalePlan {
                target_nr_task_managers: min_step + METRICS.cluster.nr_task_managers,
                current_nr_task_managers: METRICS.cluster.nr_task_managers,
            };
            let actual = probe_rx.recv().await;
            tracing::info!(
                ?expected,
                ?actual,
                ?history,
                ?calc,
                "scale up decision with no history..."
            );
            assert_eq!(Some(expected), actual);
        });
    }
}
