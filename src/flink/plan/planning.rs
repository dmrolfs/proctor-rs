use std::fmt::{self, Debug};
use std::time::Duration;

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;

use crate::error::PlanError;
use crate::flink::decision::result::DecisionResult;
use crate::flink::plan::{
    Appraisal, AppraisalRepository, FlinkScalePlan, RecordsPerSecond, TimestampSeconds, WorkloadForecast,
    WorkloadForecastBuilder, WorkloadMeasurement,
};
use crate::flink::MetricCatalog;
use crate::graph::stage::Stage;
use crate::graph::{Inlet, Outlet, Port, SinkShape, SourceShape};
use crate::phases::plan::DataDecisionStage;
use crate::ProctorResult;

#[derive(Debug)]
pub struct ForecastCalculator<F: WorkloadForecastBuilder> {
    forecast_builder: F,
    pub restart: Duration,
    pub max_catch_up: Duration,
    pub valid_offset: Duration,
}

impl<F: WorkloadForecastBuilder> ForecastCalculator<F> {
    pub fn new(
        forecast_builder: F, restart: Duration, max_catch_up: Duration, valid_offset: Duration,
    ) -> Result<Self, PlanError> {
        let restart = Self::check_duration(restart)?;
        let max_catch_up = Self::check_duration(max_catch_up)?;
        let valid_offset = Self::check_duration(valid_offset)?;
        Ok(Self {
            forecast_builder,
            restart,
            max_catch_up,
            valid_offset,
        })
    }

    fn check_duration(d: Duration) -> Result<Duration, PlanError> {
        if (f64::MAX as u128) < d.as_millis() {
            return Err(PlanError::DurationLimitExceeded(d.as_millis()));
        }

        Ok(d)
    }

    fn observations_needed(&self) -> (usize, usize) {
        self.forecast_builder.observations_needed()
    }

    fn add_observation(&mut self, measurement: WorkloadMeasurement) {
        self.forecast_builder.add_observation(measurement)
    }

    fn clear(&mut self) {
        self.forecast_builder.clear()
    }

    #[tracing::instrument(
        level="debug",
        skip(self),
        fields(restart=?self.restart, max_catch_up=?self.max_catch_up, valid_offset=?self.valid_offset)
    )]
    pub fn calculate_target_rate(&mut self, buffered_records: i64) -> Result<RecordsPerSecond, PlanError> {
        let now = chrono::Utc::now().into();
        let recovery = self.calculate_recovery_timestamp_from(now);
        let valid = self.calculate_valid_timestamp_after_recovery(recovery);
        tracing::debug!( now=?now.as_utc(), recovery=?recovery.as_utc(), valid=?valid.as_utc(), "scaling adjustment timestamps estimated." );

        let forecast = self.forecast_builder.build_forecast()?;
        tracing::debug!(?forecast, "workload forecast model calculated.");

        let total_records = self.total_records_between(&forecast, now, recovery)? + buffered_records as f64;
        tracing::debug!(%total_records, "estimated total records to process before valid time");

        let recovery_rate = self.recovery_rate(total_records);
        let valid_workload_rate = forecast.workload_at(valid)?;
        let target_rate = RecordsPerSecond::max(recovery_rate, valid_workload_rate);

        tracing::debug!(
            %recovery_rate,
            %valid_workload_rate,
            %target_rate,
            "target rate calculated as max of recovery and workload (at valid time) rates."
        );

        Ok(target_rate)
    }

    fn calculate_recovery_timestamp_from(&self, timestamp: TimestampSeconds) -> TimestampSeconds {
        timestamp + self.restart + self.max_catch_up
    }

    fn calculate_valid_timestamp_after_recovery(&self, recovery: TimestampSeconds) -> TimestampSeconds {
        recovery + self.valid_offset
    }

    fn total_records_between(
        &self, forecast: &Box<dyn WorkloadForecast>, start: TimestampSeconds, end: TimestampSeconds,
    ) -> Result<f64, PlanError> {
        let total = forecast.total_records_between(start, end)?;
        tracing::debug!("total records between [{}, {}] = {}", start, end, total);
        Ok(total)
    }

    fn recovery_rate(&self, total_records: f64) -> RecordsPerSecond {
        let catch_up = self.max_catch_up.as_secs_f64();
        RecordsPerSecond::new(total_records / catch_up)
    }
}

pub struct FlinkScalePlanning<F: WorkloadForecastBuilder> {
    name: String,
    inlet: Inlet<MetricCatalog>,
    decision_inlet: Inlet<DecisionResult<MetricCatalog>>,
    outlet: Outlet<FlinkScalePlan>,
    forecast_calculator: ForecastCalculator<F>,
    appraisal_repository: Box<dyn AppraisalRepository>,
}

impl<F: WorkloadForecastBuilder> FlinkScalePlanning<F> {
    #[tracing::instrument(level = "info", skip(name))]
    pub fn new(
        name: &str, restart_duration: Duration, max_catch_up_duration: Duration, recovery_valid_offset: Duration,
        forecast_builder: F, appraisal_repository: Box<dyn AppraisalRepository>,
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
            appraisal_repository,
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
            .field("appraisal_repository", &self.appraisal_repository)
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
        let forecast = &mut self.forecast_calculator;
        let appraisal_repository = &mut self.appraisal_repository;

        let mut appraisal = appraisal_repository
            .load(name.as_str())
            .await?
            .unwrap_or(Appraisal::default());
        let mut anticipated_workload = RecordsPerSecond::default();

        // todo: rework considering forecast caclulator refactor
        loop {
            tokio::select! {
                Some(data) = rx_data.recv() => {
                    // if let Some(prediction) = Self::predict_anticipated_workload(data, forecast)? {
                    //     tracing::debug!(prior_prediction=%anticipated_workload, %prediction, "updating anticipated workload.");
                    //     anticipated_workload = prediction;
                    // }
                    todo!();
                },

                Some(decision) = rx_decision.recv() => {
                    Self::update_appraisal(&decision, name.as_str(), &mut appraisal, appraisal_repository).await?;

                    let _foo: () = if let DR::NoAction(ref metrics) = decision {
                        Self::handle_do_not_scale_decision(metrics)?
                    } else {
                        Self::handle_scale_decision(decision, anticipated_workload, &appraisal, outlet).await?
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
    async fn update_appraisal(
        decision: &DecisionResult<MetricCatalog>, name: &str, appraisal: &mut Appraisal,
        repository: &mut Box<dyn AppraisalRepository>,
    ) -> Result<(), PlanError> {
        use crate::flink::decision::result::DecisionResult as DR;

        let update_repository = match decision {
            DR::NoAction(_) => false,
            DR::ScaleUp(metrics) => {
                appraisal.add_upper_benchmark(metrics.into());
                true
            },
            DR::ScaleDown(metrics) => {
                appraisal.add_lower_benchmark(metrics.into());
                true
            },
        };

        if update_repository {
            repository.save(name, appraisal).await?;
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
        decision: DecisionResult<MetricCatalog>, anticipated_workload: RecordsPerSecond, appraisal: &Appraisal,
        outlet: &Outlet<FlinkScalePlan>,
    ) -> Result<(), PlanError> {
        let cur_nr_task_managers = decision.item().cluster.nr_task_managers;
        // let total_records =
        let required_cluster_size = appraisal.cluster_size_for_workload(anticipated_workload);
        // todo WORK HERE
        todo!()
    }

    #[inline]
    async fn do_close(mut self: Box<Self>) -> Result<(), PlanError> {
        tracing::trace!("closing flink scale planning ports.");
        self.inlet.close().await;
        self.decision_inlet.close().await;
        self.outlet.close().await;
        // self.appraisal_repository.close().await?;
        Ok(())
    }
}
