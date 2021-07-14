use std::fmt::{self, Debug};

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;

use crate::error::PlanError;
use crate::flink::decision::result::DecisionResult;
use crate::flink::plan::{
    Appraisal, AppraisalRepository, FlinkScalePlan, RecordsPerSecond, Workload, WorkloadForecast,
};
use crate::flink::MetricCatalog;
use crate::graph::stage::Stage;
use crate::graph::{Inlet, Outlet, Port, SinkShape, SourceShape};
use crate::phases::plan::DataDecisionStage;
use crate::ProctorResult;

pub struct FlinkScalePlanning<F: WorkloadForecast> {
    name: String,
    inlet: Inlet<MetricCatalog>,
    decision_inlet: Inlet<DecisionResult<MetricCatalog>>,
    outlet: Outlet<FlinkScalePlan>,
    forecast: F,
    appraisal_repository: Box<dyn AppraisalRepository>,
}

impl<F: WorkloadForecast> FlinkScalePlanning<F> {
    #[tracing::instrument(level = "info", skip(name))]
    pub fn new(name: &str, forecast: F, appraisal_repository: Box<dyn AppraisalRepository>) -> Self {
        let name = name.to_string();
        let inlet = Inlet::new(name.clone());
        let decision_inlet = Inlet::new(format!("decision_{}", name.clone()));
        let outlet = Outlet::new(name.clone());
        Self {
            name,
            inlet,
            decision_inlet,
            outlet,
            forecast,
            appraisal_repository,
        }
    }
}

impl<F: WorkloadForecast> Debug for FlinkScalePlanning<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlinkScalePlanning")
            .field("name", &self.name)
            .field("inlet", &self.inlet)
            .field("decision_inlet", &self.decision_inlet)
            .field("outlet", &self.outlet)
            .field("forecast", &self.forecast)
            .field("appraisal_repository", &self.appraisal_repository)
            .finish()
    }
}

impl<F: WorkloadForecast> SourceShape for FlinkScalePlanning<F> {
    type Out = FlinkScalePlan;

    #[inline]
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

impl<F: WorkloadForecast> SinkShape for FlinkScalePlanning<F> {
    type In = MetricCatalog;

    #[inline]
    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

impl<F: 'static + WorkloadForecast> DataDecisionStage for FlinkScalePlanning<F> {
    type Decision = DecisionResult<MetricCatalog>;

    #[inline]
    fn decision_inlet(&self) -> Inlet<Self::Decision> {
        self.decision_inlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<F: 'static + WorkloadForecast> Stage for FlinkScalePlanning<F> {
    #[inline]
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

impl<F: 'static + WorkloadForecast> FlinkScalePlanning<F> {
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
        let forecast = &mut self.forecast;
        let appraisal_repository = &mut self.appraisal_repository;

        let mut appraisal = appraisal_repository.load(name.as_str()).await?.unwrap_or(Appraisal::default());
        let mut anticipated_workload = RecordsPerSecond::default();

        loop {
            tokio::select! {
                Some(data) = rx_data.recv() => {
                    if let Some(prediction) = Self::predict_anticipated_workload(data, forecast)? {
                        tracing::debug!(prior_prediction=%anticipated_workload, %prediction, "updating anticipated workload.");
                        anticipated_workload = prediction;
                    }
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

    #[tracing::instrument(level = "info", skip(data))]
    fn predict_anticipated_workload(
        data: MetricCatalog, forecast: &mut impl WorkloadForecast,
    ) -> Result<Option<RecordsPerSecond>, PlanError> {
        forecast.add_observation(data.into());
        let result = match forecast.predict_next_workload()? {
            Workload::RecordsInPerSecond(rips) => Some(rips),

            Workload::NotEnoughData => {
                let (nr_required, nr_needed) = forecast.observations_needed();
                tracing::debug!(
                    %nr_required, %nr_needed,
                    "not enough data points to make workload prediction - need {} more observations to make predictions.",
                    nr_needed
                );
                None
            },

            Workload::HeuristicsExceedThreshold {} => {
                tracing::warn!("forecast algorithm exceeded heuristics threshold - clearing");
                forecast.clear();
                None
            },
        };

        Ok(result)
    }

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
