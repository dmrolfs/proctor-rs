use std::fmt::{self, Debug};

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;

use super::FlinkScalePlan;
use crate::error::PlanError;
use crate::flink::decision::result::DecisionResult;
use crate::flink::plan::appraisal::Appraisal;
use crate::flink::plan::forecast::WorkloadForecast;
use crate::flink::plan::Benchmark;
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
    appraisal: Appraisal,
}

impl<F: WorkloadForecast> FlinkScalePlanning<F> {
    #[tracing::instrument(level = "info", skip(name))]
    pub fn new<S: Into<String>>(name: S, forecast: F) -> Self {
        let name = name.into();
        let inlet = Inlet::new(name.clone());
        let decision_inlet = Inlet::new(format!("decision_{}", name.clone()));
        let outlet = Outlet::new(name.clone());

        Self {
            name,
            inlet,
            decision_inlet,
            outlet,
            forecast,
            appraisal: Appraisal::default(),
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
            .field("appraisal", &self.appraisal)
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

impl<F: WorkloadForecast> FlinkScalePlanning<F> {
    #[inline]
    async fn do_check(&self) -> Result<(), PlanError> {
        self.inlet.check_attachment().await?;
        self.decision_inlet.check_attachment().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    #[inline]
    async fn do_run(&mut self) -> Result<(), PlanError> {
        let outlet = &self.outlet;
        let rx_data = &mut self.inlet;
        let rx_decision = &mut self.decision_inlet;
        let forecast = &mut self.forecast;
        let appraisal = &self.appraisal;

        loop {
            tokio::select! {
                Some(data) = rx_data.recv() => {
                    Self::handle_data_item(data, forecast).await?;
                },

                Some(decision) = rx_decision.recv() => {
                    Self::handle_decision(decision, outlet).await?;
                },

                else => {
                    tracing::info!("Flink scale planning done - breaking...");
                    break;
                },
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "info", skip(data), fields())]
    async fn handle_data_item(data: MetricCatalog, forecast: &mut impl WorkloadForecast) -> Result<(), PlanError> {
        forecast.add_observation(data.into());
        todo!()
    }

    #[tracing::instrument(level = "info", skip(), fields())]
    async fn handle_decision(
        decision: DecisionResult<MetricCatalog>, outlet: &Outlet<FlinkScalePlan>,
    ) -> Result<(), PlanError> {
        todo!()
    }

    #[inline]
    async fn do_close(mut self: Box<Self>) -> Result<(), PlanError> {
        tracing::trace!("closing flink scale planning ports.");
        self.inlet.close().await;
        self.decision_inlet.close().await;
        self.outlet.close().await;
        Ok(())
    }
}
