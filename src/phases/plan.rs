use std::fmt::{self, Debug};

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;

use crate::error::PlanError;
use crate::graph::stage::Stage;
use crate::graph::{Inlet, Outlet, Port, SinkShape, SourceShape, ThroughShape};
use crate::{AppData, ProctorResult};

pub trait DataDecisionStage: Stage + ThroughShape + 'static {
    type Decision;
    fn decision_inlet(&self) -> Inlet<Self::Decision>;
}

pub struct Plan<In, Decision, Out> {
    name: String,
    inner_plan: Box<dyn DataDecisionStage<In = In, Decision = Decision, Out = Out>>,
    inlet: Inlet<In>,
    decision_inlet: Inlet<Decision>,
    outlet: Outlet<Out>,
}

impl<In, Decision, Out> Plan<In, Decision, Out> {
    #[tracing::instrument(level = "info", skip(name))]
    pub fn new<S: Into<String>>(
        name: S, plan: impl DataDecisionStage<In = In, Decision = Decision, Out = Out>,
    ) -> Self {
        let inlet = plan.inlet();
        let decision_inlet = plan.decision_inlet();
        let outlet = plan.outlet();

        Self {
            name: name.into(),
            inner_plan: Box::new(plan),
            inlet,
            decision_inlet,
            outlet,
        }
    }
}

impl<In, Decision, Out> Debug for Plan<In, Decision, Out> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Plan")
            .field("name", &self.name)
            .field("inner_plan", &self.inner_plan)
            .field("inlet", &self.inlet)
            .field("decision_inlet", &self.decision_inlet)
            .field("outlet", &self.outlet)
            .finish()
    }
}

impl<In, Decision, Out> SinkShape for Plan<In, Decision, Out> {
    type In = In;

    #[inline]
    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

impl<In, Decision, Out> SourceShape for Plan<In, Decision, Out> {
    type Out = Out;

    #[inline]
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<In: AppData, Decision: AppData, Out: AppData> Stage for Plan<In, Decision, Out> {
    #[inline]
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run scaling_plan phase", skip(self))]
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

// this implementation block provides a convenient means to ground errors to the phase error.
impl<In: AppData, Decision: AppData, Out: AppData> Plan<In, Decision, Out> {
    #[inline]
    async fn do_check(&self) -> Result<(), PlanError> {
        self.inlet.check_attachment().await?;
        self.decision_inlet.check_attachment().await?;
        self.inner_plan
            .check()
            .await
            .map_err(|err| PlanError::StageError(err.into()))?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    #[inline]
    async fn do_run(&mut self) -> Result<(), PlanError> {
        self.inner_plan
            .run()
            .await
            .map_err(|err| PlanError::StageError(err.into()))?;

        Ok(())
    }

    #[inline]
    async fn do_close(mut self: Box<Self>) -> Result<(), PlanError> {
        tracing::trace!("closing scaling_plan ports.");
        self.inlet.close().await;
        self.decision_inlet.close().await;
        self.outlet.close().await;
        self.inner_plan
            .close()
            .await
            .map_err(|err| PlanError::StageError(err.into()))?;
        Ok(())
    }
}
