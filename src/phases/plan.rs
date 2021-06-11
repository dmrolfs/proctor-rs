use crate::error::PlanError;
use crate::graph::stage::{Stage, ThroughStage};
use crate::graph::{Inlet, Outlet, Port, SinkShape, SourceShape};
use crate::{AppData, ProctorResult};
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use std::fmt::{self, Debug};

pub struct Plan<In, Out> {
    name: String,
    inner_plan: Box<dyn ThroughStage<In, Out>>,
    inlet: Inlet<In>,
    outlet: Outlet<Out>,
}

impl<In, Out> Plan<In, Out> {
    #[tracing::instrument(level = "info", skip(name))]
    pub fn new<S: Into<String>>(name: S, plan: impl ThroughStage<In, Out>) -> Self {
        let inlet = plan.inlet();
        let outlet = plan.outlet();

        Self {
            name: name.into(),
            inner_plan: Box::new(plan),
            inlet,
            outlet,
        }
    }
}

impl<In, Out> Debug for Plan<In, Out> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Plan")
            .field("name", &self.name)
            .field("inner_plan", &self.inner_plan)
            .field("inlet", &self.inlet)
            .field("outlet", &self.outlet)
            .finish()
    }
}

impl<In, Out> SinkShape for Plan<In, Out> {
    type In = In;

    #[inline]
    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

impl<In, Out> SourceShape for Plan<In, Out> {
    type Out = Out;

    #[inline]
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<In: AppData, Out: AppData> Stage for Plan<In, Out> {
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
impl<In: AppData, Out: AppData> Plan<In, Out> {
    #[inline]
    async fn do_check(&self) -> Result<(), PlanError> {
        self.inlet.check_attachment().await?;
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
        self.outlet.close().await;
        self.inner_plan
            .close()
            .await
            .map_err(|err| PlanError::StageError(err.into()))?;
        Ok(())
    }
}
