use std::fmt::{self, Debug};

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;

use crate::graph::shape::{SinkShape, SourceShape};
use crate::graph::{stage, Inlet, Outlet, Port, Stage};
use crate::{AppData, ProctorResult, SharedString};

pub struct Identity<T> {
    name: SharedString,
    inlet: Inlet<T>,
    outlet: Outlet<T>,
}

impl<T> Identity<T> {
    pub fn new(name: impl Into<SharedString>, inlet: Inlet<T>, outlet: Outlet<T>) -> Self {
        Self { name: name.into(), inlet, outlet }
    }
}

impl<T> SourceShape for Identity<T> {
    type Out = T;

    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

impl<T> SinkShape for Identity<T> {
    type In = T;

    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<T: AppData> Stage for Identity<T> {
    fn name(&self) -> SharedString {
        self.name.clone()
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.inlet.check_attachment().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run identity through", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        while let Some(value) = self.inlet.recv().await {
            let _timer = stage::start_stage_eval_time(self.name.as_ref());
            self.outlet.send(value).await?;
        }

        Ok(())
    }

    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        tracing::trace!("closing identity-through ports.");
        self.inlet.close().await;
        self.outlet.close().await;
        Ok(())
    }
}

impl<T> Debug for Identity<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Identity")
            .field("name", &self.name)
            .field("inlet", &self.inlet)
            .field("outlet", &self.outlet)
            .finish()
    }
}
