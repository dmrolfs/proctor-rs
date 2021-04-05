use crate::graph::shape::{Shape, SinkShape, SourceShape, ThroughShape};
use crate::graph::{GraphResult, Inlet, Outlet, Port, Stage};
use crate::AppData;
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use std::fmt;

pub struct Identity<T: AppData> {
    name: String,
    inlet: Inlet<T>,
    outlet: Outlet<T>,
}

impl<T: AppData> Identity<T> {
    pub fn new<S: Into<String>>(name: S, inlet: Inlet<T>, outlet: Outlet<T>) -> Self {
        Self {
            name: name.into(),
            inlet,
            outlet,
        }
    }

    #[inline]
    pub fn inlet(&mut self) -> &mut Inlet<T> {
        &mut self.inlet
    }
}

impl<T: AppData> Shape for Identity<T> {}

impl<T: AppData> ThroughShape for Identity<T> {}

impl<T: AppData> SourceShape for Identity<T> {
    type Out = T;
    #[inline]
    fn outlet(&mut self) -> &mut Outlet<Self::Out> {
        &mut self.outlet
    }
}

impl<T: AppData> SinkShape for Identity<T> {
    type In = T;
    #[inline]
    fn inlet(&mut self) -> &mut Inlet<Self::In> {
        &mut self.inlet
    }
}

#[dyn_upcast]
#[async_trait]
impl<T: AppData + 'static> Stage for Identity<T> {
    #[inline]
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(level="info", name="run identity through", skip(self),)]
    async fn run(&mut self) -> GraphResult<()> {
        while let Some(value) = self.inlet.recv().await {
            self.outlet.send(value).await?;
        }

        Ok(())
    }

    async fn close(mut self: Box<Self>) -> GraphResult<()> {
        tracing::trace!("closing identity-through ports.");
        self.inlet.close().await;
        self.outlet.close().await;
        Ok(())
    }
}

impl<T: AppData> fmt::Debug for Identity<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Identity")
            .field("name", &self.name)
            .field("inlet", &self.inlet)
            .field("outlet", &self.outlet)
            .finish()
    }
}
