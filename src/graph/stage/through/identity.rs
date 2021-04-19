use crate::graph::shape::{SinkShape, SourceShape};
use crate::graph::{GraphResult, Inlet, Outlet, Port, Stage};
use crate::AppData;
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use std::fmt::{self, Debug};

pub struct Identity<T> {
    name: String,
    inlet: Inlet<T>,
    outlet: Outlet<T>,
}

impl<T> Identity<T> {
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

impl<T> SourceShape for Identity<T> {
    type Out = T;
    #[inline]
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

impl<T> SinkShape for Identity<T> {
    type In = T;
    #[inline]
    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<T: AppData> Stage for Identity<T> {
    #[inline]
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> GraphResult<()> {
        self.inlet.check_attachment().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run identity through", skip(self))]
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

impl<T> Debug for Identity<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Identity")
            .field("name", &self.name)
            .field("inlet", &self.inlet)
            .field("outlet", &self.outlet)
            .finish()
    }
}
