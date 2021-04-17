use crate::graph::shape::SinkShape;
use crate::graph::{GraphResult, Inlet, Port, Stage};
use crate::AppData;
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use std::fmt;

pub struct LoggedSink<In> {
    name: String,
    inlet: Inlet<In>,
}

impl<In> LoggedSink<In> {
    pub fn new<S: Into<String>>(name: S) -> Self {
        let name = name.into();
        let inlet = Inlet::new(name.clone());
        Self { name, inlet }
    }
}

#[dyn_upcast]
#[async_trait]
impl<In: AppData> Stage for LoggedSink<In> {
    #[inline]
    fn name(&self) -> &str {
        self.name.as_ref()
    }

    #[tracing::instrument(level="info", skip(self))]
    async fn check(&self) -> GraphResult<()> {
        self.inlet.check_attachment().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run logging sink", skip(self))]
    async fn run(&mut self) -> GraphResult<()> {
        while let Some(input) = self.inlet.recv().await {
            tracing::warn!("in graph sink: {:?}", input);
        }
        Ok(())
    }

    async fn close(mut self: Box<Self>) -> GraphResult<()> {
        tracing::trace!("closing logging-sink inlet.");
        self.inlet.close().await;
        Ok(())
    }
}

impl<In> SinkShape for LoggedSink<In> {
    type In = In;

    #[inline]
    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

impl<In> fmt::Debug for LoggedSink<In> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoggedSink")
            .field("name", &self.name)
            .field("inlet", &self.inlet)
            .finish()
    }
}
