use std::fmt;

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;

use crate::graph::shape::SinkShape;
use crate::graph::{Inlet, Port, Stage, PORT_DATA};
use crate::{AppData, ProctorResult, SharedString};

pub struct LoggedSink<In> {
    name: String,
    inlet: Inlet<In>,
}

impl<In> LoggedSink<In> {
    pub fn new<S: Into<String>>(name: S) -> Self {
        let name: SharedString = SharedString::Owned(name.into());
        let inlet = Inlet::new(name.clone(), PORT_DATA);
        Self { name: name.into_owned(), inlet }
    }
}

#[dyn_upcast]
#[async_trait]
impl<In: AppData> Stage for LoggedSink<In> {
    #[inline]
    fn name(&self) -> &str {
        self.name.as_ref()
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.inlet.check_attachment().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run logging sink", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        while let Some(input) = self.inlet.recv().await {
            tracing::warn!("in graph sink: {:?}", input);
        }
        Ok(())
    }

    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
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
