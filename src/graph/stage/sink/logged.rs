use crate::graph::shape::{Shape, SinkShape};
use crate::graph::{GraphResult, Inlet, Port, Stage};
use crate::AppData;
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use std::fmt;

pub struct LoggedSink<In>
where
    In: AppData,
{
    name: String,
    inlet: Inlet<In>,
}

impl<In> LoggedSink<In>
where
    In: AppData,
{
    pub fn new<S>(name: S) -> Self
    where
        S: Into<String>,
    {
        let name = name.into();
        let inlet = Inlet::new(name.clone());
        Self { name, inlet }
    }
}

#[dyn_upcast]
#[async_trait]
impl<In> Stage for LoggedSink<In>
where
    In: AppData + 'static,
{
    #[inline]
    fn name(&self) -> &str {
        self.name.as_ref()
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

impl<In> Shape for LoggedSink<In> where In: AppData {}

impl<In> SinkShape for LoggedSink<In>
where
    In: AppData,
{
    type In = In;

    #[inline]
    fn inlet(&mut self) -> &mut Inlet<Self::In> {
        &mut self.inlet
    }
}

impl<In> fmt::Debug for LoggedSink<In>
where
    In: AppData,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoggedSink")
            .field("name", &self.name)
            .field("inlet", &self.inlet)
            .finish()
    }
}
