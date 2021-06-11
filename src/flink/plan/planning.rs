use super::FlinkScalePlan;
use crate::flink::decision::result::DecisionResult;
use crate::flink::MetricCatalog;
use crate::graph::stage::Stage;
use crate::graph::{Inlet, Outlet, Port, SinkShape, SourceShape};
use crate::ProctorResult;
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use std::fmt::{self, Debug};

pub struct FlinkScalePlanning {
    name: String,
    inlet: Inlet<DecisionResult<MetricCatalog>>,
    outlet: Outlet<FlinkScalePlan>,
}

impl FlinkScalePlanning {
    #[tracing::instrument(level = "info", skip(name))]
    pub fn new<S: Into<String>>(name: S) -> Self {
        let name = name.into();
        let inlet = Inlet::new(name.clone());
        let outlet = Outlet::new(name.clone());
        Self { name, inlet, outlet }
    }
}

impl Debug for FlinkScalePlanning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlinkScalePlanning")
            .field("name", &self.name)
            .field("inlet", &self.inlet)
            .field("outlet", &self.outlet)
            .finish()
    }
}

impl SourceShape for FlinkScalePlanning {
    type Out = FlinkScalePlan;

    #[inline]
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

impl SinkShape for FlinkScalePlanning {
    type In = DecisionResult<MetricCatalog>;

    #[inline]
    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl Stage for FlinkScalePlanning {
    #[inline]
    fn name(&self) -> &str {
        self.name.as_str()
    }

    async fn check(&self) -> ProctorResult<()> {
        self.inlet.check_attachment().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    async fn run(&mut self) -> ProctorResult<()> {
        todo!()
    }

    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        tracing::trace!("closing flink scale planning ports.");
        self.inlet.close().await;
        self.outlet.close().await;
        Ok(())
    }
}

impl FlinkScalePlanning {}
