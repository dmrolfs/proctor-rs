use crate::elements::{PolicyFilter, PolicyFilterApi, PolicyFilterMonitor, TelemetryData, Policy};
use crate::graph::{Graph, Inlet, Outlet, Shape, SourceShape, SinkShape, ThroughShape, GraphResult, Port};
use crate::AppData;
use crate::graph::stage::{self, Stage, WithApi, WithMonitor};
use cast_trait_object::dyn_upcast;
use async_trait::async_trait;
use std::fmt;

mod context;
mod policy;


pub struct Eligibility<E: AppData + Clone> {
    name: String,
    policy_filter: Box<PolicyFilter<TelemetryData, E>>,
    inlet: Inlet<TelemetryData>,
    outlet: Outlet<TelemetryData>,
}

impl<E: AppData + Clone> Eligibility<E> {
    pub fn new<S: Into<String>>(
        name: S,
        policy: impl Policy<Item = TelemetryData, Environment = E> + 'static
    ) -> Self {
        let name = name.into();
        let policy_filter = Box::new(PolicyFilter::new(format!("eligibility_{}", name), Box::new(policy)));
        let inlet = Inlet::new(name.clone());
        let outlet = Outlet::new(name.clone());

        Self { name, policy_filter, inlet, outlet, }
    }

    #[inline]
    pub fn environment_inlet(&mut self) -> &mut Inlet<E> { self.policy_filter.environment_inlet() }

    // #[inline]
    // pub fn tx_policy_api(&self) -> PolicyFilterApi<E> { self.policy_filter.tx_api() }
    //
    // #[inline]
    // pub fn rx_policy_monitor(&self) -> PolicyFilterMonitor<TelemetryData, E> { self.policy_filter.rx_monitor() }
}

impl<E: AppData + Clone> Eligibility<E> {
    fn make_graph() -> Graph {
todo!()
    }
}

impl<E: AppData + Clone> Shape for Eligibility<E> { }
impl<E: AppData + Clone> ThroughShape for Eligibility<E> { }
impl<E: AppData + Clone> SinkShape for Eligibility<E> {
    type In = TelemetryData;
    #[inline]
    fn inlet(&mut self) -> &mut Inlet<Self::In> { &mut self.inlet }
}

impl<E: AppData + Clone> SourceShape for Eligibility<E> {
    type Out = TelemetryData;
    #[inline]
    fn outlet(&mut self) -> &mut Outlet<Self::Out> { &mut self.outlet }
}

#[dyn_upcast]
#[async_trait]
impl<E: AppData + Clone> Stage for Eligibility<E> {
    #[inline]
    fn name(&self) -> &str { self.name.as_ref() }

    #[tracing::instrument(level="info", name="run eligibility through", skip(self))]
    async fn run(&mut self) -> GraphResult<()> {
        todo!()
    }

    async fn close(mut self: Box<Self>) -> GraphResult<()> {
        tracing::trace!("closing eligibility ports.");
        self.inlet.close().await;
        self.policy_filter.close().await;
        self.outlet.close().await;
        Ok(())
    }
}

impl<E: AppData + Clone> WithApi for Eligibility<E> {
    type Sender = PolicyFilterApi<E>;
    #[inline]
    fn tx_api(&self) -> Self::Sender { self.policy_filter.tx_api() }
}

impl<E: AppData + Clone> WithMonitor for Eligibility<E> {
    type Receiver = PolicyFilterMonitor<TelemetryData, E>;
    #[inline]
    fn rx_monitor(&self) -> Self::Receiver { self.policy_filter.rx_monitor() }
}

impl<E: AppData + Clone> fmt::Debug for Eligibility<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Eligibility")
            .field("name", &self.name)
            .field("policy_filter", &self.policy_filter)
            .field("inlet", &self.inlet)
            .field("outlet", &self.outlet)
            .finish()
    }
}

