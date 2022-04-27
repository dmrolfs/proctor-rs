use std::fmt::{self, Debug};
use std::sync::Arc;

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use tokio::sync::broadcast;
use tracing::Instrument;

use crate::error::PlanError;
use crate::graph::stage::{Stage, WithMonitor};
use crate::graph::{stage, Inlet, Outlet, Port, SinkShape, SourceShape, PORT_CONTEXT, PORT_DATA};
use crate::{AppData, Correlation, ProctorResult};

// pub type Event<P> = PlanEvent<<P as Planning>::Context, <P as Planning>::Decision, <P as
// Planning>::Out>;
pub type PlanMonitor<P> = broadcast::Receiver<Arc<PlanEvent<P>>>;

#[derive(Debug, Clone, PartialEq)]
pub enum PlanEvent<P: Planning + ?Sized> {
    DecisionPlanned(P::Decision, P::Out),
    DecisionIgnored(P::Decision),
    ContextChanged(P::Context),
}

#[async_trait]
pub trait Planning: Debug + Send + Sync {
    type Observation: AppData + Clone;
    type Decision: AppData + Correlation + Clone;
    type Context: AppData + Correlation + Clone;
    type Out: AppData + Clone;

    fn set_outlet(&mut self, outlet: Outlet<Self::Out>);

    fn add_observation(&mut self, observation: Self::Observation);

    /// Patch planning context with populated properties of incoming context records. Optional
    /// fields that are None may be ignored.
    async fn patch_context(&mut self, context: Self::Context) -> Result<Option<PlanEvent<Self>>, PlanError>;

    async fn handle_decision(&mut self, decision: Self::Decision) -> Result<Option<Self::Out>, PlanError>;

    async fn close(mut self) -> Result<(), PlanError>;
}

pub struct Plan<P: Planning> {
    name: String,
    planning: P,
    inlet: Inlet<P::Observation>, // todo: consider making observation inlet secondary to decision (in Sink Shape)
    decision_inlet: Inlet<P::Decision>,
    context_inlet: Inlet<P::Context>,
    outlet: Outlet<P::Out>,
    pub tx_monitor: broadcast::Sender<Arc<PlanEvent<P>>>,
}

impl<P: Planning> Plan<P> {
    #[tracing::instrument(level = "trace", skip(name))]
    pub fn new(name: impl AsRef<str>, mut planning: P) -> Self {
        let name = format!("{}_plan", name.as_ref());
        let inlet = Inlet::new(name.clone(), PORT_DATA);
        let decision_inlet = Inlet::new(name.clone(), "decision");
        let context_inlet = Inlet::new(name.clone(), PORT_CONTEXT);
        let outlet = Outlet::new(name.clone(), PORT_DATA);
        planning.set_outlet(outlet.clone());

        let (tx_monitor, _) = broadcast::channel(num_cpus::get() * 2);

        Self {
            name,
            planning,
            inlet,
            decision_inlet,
            context_inlet,
            outlet,
            tx_monitor,
        }
    }

    pub fn decision_inlet(&self) -> Inlet<P::Decision> {
        self.decision_inlet.clone()
    }

    pub fn context_inlet(&self) -> Inlet<P::Context> {
        self.context_inlet.clone()
    }
}

impl<P: Planning> Debug for Plan<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Plan")
            .field("name", &self.name)
            .field("planning", &self.planning)
            .field("inlet", &self.inlet)
            .field("decision_inlet", &self.decision_inlet)
            .field("context_inlet", &self.context_inlet)
            .field("outlet", &self.outlet)
            .finish()
    }
}

impl<P: Planning> SinkShape for Plan<P> {
    type In = P::Observation;

    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

impl<P: Planning> SourceShape for Plan<P> {
    type Out = P::Out;

    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

impl<P: Planning> WithMonitor for Plan<P> {
    type Receiver = PlanMonitor<P>;

    fn rx_monitor(&self) -> Self::Receiver {
        self.tx_monitor.subscribe()
    }
}

#[dyn_upcast]
#[async_trait]
impl<P: 'static + Planning> Stage for Plan<P> {
    fn name(&self) -> &str {
        &self.name
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", name = "run scaling_plan phase", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        self.do_run().await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        self.do_close().await?;
        Ok(())
    }
}

// this implementation block provides a convenient means to ground errors to the phase error.
impl<P: Planning> Plan<P> {
    async fn do_check(&self) -> Result<(), PlanError> {
        self.inlet.check_attachment().await?;
        self.decision_inlet.check_attachment().await?;
        self.context_inlet.check_attachment().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    async fn do_run(&mut self) -> Result<(), PlanError> {
        let rx_data = &mut self.inlet;
        let rx_context = &mut self.context_inlet;
        let rx_decision = &mut self.decision_inlet;
        let tx_monitor = &self.tx_monitor;
        let planning = &mut self.planning;

        loop {
            tokio::select! {
                Some(data) = rx_data.recv() => planning.add_observation(data),

                Some(context) = rx_context.recv() => {
                    let span = tracing::info_span!("DMR(debug):Plan handle context", correlation=?context.correlation(),);
                    if let Some(event) = planning.patch_context(context).instrument(span).await? {
                        Self::publish_event(tx_monitor, event);
                    }
                },

                Some(decision) = rx_decision.recv() => {
                    let _timer = stage::start_stage_eval_time(&self.name);

                    let span = tracing::info_span!("DMR(debug):Plan handle decision", correlation=?decision.correlation(),);
                    let event = match planning.handle_decision(decision.clone()).instrument(span).await? {
                        Some(out) => PlanEvent::DecisionPlanned(decision, out),
                        None => PlanEvent::DecisionIgnored(decision),
                    };

                    Self::publish_event(tx_monitor, event);
                },

                else => break,
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(tx_monitor))]
    fn publish_event(tx_monitor: &broadcast::Sender<Arc<PlanEvent<P>>>, event: PlanEvent<P>) {
        match tx_monitor.send(Arc::new(event)) {
            Ok(nr_subsribers) => tracing::debug!(%nr_subsribers, "published event to subscribers"),
            Err(err) => {
                tracing::warn!(error=?err, "failed to publish event - can add subscribers to receive future events.")
            },
        }
    }

    async fn do_close(mut self: Box<Self>) -> Result<(), PlanError> {
        tracing::trace!(stage=%self.name, "closing scaling_plan ports.");
        self.inlet.close().await;
        self.decision_inlet.close().await;
        self.context_inlet.close().await;
        self.outlet.close().await;
        self.planning.close().await?;
        Ok(())
    }
}
