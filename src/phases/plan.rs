use std::fmt::{self, Debug};

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use tokio::sync::broadcast;

use crate::error::PlanError;
use crate::graph::stage::{Stage, WithMonitor};
use crate::graph::{Inlet, Outlet, Port, SinkShape, SourceShape};
use crate::{AppData, ProctorResult};

type PlanMonitor<P> =
    broadcast::Receiver<PlanEvent<<P as Planning>::Observation, <P as Planning>::Decision, <P as Planning>::Out>>;
type Event<P> = PlanEvent<<P as Planning>::Observation, <P as Planning>::Decision, <P as Planning>::Out>;

#[derive(Debug, Clone, PartialEq)]
pub enum PlanEvent<Observation, Decision, Out> {
    ObservationAdded(Observation),
    DecisionPlanned(Decision, Out),
    DecisionIgnored(Decision),
}

#[async_trait]
pub trait Planning: Debug + Send + Sync {
    type Observation: AppData + Clone;
    type Decision: AppData + Clone;
    type Out: AppData + Clone;

    fn set_outlet(&mut self, outlet: Outlet<Self::Out>);
    fn add_observation(&mut self, observation: Self::Observation);
    async fn handle_decision(&mut self, decision: Self::Decision) -> Result<Option<Self::Out>, PlanError>;
    async fn close(mut self) -> Result<(), PlanError>;
}

pub struct Plan<P: Planning> {
    name: String,
    planning: P,
    inlet: Inlet<P::Observation>,
    decision_inlet: Inlet<P::Decision>,
    outlet: Outlet<P::Out>,
    // pub tx_monitor: broadcast::Sender<PlanEvent<P::Observation, P::Decision, P::Out>>,
    pub tx_monitor: broadcast::Sender<Event<P>>,
}

impl<P: Planning> Plan<P> {
    #[tracing::instrument(level = "info", skip(name))]
    pub fn new(name: impl AsRef<str>, mut planning: P) -> Self {
        let inlet = Inlet::new(name.as_ref());
        let decision_inlet = Inlet::new(format!("decision_{}", name.as_ref()));
        let outlet = Outlet::new(name.as_ref());
        planning.set_outlet(outlet.clone());

        let (tx_monitor, _) = broadcast::channel(num_cpus::get() * 2);

        Self {
            name: name.as_ref().to_string(),
            planning,
            inlet,
            decision_inlet,
            outlet,
            tx_monitor,
        }
    }

    pub fn decision_inlet(&self) -> Inlet<P::Decision> {
        self.decision_inlet.clone()
    }
}

impl<P: Planning> Debug for Plan<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Plan")
            .field("name", &self.name)
            .field("planning", &self.planning)
            .field("inlet", &self.inlet)
            .field("decision_inlet", &self.decision_inlet)
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
impl<P: Planning> Plan<P> {
    async fn do_check(&self) -> Result<(), PlanError> {
        self.inlet.check_attachment().await?;
        self.decision_inlet.check_attachment().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    async fn do_run(&mut self) -> Result<(), PlanError> {
        let rx_data = &mut self.inlet;
        let rx_decision = &mut self.decision_inlet;
        let tx_monitor = &self.tx_monitor;
        let planning = &mut self.planning;

        loop {
            tokio::select! {
                Some(data) = rx_data.recv() => {
                    let observation: P::Observation = data.into();
                    planning.add_observation(observation.clone());
                    Self::emit_event(tx_monitor, PlanEvent::ObservationAdded(observation));
                },

                Some(decision) = rx_decision.recv() => {
                    let out = planning.handle_decision(decision.clone()).await?;
                    let event = if let Some(out) = out {
                        PlanEvent::DecisionPlanned(decision, out)
                    } else {
                        PlanEvent::DecisionIgnored(decision)
                    };

                    Self::emit_event(tx_monitor, event);
                },

                else => {
                    tracing::info!("Plan stage done - breaking...");
                    break;
                },
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(tx_monitor))]
    fn emit_event(tx_monitor: &broadcast::Sender<Event<P>>, event: Event<P>) {
        match tx_monitor.send(event) {
            Ok(nr_subsribers) => tracing::debug!(%nr_subsribers, "published event to subscribers"),
            Err(err) => {
                tracing::warn!(error=?err, "failed to publish event - can add subscribers to receive future events.")
            }
        }
    }

    async fn do_close(mut self: Box<Self>) -> Result<(), PlanError> {
        tracing::trace!("closing scaling_plan ports.");
        self.inlet.close().await;
        self.decision_inlet.close().await;
        self.outlet.close().await;
        self.planning.close().await?;
        Ok(())
    }
}
