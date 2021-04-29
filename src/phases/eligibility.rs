use crate::elements::{Policy, PolicyFilter, PolicyFilterApi, PolicyFilterEvent, PolicyFilterMonitor};
use crate::error::GraphError;
use crate::graph::stage::{Stage, WithApi, WithMonitor};
use crate::graph::{GraphResult, Inlet, Outlet, Port, SinkShape, SourceShape, ThroughShape};
use crate::{AppData, ProctorContext};
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use std::fmt::{self, Debug};
use tokio::sync::broadcast;

mod context;
mod policy;

pub struct Eligibility<D, C> {
    name: String,
    // pub subscription: TelemetrySubscription,
    inner_stage: Option<Box<dyn InnerStage<D>>>,
    pub context_inlet: Inlet<C>,
    inlet: Inlet<D>,
    outlet: Outlet<D>,
    tx_policy_api: PolicyFilterApi<C>,
    tx_policy_monitor: broadcast::Sender<PolicyFilterEvent<D, C>>,
}

impl<D: AppData + Clone, C: ProctorContext> Eligibility<D, C> {
    #[tracing::instrument(level = "info", skip(name))]
    pub fn new<S: Into<String>>(name: S, policy: impl Policy<Item = D, Context = C> + 'static) -> Self {
        let name = name.into();
        let policy_filter: PolicyFilter<D, C> =
            PolicyFilter::new(format!("{}_eligibility_policy", name), Box::new(policy));
        let context_inlet = policy_filter.context_inlet();
        let inlet = policy_filter.inlet();
        let outlet = policy_filter.outlet();
        let tx_policy_api = policy_filter.tx_api();
        let tx_policy_monitor = policy_filter.tx_monitor.clone();

        Self {
            name,
            inner_stage: Some(Box::new(policy_filter)),
            context_inlet,
            inlet,
            outlet,
            tx_policy_api,
            tx_policy_monitor,
        }
        // let name = name.into();
        // // let subscription = policy.subscription(name.as_str());
        //
        // let inlet = Inlet::new(format!("{}_eligibility_inlet", name));
        // let outlet = Outlet::new(format!("{}_eligibility_outlet", name));
        //
        // // let telemetry_into_context = crate::elements::make_from_telemetry::<C, _>(&subscription.name, true).await?;
        // // let context_subscription_inlet = telemetry_into_context.inlet().clone();
        //
        // let policy_filter = PolicyFilter::new(format!("{}_eligibility_policy", name), Box::new(policy));
        // let tx_policy_api = policy_filter.tx_api();
        // let tx_policy_monitor = policy_filter.tx_monitor.clone();
        // let context_inlet = policy_filter.context_inlet();
        //
        // // (telemetry_into_context.outlet(), policy_filter.context_inlet())
        // //     .connect()
        // //     .await;
        //
        // let into_graph = Outlet::new(format!("{}_into_eligibility_graph", name));
        // let policy_inlet = policy_filter.inlet();
        // let policy_outlet = policy_filter.outlet();
        // let from_graph = Inlet::new(format!("{}_from_eligibility_graph", name));
        //
        // (&into_graph, &policy_inlet).connect().await;
        // (&policy_outlet, &from_graph).connect().await;
        //
        // let in_bridge = stage::Identity::new(format!("{}_eligibility_ingress_bridge", name), inlet.clone(), into_graph);
        // let out_bridge = stage::Identity::new(format!("{}_eligibility_egress_bridge", name), from_graph, outlet.clone());
        //
        // let mut graph = Graph::default();
        // graph.push_front(Box::new(in_bridge)).await;
        // // graph.push_back(telemetry_into_context.dyn_upcast()).await;
        // graph.push_back(Box::new(policy_filter)).await;
        // graph.push_back(Box::new(out_bridge)).await;
        //
        // let composite = stage::CompositeThrough::new(
        //     format!("{}__[eligibility]", name),
        //     graph,
        //     inlet.clone(),
        //     outlet.clone(),
        // )
        // .await;
        //
        // Ok(Self {
        //     name,
        //     // subscription,
        //     inner_stage: Some(Box::new(composite)),
        //     context_inlet,
        //     inlet,
        //     outlet,
        //     tx_policy_api,
        //     tx_policy_monitor,
        // })
    }

    // pub fn new<S: Into<String>>(
    //     name: S, policy: impl Policy<Item = TelemetryData, Context = C> + 'static,
    // ) -> ProctorResult<Self> {
    //     tracing::warn!("AAA-1");
    //     let name = name.into();
    //     tracing::warn!("AAA-2");
    //
    //     let subscription = policy.subscription(name.as_str());
    //
    //     tracing::warn!("BBB");
    //     let policy_filter = PolicyFilter::new(format!("eligibility_{}", name), Box::new(policy));
    //     let tx_policy_api = policy_filter.tx_api();
    //     let tx_policy_monitor = policy_filter.tx_monitor.clone();
    //     let context_inlet = policy_filter.context_inlet();
    //
    //     let inlet = Inlet::new(format!("into_{}", name));
    //     let outlet = Outlet::new(format!("from_{}", name));
    //     tracing::warn!("CCC");
    //     let inner = InnerEligibility::new(
    //         name.as_str(),
    //         subscription,
    //         policy_filter,
    //         tx_clearinghouse,
    //         inlet.clone(),
    //         outlet.clone(),
    //     );
    //
    //     tracing::warn!("DDD");
    //     Ok(Self {
    //         name,
    //         inner: Some(inner),
    //         context_inlet,
    //         inlet,
    //         outlet,
    //         tx_policy_api,
    //         tx_policy_monitor,
    //     })
    // }

    // pub fn take_subscribe_command(&mut self) -> Option<(ClearinghouseCmd, oneshot::Receiver<Ack>)> {
    //     self.subscribe_command.take()
    // }

    #[inline]
    pub fn context_inlet(&self) -> Inlet<C> {
        self.context_inlet.clone()
    }
}

impl<D, C: Debug> Debug for Eligibility<D, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Eligibility")
            .field("name", &self.name)
            // .field("subscription", &self.subscription)
            .field("inner_stage", &self.inner_stage)
            .field("context_inlet", &self.context_inlet)
            .field("inlet", &self.inlet)
            .field("outlet", &self.outlet)
            .finish()
    }
}

trait InnerStage<D>: Stage + ThroughShape<In = D, Out = D> + 'static {}
impl<D, T: 'static + Stage + ThroughShape<In = D, Out = D>> InnerStage<D> for T {}

impl<D, C> SinkShape for Eligibility<D, C> {
    type In = D;
    #[inline]
    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

impl<D, C> SourceShape for Eligibility<D, C> {
    type Out = D;
    #[inline]
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<D: AppData, C: ProctorContext> Stage for Eligibility<D, C> {
    #[inline]
    fn name(&self) -> &str {
        self.name.as_ref()
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> GraphResult<()> {
        self.inlet.check_attachment().await?;
        self.context_inlet.check_attachment().await?;
        self.outlet.check_attachment().await?;
        if let Some(ref inner) = self.inner_stage {
            inner.check().await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run eligibility through", skip(self))]
    async fn run(&mut self) -> GraphResult<()> {
        match self.inner_stage.as_mut() {
            Some(inner) => inner.run().await,
            None => Err(GraphError::GraphPrecondition(
                "eligibility already spent - cannot run.".to_string(),
            )),
        }
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn close(mut self: Box<Self>) -> GraphResult<()> {
        tracing::trace!("closing eligibility ports.");
        self.inlet.close().await;
        // self.context_inlet.close().await;
        if let Some(inner) = self.inner_stage.take() {
            inner.close().await?;
        }
        self.outlet.close().await;
        Ok(())
    }
}

impl<D, C> WithApi for Eligibility<D, C> {
    type Sender = PolicyFilterApi<C>;
    #[inline]
    fn tx_api(&self) -> Self::Sender {
        self.tx_policy_api.clone()
    }
}

impl<D, C> WithMonitor for Eligibility<D, C> {
    type Receiver = PolicyFilterMonitor<D, C>;
    #[inline]
    fn rx_monitor(&self) -> Self::Receiver {
        self.tx_policy_monitor.subscribe()
    }
}

// enum InnerEligibility<C> {
//     Quiescent {
//         name: String,
//         subscription: TelemetrySubscription,
//         policy_filter: PolicyFilter<TelemetryData, C>,
//         tx_clearinghouse: ClearinghouseApi,
//         into_eligibility: Inlet<TelemetryData>,
//         from_eligibility: Outlet<TelemetryData>,
//     },
//     Active(Box<dyn InnerStage>),
// }
//
// impl<C: Debug> Debug for InnerEligibility<C> {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         match self {
//             Self::Quiescent {
//                 name,
//                 subscription,
//                 policy_filter,
//                 tx_clearinghouse: _,
//                 into_eligibility,
//                 from_eligibility,
//             } => f
//                 .debug_struct("Quiescent")
//                 .field("name", name)
//                 .field("subscription", subscription)
//                 .field("policy_filter", policy_filter)
//                 .field("into_eligibility", into_eligibility)
//                 .field("from_eligibility", from_eligibility)
//                 .finish(),
//             Self::Active(stage) => f.debug_struct("Active").field("stage", stage).finish(),
//         }
//     }
// }
//
// impl<C: ProctorContext> InnerEligibility<C> {
//     fn new<S: Into<String>>(
//         name: S, subscription: TelemetrySubscription, policy_filter: PolicyFilter<TelemetryData, C>,
//         tx_clearinghouse: ClearinghouseApi, into_eligibility: Inlet<TelemetryData>,
//         from_eligibility: Outlet<TelemetryData>,
//     ) -> Self {
//         Self::Quiescent {
//             name: name.into(),
//             subscription,
//             policy_filter,
//             tx_clearinghouse,
//             into_eligibility,
//             from_eligibility,
//         }
//     }
//
//     #[tracing::instrument(level = "info")]
//     async fn prepare_for_run(self) -> GraphResult<Self> {
//         match self {
//             Self::Quiescent {
//                 name,
//                 subscription,
//                 policy_filter,
//                 tx_clearinghouse,
//                 into_eligibility,
//                 from_eligibility,
//             } => {
//                 let (context_source, context_source_outlet) =
//                     Self::subscribe_eligibility_to_context(subscription, tx_clearinghouse).await?;
//                 let inner = Self::make_eligibility_inner(
//                     name.as_str(),
//                     context_source,
//                     context_source_outlet,
//                     policy_filter,
//                     into_eligibility,
//                     from_eligibility,
//                 )
//                 .await?;
//                 Ok(Self::Active(inner))
//             }
//             inner @ Self::Active(_) => Ok(inner),
//         }
//     }
//
//     #[tracing::instrument(level = "info", skip(context_source_outlet, into_eligibility, from_eligibility,))]
//     async fn make_eligibility_inner(
//         name: &str, context_source: Box<dyn Stage>, context_source_outlet: Outlet<C>,
//         policy_filter: PolicyFilter<TelemetryData, C>, into_eligibility: Inlet<TelemetryData>,
//         from_eligibility: Outlet<TelemetryData>,
//     ) -> GraphResult<Box<dyn InnerStage>> {
//         (context_source_outlet, policy_filter.context_inlet()).connect().await;
//
//         let into_graph = Outlet::new(format!("into-{}-eligibility-graph", name));
//         let policy_inlet = policy_filter.inlet();
//         let policy_outlet = policy_filter.outlet();
//         let from_graph = Inlet::new(format!("from-{}-eligibility-graph", name));
//
//         (&into_graph, &policy_inlet).connect().await;
//         (&policy_outlet, &from_graph).connect().await;
//
//         let in_bridge = stage::Identity::new(
//             format!("{}-bridge-into-eligibility", name),
//             into_eligibility.clone(),
//             into_graph,
//         );
//         let out_bridge = stage::Identity::new(
//             format!("{}-bridge-from-eligibility", name),
//             from_graph,
//             from_eligibility.clone(),
//         );
//
//         let mut graph = Graph::default();
//         graph.push_front(Box::new(in_bridge)).await;
//         graph.push_back(context_source).await;
//         graph.push_back(Box::new(policy_filter)).await;
//         graph.push_back(Box::new(out_bridge)).await;
//
//         let composite = stage::CompositeThrough::new(
//             format!("eligibility_composite_{}", name),
//             graph,
//             into_eligibility,
//             from_eligibility,
//         )
//         .await;
//
//         let result: Box<dyn InnerStage> = Box::new(composite);
//         Ok(result)
//     }
//
//     //todo: simplify return to a Stage + SourceShape<C> once upcasting is better support wrt type constraints and/or auto trait support is expanded.
//     // but until then settled on this approach to return outlet to be connected with stage.
//     #[tracing::instrument(level = "info")]
//     async fn subscribe_eligibility_to_context(
//         subscription: TelemetrySubscription, tx_clearinghouse: ClearinghouseApi,
//     ) -> GraphResult<(Box<dyn Stage>, Outlet<C>)> {
//         tracing::warn!("AAAA");
//         let convert_telemetry = crate::elements::make_from_telemetry::<C, _>(&subscription.name, true).await?;
//
//         tracing::warn!("BBBB");
//         let (cmd, ack) = ClearinghouseCmd::subscribe(subscription, convert_telemetry.inlet());
//         tx_clearinghouse.send(cmd)?;
//         tracing::warn!("CCCC");
//         ack.await?;
//         tracing::warn!("DDDD");
//
//         let outlet = convert_telemetry.outlet();
//         tracing::warn!("EEEE");
//         Ok((convert_telemetry.dyn_upcast(), outlet))
//     }
// }
//
// impl<C: ProctorContext> InnerEligibility<C> {
//     fn name(&self) -> &str {
//         match self {
//             Self::Quiescent { name, .. } => name.as_str(),
//             Self::Active(stage) => stage.name(),
//         }
//     }
//
//     #[tracing::instrument(level="info", skip(self), fields(stage=%self.name()))]
//     async fn run(&mut self) -> GraphResult<()> {
//         if let Self::Active(stage) = self {
//             stage.run().await
//         } else {
//             Err(crate::error::GraphError::GraphPrecondition(
//                 "eligibility needs pre-run initialization.".to_string(),
//             ))
//         }
//     }
//
//     #[tracing::instrument(level="info", skip(self), fields(stage=%self.name()))]
//     async fn close(self) -> GraphResult<()> {
//         match self {
//             Self::Active(stage) => stage.close().await,
//             _ => Ok(()),
//         }
//     }
// }