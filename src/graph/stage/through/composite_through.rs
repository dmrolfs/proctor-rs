use std::fmt::Debug;

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;

use crate::graph::shape::{SinkShape, SourceShape};
use crate::graph::{stage, Connect, Graph, Inlet, Outlet, Port, Stage};
use crate::{AppData, ProctorResult};

/// Through shape that encapsulates externally created stages, supporting graph stage composition.
///
/// Examples
///
/// ```rust
/// use proctor::graph::stage::{self, Stage};
/// use proctor::graph::{Connect, Graph};
/// use proctor::graph::{SinkShape, SourceShape};
/// use proctor::tracing::{get_subscriber, init_subscriber};
/// use futures::future;
/// use proctor::error::StageError;
///
/// #[tokio::main(flavor = "multi_thread", worker_threads = 16)]
/// async fn main() -> anyhow::Result<()> {
/// let subscriber = get_subscriber("sandbox", "trace");
///     init_subscriber(subscriber);
///
///     let main_span = tracing::info_span!("main");
///     let _main_span_guard = main_span.enter();
///
///     tracing::info!("Nr CPUS:{}", num_cpus::get());
///
///     let mut source = stage::Sequence::new("I. Sequence Source", (4..=10));
///
///     let bar = "17".to_string();
///     let mut sq_plus = stage::AndThen::new("II.a. AndThen-sq_plus", move |x| {
///         future::ready(x * x + bar.parse::<i32>().expect("failed to parse bar."))
///     });
///
///     let mut add_five = stage::AndThen::new("II.b AndThen-add_five", |x: i32| {
///         tracing::info!(?x, "adding 5 to x");
///         future::ready(x + 5)
///     });
///     let cg_inlet = sq_plus.inlet().clone();
///     (sq_plus.outlet(), add_five.inlet()).connect().await;
///     let cg_outlet = add_five.outlet().clone();
///
///     tracing::info!("cg_inlet is_attached:{}", cg_inlet.is_attached().await);
///     assert!(!cg_inlet.is_attached().await);
///     assert!(sq_plus.outlet().is_attached().await);
///     assert!(add_five.inlet().is_attached().await);
///     assert!(!cg_outlet.is_attached().await);
///
///     let mut cg = Graph::default();
///     cg.push_back(Box::new(sq_plus)).await;
///     cg.push_back(Box::new(add_five)).await;
///     let mut composite = stage::CompositeThrough::new("II. CompositeThrough-middle", cg, cg_inlet.clone(), cg_outlet.clone()).await;
///
///     let mut fold = stage::Fold::<_, i32, i32>::new("III. Fold-sum", 0, |acc, x| {
///         tracing::info!(%acc, ?x, "folding received value.");
///         acc + x
///     });
///     let rx_sum_sq = fold.take_final_rx().unwrap();
///
///     (source.outlet(), composite.inlet()).connect().await;
///     (composite.outlet(), fold.inlet()).connect().await;
///
///     assert!(cg_inlet.is_attached().await);
///     assert!(cg_outlet.is_attached().await);
///
///     assert!(source.outlet().is_attached().await);
///     assert!(composite.inlet().is_attached().await);
///     assert!(composite.outlet().is_attached().await);
///     assert!(fold.inlet().is_attached().await);
///
///     let mut g = Graph::default();
///     g.push_back(Box::new(source)).await;
///     g.push_back(Box::new(composite)).await;
///     g.push_back(Box::new(fold)).await;
///     g.run().await?;
///
///     let result = rx_sum_sq.await?;
///     assert_eq!(result, 525);
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct CompositeThrough<In, Out> {
    name: String,
    graph: Option<Graph>,
    inlet: Inlet<In>,
    outlet: Outlet<Out>,
}

impl<In: AppData, Out: AppData> CompositeThrough<In, Out> {
    pub async fn new(name: impl Into<String>, graph: Graph, graph_inlet: Inlet<In>, graph_outlet: Outlet<Out>) -> Self {
        let name = name.into();
        let (graph, inlet, outlet) = Self::extend_graph(name.as_ref(), graph, graph_inlet, graph_outlet).await;
        Self { name, graph: Some(graph), inlet, outlet }
    }

    async fn extend_graph(
        name: &str, mut graph: Graph, graph_inlet: Inlet<In>, graph_outlet: Outlet<Out>,
    ) -> (Graph, Inlet<In>, Outlet<Out>) {
        let composite_inlet = Inlet::new(format!("{}__[composite_inlet]", name));
        let into_graph = Outlet::new(format!("{}__into_composite_graph", name));
        let from_graph = Inlet::new(format!("{}__from_composite_graph", name));
        let composite_outlet = Outlet::new(format!("{}__[composite_outlet]", name));

        (&into_graph, &graph_inlet).connect().await;
        (&graph_outlet, &from_graph).connect().await;

        let in_bridge = stage::Identity::new(
            format!("{}__composite_ingress_bridge", name),
            composite_inlet.clone(),
            into_graph,
        );
        let out_bridge = stage::Identity::new(
            format!("{}__composite_egress_bridge", name),
            from_graph,
            composite_outlet.clone(),
        );

        graph.push_front(Box::new(in_bridge)).await;
        graph.push_back(Box::new(out_bridge)).await;

        (graph, composite_inlet, composite_outlet)
    }
}

impl<In, Out> SourceShape for CompositeThrough<In, Out> {
    type Out = Out;

    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

impl<In, Out> SinkShape for CompositeThrough<In, Out> {
    type In = In;

    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<In: AppData, Out: AppData> Stage for CompositeThrough<In, Out> {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.inlet.check_attachment().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run composite through", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        match self.graph.take() {
            None => Ok(()),
            Some(g) => g.run().await,
        }
    }

    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        tracing::trace!("closing composite graph, inlet and outlet.");
        self.inlet.close().await;
        self.outlet.close().await;
        Ok(())
    }
}
