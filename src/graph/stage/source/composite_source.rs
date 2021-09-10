use std::fmt::Debug;

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;

use crate::graph::shape::SourceShape;
use crate::graph::{stage, Connect, Graph, Inlet, Outlet, Port, Stage};
use crate::{AppData, ProctorResult};

/// Source shape that encapsulates externally created stages, supporting graph stage composition.
///
/// Examples
///
/// ```rust
/// #[macro_use]
/// extern crate proctor_derive;
///
/// use std::collections::{BTreeMap, HashMap};
/// use std::iter::FromIterator;
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// use futures::future::FutureExt;
/// use proctor::elements::telemetry::ToTelemetry;
/// use proctor::elements::Telemetry;
/// use proctor::graph::stage::{self, tick, Stage};
/// use proctor::graph::{Connect, Graph, SinkShape, SourceShape};
/// use proctor::tracing::{get_subscriber, init_subscriber};
/// use proctor::AppData;
/// use reqwest::header::HeaderMap;
/// use serde::Deserialize;
/// use tokio::sync::Mutex;
///
/// #[derive(Debug, Clone, Deserialize)]
/// pub struct HttpBinResponse {
///     pub args: HashMap<String, String>,
///     pub headers: HashMap<String, String>,
///     pub origin: String,
///     pub url: String,
/// }
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let subscriber = get_subscriber("eth_scan", "trace");
///     init_subscriber(subscriber);
///
///     let main_span = tracing::info_span!("main");
///     let _main_span_guard = main_span.enter();
///
///     // dmr: this is a hard fought example of how to modify an counter within an async closure.
///     // dmr: important part is two-layer closure.
///     // dmr: https://www.fpcomplete.com/blog/captures-closures-async/
///     let count = Arc::new(Mutex::new(0_usize));
///
///     let mut tick = stage::Tick::with_constraint(
///         "tick",
///         Duration::from_nanos(0),
///         Duration::from_millis(50),
///         (),
///         tick::Constraint::by_count(3),
///     );
///
///     let gen = move |_| {
///         let cc = Arc::clone(&count);
///
///         let to_telemetry_data = move |r: HttpBinResponse| async move {
///             let mine = cc.clone();
///             let mut cnt = mine.lock().await;
///             *cnt += 1;
///             let mut data = BTreeMap::new();
///             for (k, v) in &r.args {
///                 let tv = v.as_str().to_telemetry();
///                 data.insert(format!("args.{}.{}", cnt, k), tv);
///             }
///             Telemetry::try_from(&data).unwrap()
///         };
///
///         async move {
///             let url = "https://httpbin.org/get?f=foo&b=bar";
///             let mut default_headers = HeaderMap::new();
///             default_headers.insert(
///                 "x-api-key",
///                 "fe37af1e07mshd1763d86e5f2a8cp1714cfjsnb6145a35e7ca".parse().unwrap(),
///             );
///             let client = reqwest::Client::builder().default_headers(default_headers).build()?;
///             let resp = client
///                 .get(url)
///                 .send()
///                 .await?
///                 .json::<HttpBinResponse>()
///                 .await
///                 .map_err::<anyhow::Error, _>(|err| err.into())?;
///
///             let result: anyhow::Result<Telemetry> = Ok(to_telemetry_data(resp).await);
///             result
///         }
///         .map(|r| r.unwrap())
///     };
///
///     let mut generator = stage::TriggeredGenerator::new("generator", gen);
///
///     let composite_outlet = generator.outlet().clone();
///
///     (tick.outlet(), generator.inlet()).connect().await;
///
///     let mut cg = Graph::default();
///     cg.push_back(Box::new(tick)).await;
///     cg.push_back(Box::new(generator)).await;
///     let mut composite = stage::CompositeSource::new("composite_source", cg, composite_outlet).await;
///
///     let mut fold = stage::Fold::<_, Telemetry, _>::new("gather latest", Telemetry::new(), |mut acc, mg| {
///         acc.extend(mg);
///         acc
///     });
///     let rx_gather = fold.take_final_rx().unwrap();
///
///     (composite.outlet(), fold.inlet()).connect().await;
///
///     let mut g = Graph::default();
///     g.push_back(Box::new(composite)).await;
///     g.push_back(Box::new(fold)).await;
///     g.run().await?;
///
///     let actual = rx_gather.await.expect("fold didn't release");
///     assert_eq!(
///         actual,
///         maplit::hashmap! {
///             "args.1.f" => "foo".to_telemetry(),
///             "args.1.b" => "bar".to_telemetry(),
///             "args.2.f" => "foo".to_telemetry(),
///             "args.2.b" => "bar".to_telemetry(),
///             "args.3.f" => "foo".to_telemetry(),
///             "args.3.b" => "bar".to_telemetry(),
///         }
///         .into_iter()
///         .collect()
///     );
///
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct CompositeSource<Out> {
    name: String,
    graph: Option<Graph>,
    outlet: Outlet<Out>,
}

impl<Out: AppData> CompositeSource<Out> {
    pub async fn new(name: impl Into<String>, graph: Graph, graph_outlet: Outlet<Out>) -> Self {
        let name = name.into();
        let (graph, outlet) = Self::extend_graph(name.as_ref(), graph, graph_outlet).await;
        Self { name, graph: Some(graph), outlet }
    }

    async fn extend_graph(name: &str, mut graph: Graph, graph_outlet: Outlet<Out>) -> (Graph, Outlet<Out>) {
        let from_graph = Inlet::new("from_graph");
        (&graph_outlet, &from_graph).connect().await;
        let composite_outlet = Outlet::new(format!("{}_outlet", name));
        let bridge = stage::Identity::new(format!("{}_bridge", name), from_graph, composite_outlet.clone());

        graph.push_back(Box::new(bridge)).await;
        (graph, composite_outlet)
    }
}

impl<Out> SourceShape for CompositeSource<Out> {
    type Out = Out;

    #[inline]
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<Out: AppData> Stage for CompositeSource<Out> {
    #[inline]
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.outlet.check_attachment().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run composite source", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        match self.graph.take() {
            None => Ok(()),
            Some(g) => g.run().await,
        }
    }

    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        tracing::trace!("closing composite graph and outlet.");
        self.outlet.close().await;
        Ok(())
    }
}
