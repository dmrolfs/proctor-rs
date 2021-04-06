use super::TelemetryData;
use crate::graph::stage::{self, Stage};
use crate::graph::{Connect, Graph, GraphResult, Inlet, Outlet, Port, Shape, SinkShape, SourceShape, ThroughShape};
use crate::AppData;
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use reqwest::header::HeaderMap;
use reqwest::{IntoUrl, Url};
use serde::de::DeserializeOwned;
use std::fmt;

//todo: collect once every minute for scaling metrics

/// Tailorable collection source: plugin mechanism to pull metrics from various sources.
///
/// # Examples
///
/// ```
/// #[macro_use]
/// extern crate proctor_derive;
///
/// use proctor::AppData;
/// use proctor::elements;
/// use proctor::graph::stage::{self, tick, Stage};
/// use proctor::graph::{Connect, Graph, SinkShape, SourceShape, ThroughShape};
/// use reqwest::header::HeaderMap;
/// use serde::Deserialize;
/// use std::collections::HashMap;
/// use std::time::Duration;
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
///     let url = "https://httpbin.org/get?f=foo&b=bar";
///     let mut default_headers = HeaderMap::new();
///     default_headers.insert("x-api-key", "fe37af1e07mshd1763d86e5f2a8cp1714cfjsnb6145a35e7ca".parse().unwrap());
///
///     let mut count = 0;
///
///     let mut tick = stage::Tick::with_constraint(
///         "tick",
///         Duration::from_nanos(0),
///         Duration::from_millis(50),
///         (),
///         tick::Constraint::by_count(3),
///     );
///
///     let to_metric_catalog = move |r: HttpBinResponse| {
///         let cnt = &mut count;
///         *cnt += 1;
///         let mut data = HashMap::new();
///         for (k, v) in &r.args {
///             data.insert(format!("args.{}.{}", cnt, k), v.to_string());
///         }
///         elements::TelemetryData::from_data(data)
///     };
///
///     let mut collect = elements::Collect::new(
///         "collect-args",
///         url,
///         default_headers,
///         to_metric_catalog,
///     )
///     .await;
///
///     let mut fold = stage::Fold::<_, elements::TelemetryData, _>::new(
///         "gather latest",
///         None,
///         |acc: Option<elements::TelemetryData>, data| {
///             acc.map_or(Some(data.clone()), move |a| Some(a + data.clone()))
///         }
///     );
///     let rx_gather = fold.take_final_rx().unwrap();
///
///     (tick.outlet(), collect.inlet()).connect().await;
///     (collect.outlet(), fold.inlet()).connect().await;
///
///     let mut g = Graph::default();
///     g.push_back(Box::new(tick)).await;
///     g.push_back(Box::new(collect)).await;
///     g.push_back(Box::new(fold)).await;
///     g.run().await?;
///
///     match rx_gather.await.expect("fold didn't release anything.") {
///         Some(resp) => {
///             let mut exp = HashMap::new();
///             for i in 1..=3 {
///                 exp.insert(format!("args.{}.f", i), "foo".to_string());
///                 exp.insert(format!("args.{}.b", i), "bar".to_string());
///             }
///             let exp = elements::TelemetryData::from_data(exp);
///             assert_eq!(resp, exp);
///         }
///         None => panic!("did not expect no response"),
///     }
///
///     Ok(())
/// }
/// ```
pub struct Collect {
    name: String,
    target: Url,
    graph: Option<Graph>,
    trigger: Inlet<()>,
    outlet: Outlet<TelemetryData>,
}

impl Collect {
    pub async fn new<T, F, S, U>(name: S, url: U, default_headers: HeaderMap, transform: F) -> Self
    where
        T: AppData + DeserializeOwned + 'static,
        F: FnMut(T) -> TelemetryData + Send + 'static,
        S: Into<String>,
        U: IntoUrl,
    {
        let name = name.into();
        let target = url.into_url().expect("failed to parse url");
        let (graph, trigger, outlet) = Collect::make_graph::<T, _>(name.clone(), target.clone(), default_headers, transform).await;

        Self {
            name,
            target,
            graph: Some(graph),
            trigger,
            outlet,
        }
    }

    async fn make_graph<T, F>(name: String, url: Url, default_headers: HeaderMap, transform: F) -> (Graph, Inlet<()>, Outlet<TelemetryData>)
    where
        T: AppData + DeserializeOwned + 'static,
        F: FnMut(T) -> TelemetryData + Send + 'static,
    {
        let mut query = stage::AndThen::new(format!("{}-query", name), move |_| {
            let client = reqwest::Client::builder().default_headers(default_headers.clone()).build().unwrap();
            let query_url = url.clone();
            async move { Collect::do_query::<T>(&client, query_url).await.expect("failed to query") }
        });
        let mut transform = stage::Map::<_, T, TelemetryData>::new(format!("{}-transform", name), transform);

        let bridge_inlet = Inlet::new("into_collection_graph");

        let mut in_bridge = stage::Identity::new(
            format!("{}-trigger-bridge", name),
            bridge_inlet.clone(),
            Outlet::new("from_collection_graph"),
        );

        let bridge_outlet = Outlet::new("collection-outlet");
        let mut out_bridge = stage::Identity::new(
            format!("{}-output-bridge", name),
            Inlet::new("from_collection_graph"),
            bridge_outlet.clone(),
        );

        (in_bridge.outlet(), query.inlet()).connect().await;
        (query.outlet(), transform.inlet()).connect().await;
        (transform.outlet(), out_bridge.inlet()).connect().await;

        let mut graph = Graph::default();
        graph.push_back(Box::new(in_bridge)).await;
        graph.push_back(Box::new(query)).await;
        graph.push_back(Box::new(transform)).await;
        graph.push_back(Box::new(out_bridge)).await;

        (graph, bridge_inlet, bridge_outlet)
    }

    #[tracing::instrument(
        level="info",
        name="query url",
        fields(%url),
    )]
    async fn do_query<T>(client: &reqwest::Client, url: Url) -> GraphResult<T>
    where
        T: DeserializeOwned + fmt::Debug,
    {
        client.get(url).send().await?.json().await.map_err(|err| err.into())
    }
}

impl Shape for Collect {}

impl ThroughShape for Collect {}

impl SourceShape for Collect {
    type Out = TelemetryData;
    #[inline]
    fn outlet(&mut self) -> &mut Outlet<Self::Out> {
        &mut self.outlet
    }
}

impl SinkShape for Collect {
    type In = ();
    #[inline]
    fn inlet(&mut self) -> &mut Inlet<Self::In> {
        &mut self.trigger
    }
}

#[dyn_upcast]
#[async_trait]
impl Stage for Collect {
    #[inline]
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(level = "info", name = "run collect source", skip(self))]
    async fn run(&mut self) -> GraphResult<()> {
        let foo = match self.graph.take() {
            None => Ok(()),
            Some(g) => g.run().await,
        };
        foo
    }

    async fn close(mut self: Box<Self>) -> GraphResult<()> {
        tracing::trace!("closing collect inner graph and outlet.");
        self.outlet.close().await;
        Ok(())
    }
}

impl fmt::Debug for Collect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Collect")
            .field("target_url", &self.target)
            .field("graph", &self.graph)
            .field("outlet", &self.outlet)
            .finish()
    }
}
