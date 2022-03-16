use std::fmt::{self, Debug};

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use reqwest::header::HeaderMap;
use serde::de::DeserializeOwned;
use url::Url;

use super::Telemetry;
use crate::error::SenseError;
use crate::graph::stage::{self, Stage};
use crate::graph::{Connect, Graph, Inlet, Outlet, Port, SinkShape, SourceShape};
use crate::{AppData, ProctorResult, SharedString};

// todo: collect once every minute for scaling metrics

/// Tailorable sense sensor: plugin mechanism to pull metrics from various sources.
///
/// # Examples
///
/// ```
/// #[macro_use]
/// extern crate proctor_derive;
///
/// use std::collections::{BTreeMap, HashMap};
/// use std::iter::FromIterator;
/// use std::time::Duration;
///
/// use proctor::elements;
/// use proctor::elements::telemetry::ToTelemetry;
/// use proctor::graph::stage::{self, tick, Stage};
/// use proctor::graph::{Connect, Graph, SinkShape, SourceShape, ThroughShape};
/// use proctor::AppData;
/// use reqwest::header::HeaderMap;
/// use serde::Deserialize;
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
///     let url = url::Url::parse("https://httpbin.org/get?f=foo&b=bar")?;
///     let mut default_headers = HeaderMap::new();
///     default_headers.insert(
///         "x-api-key",
///         "fe37af1e07mshd1763d86e5f2a8cp1714cfjsnb6145a35e7ca".parse().unwrap(),
///     );
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
///         let mut data = BTreeMap::new();
///         for (k, v) in &r.args {
///             let tv = v.as_str().to_telemetry();
///             data.insert(format!("args.{}.{}", cnt, k), tv);
///         }
///         elements::Telemetry::try_from(&data).unwrap()
///     };
///
///     let mut collect = elements::Collect::new("collect-args".into(), url, default_headers, to_metric_catalog).await;
///
///     let mut fold = stage::Fold::<_, elements::Telemetry, _>::new(
///         "gather latest",
///         None,
///         |acc: Option<elements::Telemetry>, data| acc.map_or(Some(data.clone()), move |a| Some(a + data.clone())),
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
///             let mut exp = BTreeMap::new();
///             for i in 1..=3 {
///                 exp.insert(format!("args.{}.f", i), "foo".into());
///                 exp.insert(format!("args.{}.b", i), "bar".into());
///             }
///             assert_eq!(resp, exp.into_iter().collect());
///         },
///         None => panic!("did not expect no response"),
///     }
///
///     Ok(())
/// }
/// ```
///
pub struct Collect {
    name: SharedString,
    target: Url,
    graph: Option<Graph>,
    trigger: Inlet<()>,
    outlet: Outlet<Telemetry>,
}

impl Collect {
    #[deprecated(since = "0.3.0", note = "prefer proctor::phases::sense module.")]
    pub async fn new<T, F>(name: SharedString, url: Url, default_headers: HeaderMap, transform: F) -> Self
    where
        T: AppData + DeserializeOwned + 'static,
        F: FnMut(T) -> Telemetry + Send + Sync + 'static,
    {
        let (graph, trigger, outlet) =
            Self::make_graph::<T, _>(name.clone(), url.clone(), default_headers, transform).await;

        Self {
            name,
            target: url,
            graph: Some(graph),
            trigger,
            outlet,
        }
    }

    async fn make_graph<T, F>(
        name: SharedString, url: Url, default_headers: HeaderMap, transform: F,
    ) -> (Graph, Inlet<()>, Outlet<Telemetry>)
    where
        T: AppData + DeserializeOwned + 'static,
        F: FnMut(T) -> Telemetry + Send + Sync + 'static,
    {
        let query = stage::AndThen::new(format!("{}-query", name), move |_| {
            let client = reqwest::Client::builder()
                .default_headers(default_headers.clone())
                .build()
                .unwrap();
            let query_url = url.clone();
            async move { Self::do_query::<T>(&client, query_url).await.expect("failed to query") }
        });
        let transform = stage::Map::<_, T, Telemetry>::new(format!("{}-transform", name), transform);

        let bridge_inlet = Inlet::new(name.clone(), "sense");

        let in_bridge = stage::Identity::new(
            format!("{}-trigger-bridge", name),
            bridge_inlet.clone(),
            Outlet::new(name.clone(), "from_collection_graph"),
        );

        let bridge_outlet = Outlet::new(name.clone(), "sense");
        let out_bridge = stage::Identity::new(
            format!("{}-output-bridge", name),
            Inlet::new(name.clone(), "from_collection_graph"),
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
        level="trace",
        name="query url",
        fields(%url),
    )]
    async fn do_query<T>(client: &reqwest::Client, url: Url) -> Result<T, SenseError>
    where
        T: DeserializeOwned + fmt::Debug,
    {
        client.get(url).send().await?.json().await.map_err(|err| err.into())
    }
}

impl SourceShape for Collect {
    type Out = Telemetry;

    #[inline]
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

impl SinkShape for Collect {
    type In = ();

    #[inline]
    fn inlet(&self) -> Inlet<Self::In> {
        self.trigger.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl Stage for Collect {
    #[inline]
    fn name(&self) -> SharedString {
        self.name.clone()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", name = "run collect source", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        self.do_run().await?;
        Ok(())
    }

    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        self.do_close().await?;
        Ok(())
    }
}

impl Collect {
    #[inline]
    async fn do_check(&self) -> Result<(), SenseError> {
        self.trigger.check_attachment().await?;
        self.outlet.check_attachment().await?;
        if let Some(ref g) = self.graph {
            g.check().await.map_err(|err| SenseError::Stage(err.into()))?;
        }
        Ok(())
    }

    #[inline]
    async fn do_run(&mut self) -> Result<(), SenseError> {
        if let Some(g) = self.graph.take() {
            g.run().await.map_err(|err| SenseError::Stage(err.into()))?;
        }

        Ok(())
    }

    #[inline]
    async fn do_close(mut self: Box<Self>) -> Result<(), SenseError> {
        tracing::trace!("closing collect inner graph and outlet.");
        self.outlet.close().await;
        Ok(())
    }
}

impl Debug for Collect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Collect")
            .field("target_url", &self.target)
            .field("graph", &self.graph)
            .field("outlet", &self.outlet)
            .finish()
    }
}
