//todo: collect once every minute for scaling metrics

/// Tailorable collection source: plugin mechanism to pull metrics from various sources.
///
/// # Examples
///
/// ```rust
/// #[macro_use]
/// extern crate proctor_derive;
///
/// use proctor::elements::Telemetry;
/// use proctor::elements::telemetry::ToTelemetry;
/// use proctor::error::GraphError;
/// use proctor::graph::stage::{self, tick, Stage};
/// use proctor::graph::{Connect, Graph, GraphResult, SinkShape, SourceShape};
/// use proctor::tracing::{get_subscriber, init_subscriber};
/// use proctor::AppData;
/// use futures::future::FutureExt;
/// use reqwest::header::HeaderMap;
/// use serde::Deserialize;
/// use std::collections::{BTreeMap, HashMap};
/// use std::sync::Arc;
/// use std::time::Duration;
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
///     let subscriber = get_subscriber("sandbox", "trace");
///     init_subscriber(subscriber);
///
///     let main_span = tracing::info_span!("main");
///     let _main_span_guard = main_span.enter();
///
///     let mut tick = stage::Tick::with_constraint(
///         "tick",
///         Duration::from_nanos(0),
///         Duration::from_nanos(1),
///         (),
///         tick::Constraint::by_count(3),
///     );
///
///     //dmr: this is a hard fought example of how to modify an counter within an async closure.
///     //dmr: important part is two-layer closure.
///     //dmr: https://www.fpcomplete.com/blog/captures-closures-async/
///     let count = Arc::new(Mutex::new(0_usize));
///
///     let gen = move |_| {
///         let cc = count.clone();
///
///         async move {
///             let url = "https://httpbin.org/get?f=foo&b=bar";
///             let mut default_headers = HeaderMap::new();
///             default_headers.insert("x-api-key", "fe37af1e07mshd1763d86e5f2a8cp1714cfjsnb6145a35e7ca".parse().unwrap());
///             let client = reqwest::Client::builder().default_headers(default_headers).build().unwrap();
///             let resp = client
///                 .get(url)
///                 .send()
///                 .await
///                 .unwrap()
///                 .json::<HttpBinResponse>()
///                 .await
///                 .map_err::<GraphError, _>(|err| err.into())
///                 .unwrap();
///
///             let mine = cc.clone();
///             let mut my_count = mine.lock().await;
///             *my_count += 1;
///
///             let mut data = Telemetry::new();
///             for (k, v) in resp.args {
///                 data.insert(format!("args.{}.{}", my_count, k), v.into());
///             }
///
///             data
///         }
///     };
///
///     let mut httpbin_collection = stage::TriggeredGenerator::new("httpbin_collection", gen);
///
///     let mut fold = stage::Fold::new("gather latest", None, |acc: Option<Telemetry>, data: Telemetry| {
///         acc.map_or(Some(data.clone()), move |a| Some(a + data.clone()))
///     });
///     let rx_gather = fold.take_final_rx().unwrap();
///
///     (tick.outlet(), httpbin_collection.inlet()).connect().await;
///     (httpbin_collection.outlet(), fold.inlet()).connect().await;
///
///     let mut g = Graph::default();
///     g.push_back(Box::new(tick)).await;
///     g.push_back(Box::new(httpbin_collection)).await;
///     g.push_back(Box::new(fold)).await;
///     g.run().await?;
///
///     match rx_gather.await.expect("fold didn't release anything.") {
///         Some(resp) => {
///             let mut exp = Telemetry::new();
///             for i in 1..=3 {
///                 exp.insert(format!("args.{}.f", i), "foo".into());
///                 exp.insert(format!("args.{}.b", i), "bar".into());
///             }
///             tracing::warn!(actual=?resp,expected=?exp, "validating results");
///             assert_eq!(resp, exp);
///         }
///         None => panic!("did not expect no response"),
///     }
///
///     Ok(())
/// }
/// ```
pub type TriggeredGenerator<Gen, Fut, Out> = crate::graph::stage::AndThen<Gen, Fut, (), Out>;
