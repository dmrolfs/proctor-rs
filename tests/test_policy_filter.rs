mod fixtures;

use oso::{Oso, PolarClass};
use proctor::elements::{self, PolicyContext, TelemetryData, PolicyFilterEvent};
use proctor::graph::stage::{self, WithApi, WithMonitor};
use proctor::graph::{Connect, Graph, GraphResult, Port, SinkShape, SourceShape};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use std::any::{Any, TypeId};


#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestItem {
    #[polar(attribute)]
    pub flow: TestFlowMetrics,
    #[polar(attribute)]
    pub inbox_lag: u32,
}

impl TestItem {
    pub fn new(input_messages_per_sec: f64, inbox_lag: u32) -> Self {
        Self {
            flow: TestFlowMetrics { input_messages_per_sec, },
            inbox_lag,
        }
    }

    pub fn input_messages_per_sec(&self) -> f64 {
        self.flow.input_messages_per_sec
    }
}

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestFlowMetrics {
    #[polar(attribute)]
    pub input_messages_per_sec: f64,
}

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestEnvironment {
    #[polar(attribute)]
    pub location_code: u32,
    #[polar(attribute)]
    pub custom: HashMap<String, String>,
}

impl TestEnvironment {
    pub fn new(location_code: u32) -> Self {
        Self { location_code, custom: HashMap::default() }
    }

    pub fn with_custom(mut self, custom: HashMap<String, String>) -> Self {
        Self { custom, ..self }
    }
}

#[derive(Debug)]
struct TestPolicy {
    description: String,
    policy: String,
}

impl TestPolicy {
    pub fn new<S0: Into<String>, S1: Into<String>>(description: S0, policy: S1) -> Self {
        Self {
            description: description.into(),
            policy: policy.into(),
        }
    }
}

impl PolicyContext for TestPolicy {
    type Item = TestItem;
    type Environment = TestEnvironment;

    #[inline]
    fn description(&self) -> &str {
        self.description.as_str()
    }

    fn load_policy(&self, oso: &mut Oso) -> GraphResult<()> {
        oso.load_str(self.policy.as_str()).map_err(|err| err.into())
    }

    fn initialize(&self, oso: &mut Oso) -> GraphResult<()> {
        oso.register_class(
            TestItem::get_polar_class_builder()
                .name("TestMetricCatalog")
                .add_method("input_messages_per_sec", TestItem::input_messages_per_sec)
                .build(),
        )?;

        Ok(())
    }

    fn do_query_rule(&self, oso: &Oso, item_env: (Self::Item, Self::Environment)) -> GraphResult<oso::Query> {
        oso.query_rule("eligible", item_env).map_err(|err| err.into())
    }
}

struct TestFlow {
    pub graph_handle: JoinHandle<()>,
    pub tx_item_source_api: stage::ActorSourceApi<TestItem>,
    pub tx_env_source_api: stage::ActorSourceApi<TestEnvironment>,
    pub tx_policy_api: elements::PolicyFilterApi<TestEnvironment>,
    pub rx_policy_monitor: elements::PolicyFilterMonitor<TestItem, TestEnvironment>,
    pub tx_sink_api: stage::FoldApi<Vec<TestItem>>,
    pub rx_sink: Option<oneshot::Receiver<Vec<TestItem>>>,
}

impl TestFlow {
    pub async fn new<S0: Into<String>, S1: Into<String>>(description: S0, policy: S1) -> Self {
        let mut item_source = stage::ActorSource::<TestItem>::new("item_source");
        let tx_item_source_api = item_source.tx_api();

        let mut env_source = stage::ActorSource::<TestEnvironment>::new("env_source");
        let tx_env_source_api = env_source.tx_api();

        let policy = Box::new(TestPolicy::new(description, policy));
        let mut policy_filter = elements::PolicyFilter::new("eligibility", policy);
        let tx_policy_api = policy_filter.tx_api();
        let rx_policy_monitor = policy_filter.rx_monitor();

        let mut sink = stage::Fold::<_, TestItem, _>::new("sink", Vec::new(), |mut acc, item| {
            acc.push(item);
            acc
        });
        let tx_sink_api = sink.tx_api();
        let rx_sink = sink.take_final_rx();

        (item_source.outlet(), policy_filter.inlet()).connect().await;
        (env_source.outlet(), policy_filter.environment_inlet()).connect().await;
        (policy_filter.outlet(), sink.inlet()).connect().await;

        let mut graph = Graph::default();
        graph.push_back(Box::new(item_source)).await;
        graph.push_back(Box::new(env_source)).await;
        graph.push_back(Box::new(policy_filter)).await;
        graph.push_back(Box::new(sink)).await;
        let graph_handle = tokio::spawn(async move { graph.run().await.expect("graph run failed") });

        Self {
            graph_handle,
            tx_item_source_api,
            tx_env_source_api,
            tx_policy_api,
            rx_policy_monitor,
            tx_sink_api,
            rx_sink,
        }
    }

    pub async fn push_item(&self, item: TestItem) -> GraphResult<()> {
        let (cmd, ack) = stage::ActorSourceCmd::push(item);
        self.tx_item_source_api.send(cmd)?;
        ack.await.map_err(|err| err.into())
    }

    pub async fn push_environment(&self, env: TestEnvironment) -> GraphResult<()> {
        let (cmd, ack) = stage::ActorSourceCmd::push(env);
        self.tx_env_source_api.send(cmd)?;
        ack.await.map_err(|err| err.into())
    }

    pub async fn recv_policy_event(&mut self) -> GraphResult<elements::PolicyFilterEvent<TestItem, TestEnvironment>> {
        self.rx_policy_monitor.recv().await.map_err(|err| err.into())
    }

    pub async fn inspect_policy(&self) -> GraphResult<elements::PolicyFilterDetail<TestEnvironment>> {
        let (cmd, detail) = elements::PolicyFilterCmd::inspect();
        self.tx_policy_api.send(cmd)?;
        detail
            .await
            .map(|d| {
                tracing::info!(detail=?d, "inspected policy.");
                d
            })
            .map_err(|err| err.into())
    }

    pub async fn inspect_sink(&self) -> GraphResult<Vec<TestItem>> {
        let (cmd, acc) = stage::FoldCmd::get_accumulation();
        self.tx_sink_api.send(cmd)?;
        acc
            .await
            .map(|a| {
                tracing::info!(accumulation=?a, "inspected sink accumulation");
                a
            })
            .map_err(|err| err.into())
    }

    pub async fn close(mut self) -> GraphResult<Vec<TestItem>> {
        let (stop, _) = stage::ActorSourceCmd::stop();
        self.tx_item_source_api.send(stop)?;

        let (stop, _) = stage::ActorSourceCmd::stop();
        self.tx_env_source_api.send(stop)?;

        self.graph_handle.await?;

        self.rx_sink.take().unwrap().await.map_err(|err| err.into())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_policy_filter_happy_environment() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::telemetry::TEST_TRACING);
    let main_span = tracing::info_span!("test_policy_filter_w_pass_and_blocks");
    let _ = main_span.enter();

    // -- set up tests
    let flow = TestFlow::new("by_location", r#"eligible(item, environment) if environment.location_code == 33;"#).await;

    tracing::info!("DMR: 01. Make sure empty env...");

    let detail = flow.inspect_policy().await?;
    assert!(detail.environment.is_none());

    tracing::info!("DMR: 02. Push environment...");

    flow.push_environment(TestEnvironment::new(33)).await?;

    tracing::info!("DMR: 03. Verify environment set...");

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let detail = flow.inspect_policy().await?;
    assert!(detail.environment.is_some());

    tracing::info!("DMR: 04. Push Item...");

    let item = TestItem::new(std::f64::consts::PI, 1,);
    flow.push_item(item).await?;

    tracing::info!("DMR: 05. Look for Item in sink...");

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let actual = flow.inspect_sink().await?;
    assert_eq!(actual, vec![TestItem::new(std::f64::consts::PI, 1),]);

    tracing::info!("DMR: 06. Push another Item...");

    let item = TestItem::new(std::f64::consts::TAU, 2);
    flow.push_item(item).await?;

    tracing::info!("DMR: 07. Close flow...");

    let actual = flow.close().await?;

    tracing::info!(?actual, "DMR: 08. Verify final accumulation...");

    assert_eq!(
        actual,
        vec![
            TestItem::new(std::f64::consts::PI, 1),
            TestItem::new(std::f64::consts::TAU, 2),
        ]
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_policy_filter_w_pass_and_blocks() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::telemetry::TEST_TRACING);
    let main_span = tracing::info_span!("test_policy_filter_happy_environment");
    let _ = main_span.enter();

    let mut flow = TestFlow::new("by_location", r#"eligible(item, environment) if environment.location_code == 33;"#).await;
    flow.push_environment(TestEnvironment::new(33)).await?;
    let event = flow.recv_policy_event().await?;
    assert!(matches!(event, elements::PolicyFilterEvent::EnvironmentChanged(_)));
    tracing::info!(?event, "DMR-A: environment changed confirmed");

    let item = TestItem::new(std::f64::consts::PI, 1,);
    flow.push_item(item).await?;

    flow.push_environment(TestEnvironment::new(19)).await?;
    let event = flow.recv_policy_event().await?;
    assert!(matches!(event, elements::PolicyFilterEvent::EnvironmentChanged(_)));
    tracing::info!(?event, "DMR-B: environment changed confirmed");

    let item = TestItem::new(std::f64::consts::E, 2);
    flow.push_item(item).await?;
    let event = flow.recv_policy_event().await?;
    assert!(matches!(event, elements::PolicyFilterEvent::ItemBlocked(_)));
    tracing::info!(?event, "DMR-C: item dropped confirmed");

    let item = TestItem::new(std::f64::consts::FRAC_1_PI, 3);
    flow.push_item(item).await?;
    let event = flow.recv_policy_event().await?;
    assert!(matches!(event, elements::PolicyFilterEvent::ItemBlocked(_)));
    tracing::info!(?event, "DMR-D: item dropped confirmed");

    let item = TestItem::new(std::f64::consts::FRAC_1_SQRT_2, 4);
    flow.push_item(item).await?;
    let event = flow.recv_policy_event().await?;
    assert!(matches!(event, elements::PolicyFilterEvent::ItemBlocked(_)));
    tracing::info!(?event, "DMR-E: item dropped confirmed");


    flow.push_environment(TestEnvironment::new(33)).await?;
    let event = flow.recv_policy_event().await?;
    assert!(matches!(event, elements::PolicyFilterEvent::EnvironmentChanged(_)));
    tracing::info!(?event, "DMR-F: environment changed confirmed");

    let item = TestItem::new(std::f64::consts::LN_2, 5);
    flow.push_item(item).await?;

    let actual = flow.close().await?;

    tracing::info!(?actual, "DMR: 08. Verify final accumulation...");
    assert_eq!(
        actual,
        vec![
            TestItem::new(std::f64::consts::PI, 1),
            TestItem::new(std::f64::consts::LN_2, 5),
        ]
    );
    Ok(())
}


#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_policy_filter_before_environment_baseline() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::telemetry::TEST_TRACING);
    let main_span = tracing::info_span!("test_before_environment_baseline");
    let _ = main_span.enter();

    let flow = TestFlow::new("by_location", r#"eligible(item, environment) if environment.location_code == 33;"#).await;
    let item = TestItem {
        flow: TestFlowMetrics {
            input_messages_per_sec: 3.1415926535,
        },
        inbox_lag: 3,
    };
    flow.push_item(item).await?;
    let actual = flow.inspect_sink().await?;
    assert!(actual.is_empty());

    flow.close().await?;
    Ok(())
}
