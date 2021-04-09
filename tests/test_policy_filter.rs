mod fixtures;

use chrono::*;
use oso::{Oso, PolarClass};
use proctor::elements::{self, Policy};
use proctor::graph::stage::{self, WithApi, WithMonitor};
use proctor::graph::{Connect, Graph, GraphResult, SinkShape, SourceShape};
use proctor::ProctorContext;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestItem {
    #[polar(attribute)]
    pub flow: TestFlowMetrics,

    #[serde(with = "proctor::serde")]
    pub timestamp: DateTime<Utc>,

    #[polar(attribute)]
    pub inbox_lag: u32,
}

impl TestItem {
    pub fn new(input_messages_per_sec: f64, ts: DateTime<Utc>, inbox_lag: u32) -> Self {
        Self {
            flow: TestFlowMetrics { input_messages_per_sec },
            timestamp: ts.into(),
            inbox_lag,
        }
    }

    pub fn within_seconds(&self, secs: i64) -> bool {
        let now = Utc::now();
        let boundary = now - chrono::Duration::seconds(secs);
        boundary < self.timestamp
    }

    pub fn input_messages_per_sec(&self, lag: u32) -> f64 {
        self.flow.input_messages_per_sec * lag as f64
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
    custom: HashMap<String, String>,
}

impl TestEnvironment {
    pub fn new(location_code: u32) -> Self {
        Self {
            location_code,
            custom: HashMap::default(),
        }
    }

    pub fn with_custom(self, custom: HashMap<String, String>) -> Self {
        Self { custom, ..self }
    }
}

impl proctor::ProctorContext for TestEnvironment {
    fn subscription_fields_nucleus() -> HashSet<String> {
        maplit::hashset! {
            "location_code".to_string(),
            "input_messages_per_sec".to_string(),
        }
    }

    fn custom(&self) -> HashMap<String, String> {
        self.custom.clone()
    }
}

#[derive(Debug)]
struct TestPolicy {
    policy: String,
}

impl TestPolicy {
    pub fn new<S: AsRef<str>>(policy: S) -> Self {
        let polar = polar_core::polar::Polar::new();
        polar.load_str(policy.as_ref()).expect("failed to parse policy");
        Self {
            policy: policy.as_ref().to_string(),
        }
    }
}

impl Policy for TestPolicy {
    type Item = TestItem;
    type Environment = TestEnvironment;

    fn subscription_fields(&self) -> HashSet<String> {
        Self::Environment::subscription_fields_nucleus()
    }

    fn load_knowledge_base(&self, oso: &mut Oso) -> GraphResult<()> {
        oso.load_str(self.policy.as_str()).map_err(|err| err.into())
    }

    fn initialize_knowledge_base(&self, oso: &mut Oso) -> GraphResult<()> {
        oso.register_class(
            TestItem::get_polar_class_builder()
                .name("TestMetricCatalog")
                .add_method("input_messages_per_sec", TestItem::input_messages_per_sec)
                .add_method("within_seconds", TestItem::within_seconds)
                .build(),
        )?;

        oso.register_class(
            TestEnvironment::get_polar_class_builder()
                .name("TestEnvironment")
                .add_method("custom", ProctorContext::custom)
                // .add_method("custom", |env: &TestEnvironment| env.custom() )
                .build(),
        )?;

        Ok(())
    }

    fn query_knowledge_base(&self, oso: &Oso, item_env: (Self::Item, Self::Environment)) -> GraphResult<oso::Query> {
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
    pub async fn new<S: AsRef<str>>(policy: S) -> Self {
        let item_source = stage::ActorSource::<TestItem>::new("item_source");
        let tx_item_source_api = item_source.tx_api();

        let env_source = stage::ActorSource::<TestEnvironment>::new("env_source");
        let tx_env_source_api = env_source.tx_api();

        let policy = Box::new(TestPolicy::new(policy));
        let policy_filter = elements::PolicyFilter::new("eligibility", policy);
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
        let graph_handle = tokio::spawn(async move {
            graph
                .run()
                .await
                .map_err(|err| {
                    tracing::error!(error=?err, "graph run failed!!");
                    err
                })
                .expect("graph run failed")
        });

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

    pub async fn tell_policy(
        &self,
        command_rx: (
            elements::PolicyFilterCmd<TestEnvironment>,
            oneshot::Receiver<proctor::Ack>,
        ),
    ) -> GraphResult<proctor::Ack> {
        self.tx_policy_api.send(command_rx.0)?;
        command_rx.1.await.map_err(|err| err.into())
    }

    pub async fn recv_policy_event(&mut self) -> GraphResult<elements::PolicyFilterEvent<TestItem, TestEnvironment>> {
        self.rx_policy_monitor.recv().await.map_err(|err| err.into())
    }

    pub async fn inspect_filter_environment(&self) -> GraphResult<elements::PolicyFilterDetail<TestEnvironment>> {
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
        acc.await
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
async fn test_policy_filter_before_environment_baseline() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::telemetry::TEST_TRACING);
    let main_span = tracing::info_span!("test_before_environment_baseline");
    let _ = main_span.enter();

    let flow = TestFlow::new(r#"eligible(item, environment) if environment.location_code == 33;"#).await;
    let item = TestItem {
        flow: TestFlowMetrics {
            input_messages_per_sec: 3.1415926535,
        },
        timestamp: Utc::now().into(),
        inbox_lag: 3,
    };
    flow.push_item(item).await?;
    let actual = flow.inspect_sink().await?;
    assert!(actual.is_empty());

    flow.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_policy_filter_happy_environment() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::telemetry::TEST_TRACING);
    let main_span = tracing::info_span!("test_policy_filter_w_pass_and_blocks");
    let _ = main_span.enter();

    let flow = TestFlow::new(r#"eligible(item, environment) if environment.location_code == 33;"#).await;

    tracing::info!("DMR: 01. Make sure empty env...");

    let detail = flow.inspect_filter_environment().await?;
    assert!(detail.environment.is_none());

    tracing::info!("DMR: 02. Push environment...");

    flow.push_environment(TestEnvironment::new(33)).await?;

    tracing::info!("DMR: 03. Verify environment set...");

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let detail = flow.inspect_filter_environment().await?;
    assert!(detail.environment.is_some());

    tracing::info!("DMR: 04. Push Item...");

    let ts = Utc::now().into();
    let item = TestItem::new(std::f64::consts::PI, ts, 1);
    flow.push_item(item).await?;

    tracing::info!("DMR: 05. Look for Item in sink...");

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let actual = flow.inspect_sink().await?;
    assert_eq!(actual, vec![TestItem::new(std::f64::consts::PI, ts, 1),]);

    tracing::info!("DMR: 06. Push another Item...");

    let item = TestItem::new(std::f64::consts::TAU, ts, 2);
    flow.push_item(item).await?;

    tracing::info!("DMR: 07. Close flow...");

    let actual = flow.close().await?;

    tracing::info!(?actual, "DMR: 08. Verify final accumulation...");

    assert_eq!(
        actual,
        vec![
            TestItem::new(std::f64::consts::PI, ts, 1),
            TestItem::new(std::f64::consts::TAU, ts, 2),
        ]
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_policy_filter_w_pass_and_blocks() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::telemetry::TEST_TRACING);
    let main_span = tracing::info_span!("test_policy_filter_happy_environment");
    let _ = main_span.enter();

    let mut flow = TestFlow::new(r#"eligible(item, environment) if environment.location_code == 33;"#).await;
    flow.push_environment(TestEnvironment::new(33)).await?;
    let event = flow.recv_policy_event().await?;
    assert!(matches!(event, elements::PolicyFilterEvent::EnvironmentChanged(_)));
    tracing::info!(?event, "DMR-A: environment changed confirmed");

    let ts = Utc::now().into();
    let item = TestItem::new(std::f64::consts::PI, ts, 1);
    flow.push_item(item).await?;

    flow.push_environment(TestEnvironment::new(19)).await?;
    let event = flow.recv_policy_event().await?;
    assert!(matches!(event, elements::PolicyFilterEvent::EnvironmentChanged(_)));
    tracing::info!(?event, "DMR-B: environment changed confirmed");

    let item = TestItem::new(std::f64::consts::E, ts, 2);
    flow.push_item(item).await?;
    let event = flow.recv_policy_event().await?;
    assert!(matches!(event, elements::PolicyFilterEvent::ItemBlocked(_)));
    tracing::info!(?event, "DMR-C: item dropped confirmed");

    let item = TestItem::new(std::f64::consts::FRAC_1_PI, ts, 3);
    flow.push_item(item).await?;
    let event = flow.recv_policy_event().await?;
    assert!(matches!(event, elements::PolicyFilterEvent::ItemBlocked(_)));
    tracing::info!(?event, "DMR-D: item dropped confirmed");

    let item = TestItem::new(std::f64::consts::FRAC_1_SQRT_2, ts, 4);
    flow.push_item(item).await?;
    let event = flow.recv_policy_event().await?;
    assert!(matches!(event, elements::PolicyFilterEvent::ItemBlocked(_)));
    tracing::info!(?event, "DMR-E: item dropped confirmed");

    flow.push_environment(TestEnvironment::new(33)).await?;
    let event = flow.recv_policy_event().await?;
    assert!(matches!(event, elements::PolicyFilterEvent::EnvironmentChanged(_)));
    tracing::info!(?event, "DMR-F: environment changed confirmed");

    let item = TestItem::new(std::f64::consts::LN_2, ts, 5);
    flow.push_item(item).await?;

    let actual = flow.close().await?;

    tracing::info!(?actual, "DMR: 08. Verify final accumulation...");
    assert_eq!(
        actual,
        vec![
            TestItem::new(std::f64::consts::PI, ts, 1),
            TestItem::new(std::f64::consts::LN_2, ts, 5),
        ]
    );
    Ok(())
}

// #[tokio::test(flavor="multi_thread", worker_threads = 4)]
#[tokio::test]
async fn test_policy_w_custom_fields() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::telemetry::TEST_TRACING);
    let main_span = tracing::info_span!("test_policy_w_custom_fields");
    let _ = main_span.enter();

    let mut flow = TestFlow::new(
        r#"eligible(item, environment) if
            c = environment.custom() and
            c.cat = "Otis";"#,
    )
    .await;

    flow.push_environment(
        TestEnvironment::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_string()}),
    )
    .await?;
    let event = flow.recv_policy_event().await?;
    tracing::info!(?event, "verifying environment update...");
    assert!(matches!(event, elements::PolicyFilterEvent::EnvironmentChanged(_)));

    let ts = Utc::now().into();
    let item = TestItem::new(std::f64::consts::PI, ts, 1);
    flow.push_item(item).await?;

    let item = TestItem::new(std::f64::consts::TAU, ts, 2);
    flow.push_item(item).await?;

    let actual = flow.close().await?;
    tracing::info!(?actual, "verifying actual result...");
    assert_eq!(
        actual,
        vec![
            TestItem::new(std::f64::consts::PI, ts, 1),
            TestItem::new(std::f64::consts::TAU, ts, 2),
        ],
    );
    Ok(())
}

#[tokio::test]
async fn test_policy_w_item_n_env() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::telemetry::TEST_TRACING);
    let main_span = tracing::info_span!("test_policy_w_custom_fields");
    let _ = main_span.enter();

    tracing::info!("DMR-A:create flow...");
    // another form for policy that works
    // r#"
    //     eligible(item, env) if proper_cat(item, env) and lag_2(item, env);
    //     proper_cat(_, env) if env.custom.cat = "Otis";
    //     lag_2(item, _) if item.inbox_lag = 2;
    // "#,
    let flow = TestFlow::new(
        r#"eligible(item, env) if proper_cat(item, env) and lag_2(item, env);
proper_cat(_, env) if env.custom().cat = "Otis";
lag_2(item: TestMetricCatalog{ inbox_lag: 2 }, _);"#,
    )
    .await;

    tracing::info!("DMR-B:push env...");
    flow.push_environment(
        TestEnvironment::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_string()}),
    )
    .await?;
    tracing::info!("DMR-C:verify enviornment...");

    let ts = Utc::now().into();
    let item = TestItem::new(std::f64::consts::PI, ts, 1);
    flow.push_item(item).await?;

    let item = TestItem::new(std::f64::consts::TAU, ts, 2);
    flow.push_item(item).await?;

    let actual = flow.close().await?;
    tracing::info!(?actual, "verifying actual result...");
    assert_eq!(actual, vec![TestItem::new(std::f64::consts::TAU, ts, 2),]);
    Ok(())
}

#[tokio::test]
async fn test_policy_w_method() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::telemetry::TEST_TRACING);
    let main_span = tracing::info_span!("test_policy_w_custom_fields");
    let _ = main_span.enter();

    let flow = TestFlow::new(
        r#"eligible(item, env) if
34 < item.input_messages_per_sec(item.inbox_lag)
and item.input_messages_per_sec(item.inbox_lag) < 36;"#,
    )
    .await;

    flow.push_environment(
        TestEnvironment::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_string()}),
    )
    .await?;

    let ts = Utc::now().into();
    let item = TestItem::new(std::f64::consts::PI, ts, 1);
    flow.push_item(item).await?;

    let item = TestItem::new(17.327, ts, 2);
    flow.push_item(item).await?;

    let actual = flow.close().await?;
    tracing::info!(?actual, "verifying actual result...");
    assert_eq!(actual, vec![TestItem::new(17.327, ts, 2),]);
    Ok(())
}

#[tokio::test]
async fn test_replace_policy() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::telemetry::TEST_TRACING);
    let main_span = tracing::info_span!("test_policy_w_custom_fields");
    let _ = main_span.enter();

    let boundary_age_secs = 60 * 60;
    let good_ts = Utc::now();
    let too_old_ts = Utc::now() - chrono::Duration::seconds(boundary_age_secs + 5);

    let policy_1 = r#"eligible(_, env: TestEnvironment) if env.custom().cat = "Otis";"#;
    let policy_2 = format!("eligible(item, _) if item.within_seconds({});", boundary_age_secs);
    tracing::info!(?policy_2, "DMR: policy with timestamp");

    let flow = TestFlow::new(policy_1).await;

    flow.push_environment(
        TestEnvironment::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_string()}),
    )
    .await?;

    let item_1 = TestItem::new(std::f64::consts::PI, too_old_ts, 1);
    flow.push_item(item_1.clone()).await?;

    let item_2 = TestItem::new(17.327, good_ts, 2);
    flow.push_item(item_2.clone()).await?;

    tracing::info!("replace policy and re-send");
    let cmd_rx = elements::PolicyFilterCmd::replace_policy(elements::PolicySource::String(policy_2.to_string()));
    flow.tell_policy(cmd_rx).await?;

    flow.push_item(item_1).await?;
    flow.push_item(item_2).await?;

    let actual = flow.close().await?;
    tracing::info!(?actual, "verifying actual result...");
    assert_eq!(
        actual,
        vec![
            TestItem::new(std::f64::consts::PI, too_old_ts, 1),
            TestItem::new(17.327, good_ts, 2),
            TestItem::new(17.327, good_ts, 2),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn test_append_policy() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::telemetry::TEST_TRACING);
    let main_span = tracing::info_span!("test_policy_w_custom_fields");
    let _ = main_span.enter();

    let policy_1 = r#"eligible(item: TestMetricCatalog{ inbox_lag: 2 }, _);"#;
    let policy_2 = r#"eligible(_, env: TestEnvironment) if env.custom().cat = "Otis";"#;

    let flow = TestFlow::new(policy_1).await;

    flow.push_environment(
        TestEnvironment::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_string()}),
    )
    .await?;

    let ts = Utc::now().into();
    let item = TestItem::new(std::f64::consts::PI, ts, 1);
    flow.push_item(item).await?;

    let item = TestItem::new(17.327, ts, 2);
    flow.push_item(item).await?;

    tracing::info!("add to policy and re-send");
    let cmd_rx = elements::PolicyFilterCmd::append_policy(elements::PolicySource::String(policy_2.to_string()));
    flow.tell_policy(cmd_rx).await?;

    let item = TestItem::new(std::f64::consts::PI, ts, 1);
    flow.push_item(item).await?;

    let item = TestItem::new(17.327, ts, 2);
    flow.push_item(item).await?;

    let actual = flow.close().await?;
    tracing::info!(?actual, "verifying actual result...");
    assert_eq!(
        actual,
        vec![
            TestItem::new(17.327, ts, 2),
            TestItem::new(std::f64::consts::PI, ts, 1),
            TestItem::new(17.327, ts, 2),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn test_reset_policy() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::telemetry::TEST_TRACING);
    let main_span = tracing::info_span!("test_policy_w_custom_fields");
    let _ = main_span.enter();

    let policy_1 = r#"eligible(item: TestMetricCatalog{ inbox_lag: 2 }, _);"#;
    let policy_2 = r#"eligible(_, env) if env.custom().cat = "Otis";"#;

    let flow = TestFlow::new(policy_1).await;

    flow.push_environment(
        TestEnvironment::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_string()}),
    )
    .await?;

    let ts = Utc::now().into();
    let item = TestItem::new(std::f64::consts::PI, ts, 1);
    flow.push_item(item).await?;

    let item = TestItem::new(17.327, ts, 2);
    flow.push_item(item).await?;

    tracing::info!("add to policy and resend");
    let cmd_rx = elements::PolicyFilterCmd::append_policy(elements::PolicySource::String(policy_2.to_string()));
    flow.tell_policy(cmd_rx).await?;

    let item = TestItem::new(std::f64::consts::PI, ts, 1);
    flow.push_item(item).await?;

    let item = TestItem::new(17.327, ts, 2);
    flow.push_item(item).await?;

    tracing::info!("and reset policy and resend");
    let cmd_rx = elements::PolicyFilterCmd::reset_policy();
    flow.tell_policy(cmd_rx).await?;

    let item = TestItem::new(std::f64::consts::PI, ts, 1);
    flow.push_item(item).await?;

    let item = TestItem::new(17.327, ts, 2);
    flow.push_item(item).await?;

    let actual = flow.close().await?;
    tracing::info!(?actual, "verifying actual result...");
    assert_eq!(
        actual,
        vec![
            TestItem::new(17.327, ts, 2),
            TestItem::new(std::f64::consts::PI, ts, 1),
            TestItem::new(17.327, ts, 2),
            TestItem::new(17.327, ts, 2),
        ]
    );
    Ok(())
}
