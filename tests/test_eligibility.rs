mod fixtures;

use ::serde::de::DeserializeOwned;
use ::serde::{Deserialize, Serialize};
use ::serde_with::{serde_as, TimestampMilliSeconds};
use chrono::*;
use lazy_static::lazy_static;
use oso::{Oso, PolarClass, ToPolar};
use pretty_assertions::assert_eq;
use proctor::elements::telemetry::ToTelemetry;
use proctor::elements::{self, telemetry, Policy, PolicyFilterEvent, PolicySource, Telemetry};
use proctor::graph::stage::{self, WithApi, WithMonitor};
use proctor::graph::{Connect, Graph, GraphResult, SinkShape, SourceShape, UniformFanInShape};
use proctor::phases::collection;
use proctor::phases::collection::TelemetrySubscription;
use proctor::phases::eligibility::Eligibility;
use proctor::AppData;
use proctor::{ProctorContext, ProctorResult};
use serde_test::{assert_tokens, Token};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::iter::FromIterator;
use std::marker::PhantomData;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Data {
    pub input_messages_per_sec: f64,
    #[serde(with = "proctor::serde")]
    pub timestamp: DateTime<Utc>,
    pub inbox_lag: i64,
}

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TestFlinkEligibilityContext {
    #[polar(attribute)]
    #[serde(flatten)]
    pub task_status: TestTaskStatus,
    #[polar(attribute)]
    #[serde(flatten)]
    pub cluster_status: TestClusterStatus,

    #[polar(attribute)]
    #[serde(flatten)]
    pub custom: telemetry::Table,
}

impl ProctorContext for TestFlinkEligibilityContext {
    fn required_context_fields() -> HashSet<&'static str> {
        maplit::hashset! {
            "cluster.is_deploying",
            "cluster.last_deployment",
        }
    }

    fn optional_context_fields() -> HashSet<&'static str> {
        maplit::hashset! { "task.last_failure", }
    }

    fn custom(&self) -> telemetry::Table {
        self.custom.clone()
    }
}

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TestTaskStatus {
    #[serde(default)]
    #[serde(
        rename = "task.last_failure",
        serialize_with = "proctor::serde::serialize_optional_datetime",
        deserialize_with = "proctor::serde::deserialize_optional_datetime"
    )]
    pub last_failure: Option<DateTime<Utc>>,
}

impl TestTaskStatus {
    pub fn last_failure_within_seconds(&self, seconds: i64) -> bool {
        self.last_failure.map_or(false, |last_failure| {
            let boundary = Utc::now() - chrono::Duration::seconds(seconds);
            boundary < last_failure
        })
    }
}

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TestClusterStatus {
    #[polar(attribute)]
    #[serde(rename = "cluster.is_deploying")]
    pub is_deploying: bool,
    #[serde(with = "proctor::serde", rename = "cluster.last_deployment")]
    pub last_deployment: DateTime<Utc>,
}

impl TestClusterStatus {
    pub fn last_deployment_within_seconds(&self, seconds: i64) -> bool {
        let boundary = Utc::now() - chrono::Duration::seconds(seconds);
        boundary < self.last_deployment
    }
}

lazy_static! {
    static ref DT_1: DateTime<Utc> = Utc::now();
    static ref DT_1_STR: String = format!("{}", DT_1.format("%+"));
}

#[test]
fn test_context_serde() {
    let context = TestFlinkEligibilityContext {
        task_status: TestTaskStatus { last_failure: None },
        cluster_status: TestClusterStatus {
            is_deploying: false,
            last_deployment: DT_1.clone(),
        },
        custom: HashMap::new(),
    };

    assert_tokens(
        &context,
        &vec![
            Token::Map { len: None },
            Token::Str("task.last_failure"),
            Token::None,
            Token::Str("cluster.is_deploying"),
            Token::Bool(false),
            Token::Str("cluster.last_deployment"),
            Token::Str(&DT_1_STR),
            Token::MapEnd,
        ],
    );
}

#[derive(Debug)]
struct TestEligibilityPolicy<D> {
    custom_fields: Option<HashSet<String>>,
    policy: PolicySource,
    data_marker: PhantomData<D>,
}

impl<D> TestEligibilityPolicy<D> {
    pub fn new(policy: PolicySource) -> Self {
        policy.validate().expect("failed to parse policy");
        Self {
            custom_fields: None,
            policy,
            data_marker: PhantomData,
        }
    }

    pub fn with_custom(self, custom_fields: HashSet<String>) -> Self {
        Self {
            custom_fields: Some(custom_fields),
            ..self
        }
    }
}

impl<D: AppData + ToPolar> Policy for TestEligibilityPolicy<D> {
    type Item = D;
    type Context = TestFlinkEligibilityContext;

    fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
        tracing::warn!(
            "DMR: TestEligibilityPolicy::subscription: before custom subscription:{:?}",
            subscription
        );
        let result = if let Some(ref custom_fields) = self.custom_fields {
            tracing::warn!(
                "DMR: TestEligibilityPolicy::subscription: custom_fields:{:?}",
                custom_fields
            );
            subscription.with_optional_fields(custom_fields.clone())
        } else {
            subscription
        };

        tracing::warn!(
            "DMR: TestEligibilityPolicy::subscription: after custom subscription:{:?}",
            result
        );
        result
    }

    fn load_knowledge_base(&self, oso: &mut Oso) -> GraphResult<()> {
        self.policy.load_into(oso)
    }

    fn initialize_knowledge_base(&self, oso: &mut Oso) -> GraphResult<()> {
        oso.register_class(
            TestFlinkEligibilityContext::get_polar_class_builder()
                .name("TestEnvironment")
                .add_method("custom", ProctorContext::custom)
                .build(),
        )?;

        oso.register_class(
            TestTaskStatus::get_polar_class_builder()
                .name("TestTaskStatus")
                .add_method(
                    "last_failure_within_seconds",
                    TestTaskStatus::last_failure_within_seconds,
                )
                .build(),
        )?;

        Ok(())
    }

    fn query_knowledge_base(&self, oso: &Oso, item_env: (Self::Item, Self::Context)) -> GraphResult<oso::Query> {
        oso.query_rule("eligible", item_env).map_err(|err| err.into())
    }
}

struct TestFlow<D> {
    pub graph_handle: JoinHandle<()>,
    pub tx_data_source_api: stage::ActorSourceApi<Telemetry>,
    pub tx_context_source_api: stage::ActorSourceApi<Telemetry>,
    pub tx_clearinghouse_api: collection::ClearinghouseApi,
    pub tx_eligibility_api: elements::PolicyFilterApi<TestFlinkEligibilityContext>,
    pub rx_eligibility_monitor: elements::PolicyFilterMonitor<D, TestFlinkEligibilityContext>,
    pub tx_sink_api: stage::FoldApi<Vec<D>>,
    pub rx_sink: Option<oneshot::Receiver<Vec<D>>>,
}

impl<D: AppData + Clone + DeserializeOwned + ToPolar> TestFlow<D> {
    pub async fn new<S: Into<String>>(telemetry_subscription: TelemetrySubscription, policy: S) -> ProctorResult<Self> {
        let telemetry_source = stage::ActorSource::<Telemetry>::new("telemetry_source");
        let tx_data_source_api = telemetry_source.tx_api();

        let ctx_source = stage::ActorSource::<Telemetry>::new("context_source");
        let tx_context_source_api = ctx_source.tx_api();

        let merge = stage::MergeN::new("source_merge", 2);

        let mut clearinghouse = collection::Clearinghouse::new("clearinghouse");
        let tx_clearinghouse_api = clearinghouse.tx_api();

        let policy = TestEligibilityPolicy::new(PolicySource::String(policy.into()));
        let context_subscription = policy.subscription("eligibility_context");
        let context_channel =
            collection::SubscriptionChannel::<TestFlinkEligibilityContext>::new("eligibility_context").await?;

        let telemetry_channel = collection::SubscriptionChannel::<D>::new("data_channel").await?;
        let eligibility = Eligibility::<D, TestFlinkEligibilityContext>::new("test_flink_eligibility", policy);

        let tx_eligibility_api = eligibility.tx_api();
        let rx_eligibility_monitor = eligibility.rx_monitor();

        let mut sink = stage::Fold::<_, D, _>::new("sink", Vec::new(), |mut acc, item| {
            acc.push(item);
            acc
        });
        let tx_sink_api = sink.tx_api();
        let rx_sink = sink.take_final_rx();

        (&telemetry_source.outlet(), &merge.inlets().get(0).await.unwrap())
            .connect()
            .await;
        (&ctx_source.outlet(), &merge.inlets().get(1).await.unwrap())
            .connect()
            .await;
        (merge.outlet(), clearinghouse.inlet()).connect().await;
        clearinghouse
            .add_subscription(telemetry_subscription, &telemetry_channel.subscription_receiver)
            .await;
        clearinghouse
            .add_subscription(context_subscription, &context_channel.subscription_receiver)
            .await;
        (context_channel.outlet(), eligibility.context_inlet()).connect().await;
        (telemetry_channel.outlet(), eligibility.inlet()).connect().await;
        (eligibility.outlet(), sink.inlet()).connect().await;

        assert!(eligibility.context_inlet.is_attached().await);
        assert!(eligibility.inlet().is_attached().await);
        assert!(eligibility.outlet().is_attached().await);

        let mut graph = Graph::default();
        graph.push_back(Box::new(telemetry_source)).await;
        graph.push_back(Box::new(ctx_source)).await;
        graph.push_back(Box::new(merge)).await;
        graph.push_back(Box::new(clearinghouse)).await;
        graph.push_back(Box::new(context_channel)).await;
        graph.push_back(Box::new(telemetry_channel)).await;
        graph.push_back(Box::new(eligibility)).await;
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

        Ok(Self {
            graph_handle,
            tx_data_source_api,
            tx_context_source_api,
            tx_clearinghouse_api,
            tx_eligibility_api,
            rx_eligibility_monitor,
            tx_sink_api,
            rx_sink,
        })
    }

    pub async fn push_telemetry(&self, telemetry: Telemetry) -> GraphResult<()> {
        let (cmd, ack) = stage::ActorSourceCmd::push(telemetry);
        self.tx_data_source_api.send(cmd)?;
        ack.await.map_err(|err| err.into())
    }

    pub async fn push_context(&self, context_data: Telemetry) -> GraphResult<()> {
        let (cmd, ack) = stage::ActorSourceCmd::push(context_data);
        self.tx_context_source_api.send(cmd)?;
        ack.await.map_err(|err| err.into())
    }

    pub async fn tell_policy(
        &self,
        command_rx: (
            elements::PolicyFilterCmd<TestFlinkEligibilityContext>,
            oneshot::Receiver<proctor::Ack>,
        ),
    ) -> GraphResult<proctor::Ack> {
        self.tx_eligibility_api.send(command_rx.0)?;
        command_rx.1.await.map_err(|err| err.into())
    }

    pub async fn recv_policy_event(
        &mut self,
    ) -> GraphResult<elements::PolicyFilterEvent<D, TestFlinkEligibilityContext>> {
        self.rx_eligibility_monitor.recv().await.map_err(|err| err.into())
    }

    pub async fn inspect_policy_context(
        &self,
    ) -> GraphResult<elements::PolicyFilterDetail<TestFlinkEligibilityContext>> {
        let (cmd, detail) = elements::PolicyFilterCmd::inspect();
        self.tx_eligibility_api.send(cmd)?;
        detail
            .await
            .map(|d| {
                tracing::info!(detail=?d, "inspected policy.");
                d
            })
            .map_err(|err| err.into())
    }

    pub async fn inspect_sink(&self) -> GraphResult<Vec<D>> {
        let (cmd, acc) = stage::FoldCmd::get_accumulation();
        self.tx_sink_api.send(cmd)?;
        acc.await
            .map(|a| {
                tracing::info!(accumulation=?a, "inspected sink accumulation");
                a
            })
            .map_err(|err| err.into())
    }

    #[tracing::instrument(level = "warn", skip(self))]
    pub async fn close(mut self) -> GraphResult<Vec<D>> {
        let (stop, _) = stage::ActorSourceCmd::stop();
        self.tx_data_source_api.send(stop)?;

        let (stop, _) = stage::ActorSourceCmd::stop();
        self.tx_context_source_api.send(stop)?;

        self.graph_handle.await?;

        self.rx_sink.take().unwrap().await.map_err(|err| err.into())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_eligibility_before_context_baseline() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_eligibility_before_context_baseline");
    let _ = main_span.enter();

    tracing::warn!("test_eligibility_before_context_baseline_A");
    let mut flow: TestFlow<Data> = TestFlow::new(
        TelemetrySubscription::new("all_data"),
        r#"eligible(item, environment) if environment.location_code == 33;"#,
    )
    .await?;
    tracing::warn!("test_eligibility_before_context_baseline_B");
    let now_utc = Utc::now();
    // let now_ts = now_utc.timestamp();

    let data = Data {
        input_messages_per_sec: std::f64::consts::PI,
        timestamp: now_utc,
        inbox_lag: 3,
    };

    let data_telemetry = Telemetry::try_from(&data)?;
    tracing::warn!("test_eligibility_before_context_baseline_C");

    flow.push_telemetry(data_telemetry).await?;
    tracing::warn!("test_eligibility_before_context_baseline_D");
    let actual = flow.inspect_sink().await?;
    tracing::warn!("test_eligibility_before_context_baseline_E");
    assert!(actual.is_empty());

    tracing::warn!("test_eligibility_before_context_baseline_F");

    match flow.rx_eligibility_monitor.recv().await? {
        PolicyFilterEvent::ItemBlocked(blocked) => {
            tracing::warn!("receive item blocked notification re: {:?}", blocked);
            assert_eq!(blocked, data);
        }
        PolicyFilterEvent::ContextChanged(ctx) => {
            panic!("unexpected context change:{:?}", ctx);
        }
    }

    let actual = flow.close().await?;
    tracing::warn!("final accumulation:{:?}", actual);

    assert!(actual.is_empty());
    tracing::warn!("test_eligibility_before_context_baseline_G");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_eligibility_happy_context() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_eligibility_happy_context");
    let _ = main_span.enter();

    #[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct MeasurementData {
        measurement: f64,
    }

    let mut flow: TestFlow<MeasurementData> = TestFlow::new(
        TelemetrySubscription::new("measurements").with_required_fields(maplit::hashset! {"measurement".to_string()}),
        r#"eligible(_, context) if context.cluster_status.is_deploying == false;"#,
    )
    .await?;

    tracing::warn!("DMR: 01. Make sure empty env...");

    let detail = flow.inspect_policy_context().await?;
    assert!(detail.context.is_none());

    tracing::warn!("DMR: 02. Push environment...");

    let now = Utc::now();
    let t1 = now - chrono::Duration::days(1);
    let t1_rep = format!("{}", t1.format("%+"));
    flow.push_context(
        maplit::hashmap! {
            "cluster.is_deploying".to_string() => false.to_telemetry(),
            "cluster.last_deployment".to_string() => t1_rep.to_telemetry(),
        }
        .into(),
    )
    .await?;

    tracing::warn!("DMR: 03. Verify environment set...");

    match flow.rx_eligibility_monitor.recv().await? {
        PolicyFilterEvent::ContextChanged(Some(ctx)) => {
            tracing::warn!("notified of eligibility context change: {:?}", ctx);
            assert_eq!(
                ctx,
                TestFlinkEligibilityContext {
                    task_status: TestTaskStatus { last_failure: None },
                    cluster_status: TestClusterStatus {
                        is_deploying: false,
                        last_deployment: t1
                    },
                    custom: HashMap::new(),
                }
            );
        }
        PolicyFilterEvent::ContextChanged(None) => panic!("did not expect to clear context"),
        PolicyFilterEvent::ItemBlocked(item) => panic!("unexpected item receipt - blocked: {:?}", item),
    };
    tracing::warn!("DMR: 04. environment change verified.");

    let detail = flow.inspect_policy_context().await?;
    tracing::warn!("DMR: policy detail = {:?}", detail);
    assert!(detail.context.is_some());

    tracing::warn!("DMR: 05. Push Item...");

    let t1 = Telemetry::from_iter(maplit::hashmap! {"measurement" => std::f64::consts::PI.to_telemetry()});
    assert_eq!(
        MeasurementData {
            measurement: std::f64::consts::PI,
        },
        t1.try_into::<MeasurementData>()?
    );

    flow.push_telemetry(maplit::hashmap! {"measurement".to_string() => std::f64::consts::PI.to_telemetry()}.into())
        .await?;

    tracing::warn!("DMR: 06. Look for Item in sink...");

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let actual = flow.inspect_sink().await?;
    assert_eq!(
        actual,
        vec![MeasurementData {
            measurement: std::f64::consts::PI,
        },]
    );

    tracing::warn!("DMR: 07. Push another Item...");

    flow.push_telemetry(maplit::hashmap! {"measurement".to_string() => std::f64::consts::TAU.to_telemetry()}.into())
        .await?;

    tracing::warn!("DMR: 08. Close flow...");

    let actual = flow.close().await?;

    tracing::warn!(?actual, "DMR: 09. Verify final accumulation...");

    assert_eq!(
        actual,
        vec![
            MeasurementData {
                measurement: std::f64::consts::PI,
            },
            MeasurementData {
                measurement: std::f64::consts::TAU,
            },
        ]
    );
    Ok(())
}

#[serde_as]
#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestItem {
    #[polar(attribute)]
    pub flow: TestFlowMetrics,

    #[polar(attribute)]
    pub inbox_lag: u32,

    #[serde_as(as = "TimestampMilliSeconds")]
    pub timestamp: DateTime<Utc>,
}

impl TestItem {
    pub fn new(input_messages_per_sec: f64, inbox_lag: u32, ts: DateTime<Utc>) -> Self {
        Self {
            flow: TestFlowMetrics { input_messages_per_sec },
            inbox_lag,
            timestamp: ts,
        }
    }

    pub fn within_seconds(&self, secs: i64) -> bool {
        let now = Utc::now();
        let boundary = now - chrono::Duration::seconds(secs);
        boundary < self.timestamp
    }

    pub fn lag_duration_secs_f64(&self, message_lag: u32) -> f64 {
        (message_lag as f64) / self.flow.input_messages_per_sec
    }
}

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestFlowMetrics {
    #[polar(attribute)]
    pub input_messages_per_sec: f64,
}

#[derive(PolarClass, Debug, Clone, Serialize, Deserialize)]
struct TestContext {
    #[polar(attribute)]
    pub location_code: u32,
    custom: telemetry::Table,
}

impl TestContext {
    pub fn new(location_code: u32) -> Self {
        Self {
            location_code,
            custom: telemetry::Table::default(),
        }
    }

    pub fn with_custom(self, custom: telemetry::Table) -> Self {
        Self { custom, ..self }
    }
}

impl proctor::ProctorContext for TestContext {
    fn required_context_fields() -> HashSet<&'static str> {
        maplit::hashset! { "location_code", "input_messages_per_sec" }
    }

    fn custom(&self) -> telemetry::Table {
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
        polar.load_str(policy.as_ref()).expect("failed to parse policy text");
        Self {
            policy: policy.as_ref().to_string(),
        }
    }
}

impl Policy for TestPolicy {
    type Item = TestItem;
    type Context = TestContext;

    fn load_knowledge_base(&self, oso: &mut Oso) -> GraphResult<()> {
        oso.load_str(self.policy.as_str()).map_err(|err| err.into())
    }

    fn initialize_knowledge_base(&self, oso: &mut Oso) -> GraphResult<()> {
        oso.register_class(
            TestItem::get_polar_class_builder()
                .name("TestMetricCatalog")
                .add_method("lag_duration", TestItem::lag_duration_secs_f64)
                .add_method("within_seconds", TestItem::within_seconds)
                .build(),
        )?;

        oso.register_class(
            TestContext::get_polar_class_builder()
                .name("TestContext")
                .add_method("custom", ProctorContext::custom)
                // .add_method("custom", |ctx: &TestContext| ctx.custom())
                .build(),
        )?;

        Ok(())
    }

    fn query_knowledge_base(&self, oso: &Oso, item_env: (Self::Item, Self::Context)) -> GraphResult<oso::Query> {
        oso.query_rule("eligible", item_env).map_err(|err| err.into())
    }
}

// #[ignore]
// #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// async fn test_policy_filter_w_pass_and_blocks() -> anyhow::Result<()> {
//     lazy_static::initialize(&proctor::tracing::TEST_TRACING);
//     let main_span = tracing::info_span!("test_policy_filter_happy_environment");
//     let _ = main_span.enter();
//
//     let mut flow = TestFlow::new(r#"eligible(item, environment) if environment.location_code == 33;"#).await;
//     flow.push_environment(TestEnvironment::new(33)).await?;
//     let event = flow.recv_policy_event().await?;
//     assert!(matches!(event, elements::PolicyFilterEvent::EnvironmentChanged(_)));
//     tracing::info!(?event, "DMR-A: environment changed confirmed");
//
//     let ts = Utc::now().into();
//     let item = TestItem::new(std::f64::consts::PI, ts, 1);
//     flow.push_item(item).await?;
//
//     flow.push_environment(TestEnvironment::new(19)).await?;
//     let event = flow.recv_policy_event().await?;
//     assert!(matches!(event, elements::PolicyFilterEvent::EnvironmentChanged(_)));
//     tracing::info!(?event, "DMR-B: environment changed confirmed");
//
//     let item = TestItem::new(std::f64::consts::E, ts, 2);
//     flow.push_item(item).await?;
//     let event = flow.recv_policy_event().await?;
//     assert!(matches!(event, elements::PolicyFilterEvent::ItemBlocked(_)));
//     tracing::info!(?event, "DMR-C: item dropped confirmed");
//
//     let item = TestItem::new(std::f64::consts::FRAC_1_PI, ts, 3);
//     flow.push_item(item).await?;
//     let event = flow.recv_policy_event().await?;
//     assert!(matches!(event, elements::PolicyFilterEvent::ItemBlocked(_)));
//     tracing::info!(?event, "DMR-D: item dropped confirmed");
//
//     let item = TestItem::new(std::f64::consts::FRAC_1_SQRT_2, ts, 4);
//     flow.push_item(item).await?;
//     let event = flow.recv_policy_event().await?;
//     assert!(matches!(event, elements::PolicyFilterEvent::ItemBlocked(_)));
//     tracing::info!(?event, "DMR-E: item dropped confirmed");
//
//     flow.push_environment(TestEnvironment::new(33)).await?;
//     let event = flow.recv_policy_event().await?;
//     assert!(matches!(event, elements::PolicyFilterEvent::EnvironmentChanged(_)));
//     tracing::info!(?event, "DMR-F: environment changed confirmed");
//
//     let item = TestItem::new(std::f64::consts::LN_2, ts, 5);
//     flow.push_item(item).await?;
//
//     let actual = flow.close().await?;
//
//     tracing::info!(?actual, "DMR: 08. Verify final accumulation...");
//     assert_eq!(
//         actual,
//         vec![
//             TestItem::new(std::f64::consts::PI, ts, 1),
//             TestItem::new(std::f64::consts::LN_2, ts, 5),
//         ]
//     );
//     Ok(())
// }

// #[ignore]
// // #[tokio::test(flavor="multi_thread", worker_threads = 4)]
// #[tokio::test]
// async fn test_policy_w_custom_fields() -> anyhow::Result<()> {
//     lazy_static::initialize(&proctor::telemetry::TEST_TRACING);
//     let main_span = tracing::info_span!("test_policy_w_custom_fields");
//     let _ = main_span.enter();
//
//     let mut flow = TestFlow::new(
//         r#"eligible(item, environment) if
//             c = environment.custom() and
//             c.cat = "Otis";"#,
//     )
//     .await;
//
//     flow.push_environment(
//         TestEnvironment::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_string()}),
//     )
//     .await?;
//     let event = flow.recv_policy_event().await?;
//     tracing::info!(?event, "verifying environment update...");
//     assert!(matches!(event, elements::PolicyFilterEvent::EnvironmentChanged(_)));
//
//     let ts = Utc::now().into();
//     let item = TestItem::new(std::f64::consts::PI, ts, 1);
//     flow.push_item(item).await?;
//
//     let item = TestItem::new(std::f64::consts::TAU, ts, 2);
//     flow.push_item(item).await?;
//
//     let actual = flow.close().await?;
//     tracing::info!(?actual, "verifying actual result...");
//     assert_eq!(
//         actual,
//         vec![
//             TestItem::new(std::f64::consts::PI, ts, 1),
//             TestItem::new(std::f64::consts::TAU, ts, 2),
//         ],
//     );
//     Ok(())
// }
//
// #[ignore]
// #[tokio::test]
// async fn test_policy_w_item_n_env() -> anyhow::Result<()> {
//     lazy_static::initialize(&proctor::telemetry::TEST_TRACING);
//     let main_span = tracing::info_span!("test_policy_w_custom_fields");
//     let _ = main_span.enter();
//
//     tracing::info!("DMR-A:create flow...");
//     // another form for policy that works
//     // r#"
//     //     eligible(item, env) if proper_cat(item, env) and lag_2(item, env);
//     //     proper_cat(_, env) if env.custom.cat = "Otis";
//     //     lag_2(item, _) if item.inbox_lag = 2;
//     // "#,
//     let flow = TestFlow::new(
//         r#"eligible(item, env) if proper_cat(item, env) and lag_2(item, env);
// proper_cat(_, env) if env.custom().cat = "Otis";
// lag_2(item: TestMetricCatalog{ inbox_lag: 2 }, _);"#,
//     )
//     .await;
//
//     tracing::info!("DMR-B:push env...");
//     flow.push_environment(
//         TestEnvironment::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_string()}),
//     )
//     .await?;
//     tracing::info!("DMR-C:verify enviornment...");
//
//     let ts = Utc::now().into();
//     let item = TestItem::new(std::f64::consts::PI, ts, 1);
//     flow.push_item(item).await?;
//
//     let item = TestItem::new(std::f64::consts::TAU, ts, 2);
//     flow.push_item(item).await?;
//
//     let actual = flow.close().await?;
//     tracing::info!(?actual, "verifying actual result...");
//     assert_eq!(actual, vec![TestItem::new(std::f64::consts::TAU, ts, 2),]);
//     Ok(())
// }
//
// #[ignore]
// #[tokio::test]
// async fn test_policy_w_method() -> anyhow::Result<()> {
//     lazy_static::initialize(&proctor::telemetry::TEST_TRACING);
//     let main_span = tracing::info_span!("test_policy_w_custom_fields");
//     let _ = main_span.enter();
//
//     let flow = TestFlow::new(
//         r#"eligible(item, env) if
// 34 < item.input_messages_per_sec(item.inbox_lag)
// and item.input_messages_per_sec(item.inbox_lag) < 36;"#,
//     )
//     .await;
//
//     flow.push_environment(
//         TestEnvironment::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_string()}),
//     )
//     .await?;
//
//     let ts = Utc::now().into();
//     let item = TestItem::new(std::f64::consts::PI, ts, 1);
//     flow.push_item(item).await?;
//
//     let item = TestItem::new(17.327, ts, 2);
//     flow.push_item(item).await?;
//
//     let actual = flow.close().await?;
//     tracing::info!(?actual, "verifying actual result...");
//     assert_eq!(actual, vec![TestItem::new(17.327, ts, 2),]);
//     Ok(())
// }

// #[ignore]
// #[tokio::test]
// async fn test_replace_policy() -> anyhow::Result<()> {
//     lazy_static::initialize(&proctor::telemetry::TEST_TRACING);
//     let main_span = tracing::info_span!("test_policy_w_custom_fields");
//     let _ = main_span.enter();
//
//     let boundary_age_secs = 60 * 60;
//     let good_ts = Utc::now();
//     let too_old_ts = Utc::now() - chrono::Duration::seconds(boundary_age_secs + 5);
//
//     let policy_1 = r#"eligible(_, env: TestEnvironment) if env.custom().cat = "Otis";"#;
//     let policy_2 = format!("eligible(item, _) if item.within_seconds({});", boundary_age_secs);
//     tracing::info!(?policy_2, "DMR: policy with timestamp");
//
//     let flow = TestFlow::new(policy_1).await;
//
//     flow.push_environment(
//         TestEnvironment::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_string()}),
//     )
//     .await?;
//
//     let item_1 = TestItem::new(std::f64::consts::PI, too_old_ts, 1);
//     flow.push_item(item_1.clone()).await?;
//
//     let item_2 = TestItem::new(17.327, good_ts, 2);
//     flow.push_item(item_2.clone()).await?;
//
//     tracing::info!("replace policy and re-send");
//     let cmd_rx = elements::PolicyFilterCmd::replace_policy(elements::PolicySource::String(policy_2.to_string()));
//     flow.tell_policy(cmd_rx).await?;
//
//     flow.push_item(item_1).await?;
//     flow.push_item(item_2).await?;
//
//     let actual = flow.close().await?;
//     tracing::info!(?actual, "verifying actual result...");
//     assert_eq!(
//         actual,
//         vec![
//             TestItem::new(std::f64::consts::PI, too_old_ts, 1),
//             TestItem::new(17.327, good_ts, 2),
//             TestItem::new(17.327, good_ts, 2),
//         ]
//     );
//     Ok(())
// }
//
// #[ignore]
// #[tokio::test]
// async fn test_append_policy() -> anyhow::Result<()> {
//     lazy_static::initialize(&proctor::telemetry::TEST_TRACING);
//     let main_span = tracing::info_span!("test_policy_w_custom_fields");
//     let _ = main_span.enter();
//
//     let policy_1 = r#"eligible(item: TestMetricCatalog{ inbox_lag: 2 }, _);"#;
//     let policy_2 = r#"eligible(_, env: TestEnvironment) if env.custom().cat = "Otis";"#;
//
//     let flow = TestFlow::new(policy_1).await;
//
//     flow.push_environment(
//         TestEnvironment::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_string()}),
//     )
//     .await?;
//
//     let ts = Utc::now().into();
//     let item = TestItem::new(std::f64::consts::PI, ts, 1);
//     flow.push_item(item).await?;
//
//     let item = TestItem::new(17.327, ts, 2);
//     flow.push_item(item).await?;
//
//     tracing::info!("add to policy and re-send");
//     let cmd_rx = elements::PolicyFilterCmd::append_policy(elements::PolicySource::String(policy_2.to_string()));
//     flow.tell_policy(cmd_rx).await?;
//
//     let item = TestItem::new(std::f64::consts::PI, ts, 1);
//     flow.push_item(item).await?;
//
//     let item = TestItem::new(17.327, ts, 2);
//     flow.push_item(item).await?;
//
//     let actual = flow.close().await?;
//     tracing::info!(?actual, "verifying actual result...");
//     assert_eq!(
//         actual,
//         vec![
//             TestItem::new(17.327, ts, 2),
//             TestItem::new(std::f64::consts::PI, ts, 1),
//             TestItem::new(17.327, ts, 2),
//         ]
//     );
//     Ok(())
// }
//
// #[ignore]
// #[tokio::test]
// async fn test_reset_policy() -> anyhow::Result<()> {
//     lazy_static::initialize(&proctor::telemetry::TEST_TRACING);
//     let main_span = tracing::info_span!("test_policy_w_custom_fields");
//     let _ = main_span.enter();
//
//     let policy_1 = r#"eligible(item: TestMetricCatalog{ inbox_lag: 2 }, _);"#;
//     let policy_2 = r#"eligible(_, env) if env.custom().cat = "Otis";"#;
//
//     let flow = TestFlow::new(policy_1).await;
//
//     flow.push_environment(
//         TestEnvironment::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_string()}),
//     )
//     .await?;
//
//     let ts = Utc::now().into();
//     let item = TestItem::new(std::f64::consts::PI, ts, 1);
//     flow.push_item(item).await?;
//
//     let item = TestItem::new(17.327, ts, 2);
//     flow.push_item(item).await?;
//
//     tracing::info!("add to policy and resend");
//     let cmd_rx = elements::PolicyFilterCmd::append_policy(elements::PolicySource::String(policy_2.to_string()));
//     flow.tell_policy(cmd_rx).await?;
//
//     let item = TestItem::new(std::f64::consts::PI, ts, 1);
//     flow.push_item(item).await?;
//
//     let item = TestItem::new(17.327, ts, 2);
//     flow.push_item(item).await?;
//
//     tracing::info!("and reset policy and resend");
//     let cmd_rx = elements::PolicyFilterCmd::reset_policy();
//     flow.tell_policy(cmd_rx).await?;
//
//     let item = TestItem::new(std::f64::consts::PI, ts, 1);
//     flow.push_item(item).await?;
//
//     let item = TestItem::new(17.327, ts, 2);
//     flow.push_item(item).await?;
//
//     let actual = flow.close().await?;
//     tracing::info!(?actual, "verifying actual result...");
//     assert_eq!(
//         actual,
//         vec![
//             TestItem::new(17.327, ts, 2),
//             TestItem::new(std::f64::consts::PI, ts, 1),
//             TestItem::new(17.327, ts, 2),
//             TestItem::new(17.327, ts, 2),
//         ]
//     );
//     Ok(())
// }
