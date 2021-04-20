mod fixtures;

use chrono::*;
use lazy_static::lazy_static;
use oso::{Oso, PolarClass};
use proctor::elements::PolicyFilterEvent;
use proctor::elements::{self, Policy, PolicySource, TelemetryData};
use proctor::graph::stage::{self, WithApi, WithMonitor};
use proctor::graph::{Connect, Graph, GraphResult, SinkShape, SourceShape, UniformFanInShape};
use proctor::phases::collection;
use proctor::phases::collection::TelemetrySubscription;
use proctor::phases::eligibility::{self, Eligibility};
use proctor::{ProctorContext, ProctorResult};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use serde_cbor::Value;

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
    pub custom: HashMap<String, String>,
}

impl ProctorContext for TestFlinkEligibilityContext {
    fn required_context_fields() -> HashSet<String> {
        maplit::hashset! {
            "cluster.is_deploying".to_string(),
            "cluster.last_deployment".to_string(),
        }
    }

    fn optional_context_fields() -> HashSet<String> {
        maplit::hashset! { "task.last_failure".to_string(), }
    }

    fn custom(&self) -> HashMap<String, String> {
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

use serde_test::{assert_tokens, Token};

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
struct TestEligibilityPolicy {
    custom_fields: Option<HashSet<String>>,
    policy: PolicySource,
}

impl TestEligibilityPolicy {
    pub fn new(policy: PolicySource) -> Self {
        policy.validate().expect("failed to parse policy");
        Self {
            custom_fields: None,
            policy,
        }
    }

    pub fn with_custom(self, custom_fields: HashSet<String>) -> Self {
        Self {
            custom_fields: Some(custom_fields),
            ..self
        }
    }
}

impl Policy for TestEligibilityPolicy {
    type Item = TelemetryData;
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

struct TestFlow {
    pub graph_handle: JoinHandle<()>,
    pub tx_data_source_api: stage::ActorSourceApi<TelemetryData>,
    pub tx_context_source_api: stage::ActorSourceApi<TelemetryData>,
    pub tx_clearinghouse_api: collection::ClearinghouseApi,
    pub tx_eligibility_api: elements::PolicyFilterApi<TestFlinkEligibilityContext>,
    pub rx_eligibility_monitor: elements::PolicyFilterMonitor<TelemetryData, TestFlinkEligibilityContext>,
    pub tx_sink_api: stage::FoldApi<Vec<TelemetryData>>,
    pub rx_sink: Option<oneshot::Receiver<Vec<TelemetryData>>>,
}

impl TestFlow {
    pub async fn new<S: Into<String>>(policy: S) -> ProctorResult<Self> {
        let telemetry_source = stage::ActorSource::<TelemetryData>::new("telemetry_source");
        let tx_data_source_api = telemetry_source.tx_api();

        let ctx_source = stage::ActorSource::<TelemetryData>::new("context_source");
        let tx_context_source_api = ctx_source.tx_api();

        let merge = stage::MergeN::new("source_merge", 2);

        let mut clearinghouse = collection::Clearinghouse::new("clearinghouse");
        let tx_clearinghouse_api = clearinghouse.tx_api();

        let policy = TestEligibilityPolicy::new(PolicySource::String(policy.into()));
        let context_subscription = policy.subscription("eligibility_context");
        let context_channel =
            collection::SubscriptionChannel::<TestFlinkEligibilityContext>::new("eligibility_context").await?;

        let telemetry_subscription = TelemetrySubscription::new("data");
        let telemetry_channel = collection::SubscriptionChannel::<TelemetryData>::new("data_channel").await?;
        let eligibility = Eligibility::<TestFlinkEligibilityContext>::new("test_flink_eligibility", policy);

        let tx_eligibility_api = eligibility.tx_api();
        let rx_eligibility_monitor = eligibility.rx_monitor();

        let mut sink = stage::Fold::<_, TelemetryData, _>::new("sink", Vec::new(), |mut acc, item| {
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

    pub async fn push_telemetry(&self, telemetry: TelemetryData) -> GraphResult<()> {
        let (cmd, ack) = stage::ActorSourceCmd::push(telemetry);
        self.tx_data_source_api.send(cmd)?;
        ack.await.map_err(|err| err.into())
    }

    pub async fn push_context(&self, context_data: TelemetryData) -> GraphResult<()> {
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
    ) -> GraphResult<elements::PolicyFilterEvent<TelemetryData, TestFlinkEligibilityContext>> {
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

    pub async fn inspect_sink(&self) -> GraphResult<Vec<TelemetryData>> {
        let (cmd, acc) = stage::FoldCmd::get_accumulation();
        self.tx_sink_api.send(cmd)?;
        acc.await
            .map(|a| {
                tracing::info!(accumulation=?a, "inspected sink accumulation");
                a
            })
            .map_err(|err| err.into())
    }

    pub async fn close(mut self) -> GraphResult<Vec<TelemetryData>> {
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
    lazy_static::initialize(&proctor::telemetry::TEST_TRACING);
    let main_span = tracing::info_span!("test_eligibility_before_context_baseline");
    let _ = main_span.enter();

    tracing::warn!("test_eligibility_before_context_baseline_A");
    let mut flow = TestFlow::new(r#"eligible(item, environment) if environment.location_code == 33;"#).await?;
    tracing::warn!("test_eligibility_before_context_baseline_B");
    let data = TelemetryData::from_data(maplit::btreemap! {
       "input_messages_per_sec".to_string() => Value::Float(std::f64::consts::PI),
        "timestamp".to_string() => Value::Text(format!("{}", Utc::now().format("%+"))),
        "inbox_lag".to_string() => Value::Integer(3),

    });
    // let data = TelemetryData(maplit::hashmap! {
    //    "input_messages_per_sec".to_string() => std::f64::consts::PI.to_string(),
    //     "timestamp".to_string() => format!("{}", Utc::now().format("%+")),
    //     "inbox_lag".to_string() => 3.to_string(),
    // });
    tracing::warn!("test_eligibility_before_context_baseline_C");

    flow.push_telemetry(data.clone()).await?;
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
    lazy_static::initialize(&proctor::telemetry::TEST_TRACING);
    let main_span = tracing::info_span!("test_eligibility_happy_context");
    let _ = main_span.enter();

    let mut flow = TestFlow::new(r#"eligible(_, context) if context.is_deploying == false;"#).await?;

    tracing::warn!("DMR: 01. Make sure empty env...");

    let detail = flow.inspect_policy_context().await?;
    assert!(detail.context.is_none());

    tracing::warn!("DMR: 02. Push environment...");

    let now = Utc::now();
    let t1 = now - chrono::Duration::days(1);
    let t1_rep = format!("{}", t1.format("%+"));
    flow.push_context(
        maplit::btreemap! {
            "cluster.is_deploying".to_string() => Value::Bool(false),
            "cluster.last_deployment".to_string() => Value::Text(t1_rep),
        }
        .into(),
    )
    .await?;

    tracing::warn!("DMR: 03. Verify environment set...");

    for _ in 0..2 {
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
        PolicyFilterEvent::ItemBlocked(item) => tracing::warn!("DMR: Item blocked: {:?}", item),
    };
    }
    tracing::warn!("DMR: 03. environment change verified.");

    // tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let detail = flow.inspect_policy_context().await?;
    tracing::warn!("DMR: policy detail = {:?}", detail);
    assert!(detail.context.is_some());

    tracing::warn!("DMR: 04. Push Item...");

    // let ts = Utc::now().into();
    flow.push_telemetry(maplit::btreemap! {"measurement".to_string() => Value::Float(std::f64::consts::PI)}.into())
        .await?;

    tracing::warn!("DMR: 05. Look for Item in sink...");

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let actual = flow.inspect_sink().await?;
    assert_eq!(
        actual,
        vec![TelemetryData::from_data(
            maplit::btreemap! {"measurement".to_string() => Value::Float(std::f64::consts::PI),}

            // maplit::hashmap! {"measurement".to_string() => std::f64::consts::PI.to_string()}
        ),]
    );

    tracing::warn!("DMR: 06. Push another Item...");

    // flow.push_telemetry(maplit::hashmap! {"measurement".to_string() => std::f64::consts::TAU.to_string()}.into())
    flow.push_telemetry(maplit::btreemap! {"measurement".to_string() => Value::Float(std::f64::consts::TAU)}.into())
        .await?;

    tracing::warn!("DMR: 07. Close flow...");

    let actual = flow.close().await?;

    tracing::warn!(?actual, "DMR: 08. Verify final accumulation...");

    assert_eq!(
        actual,
        vec![
            // TelemetryData(maplit::hashmap! {"measurement".to_string() => std::f64::consts::PI.to_string()}),
            // TelemetryData(maplit::hashmap! {"measurement".to_string() => std::f64::consts::TAU.to_string()}),
            TelemetryData::from_data(maplit::btreemap! {"measurement".to_string() => Value::Float(std::f64::consts::PI)}),
            TelemetryData::from_data(maplit::btreemap! {"measurement".to_string() => Value::Float(std::f64::consts::TAU)}),
        ]
    );
    Ok(())
}

// #[ignore]
// #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// async fn test_policy_filter_w_pass_and_blocks() -> anyhow::Result<()> {
//     lazy_static::initialize(&proctor::telemetry::TEST_TRACING);
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
//
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
