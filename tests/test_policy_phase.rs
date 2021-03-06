use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::time::Duration;

use ::serde::de::DeserializeOwned;
use ::serde::{Deserialize, Serialize};
use ::serde_with::{serde_as, TimestampSeconds};
use chrono::*;
use claim::*;
use lazy_static::lazy_static;
use oso::{Oso, PolarClass, ToPolar};
use pretty_assertions::assert_eq;
use proctor::elements::telemetry::ToTelemetry;
use proctor::elements::{
    self, telemetry, Policy, PolicyFilterEvent, PolicyOutcome, PolicySettings, PolicySource, PolicySubscription,
    QueryPolicy, QueryResult, Telemetry, TelemetryValue,
};
use proctor::error::{PolicyError, ProctorError};
use proctor::graph::stage::{self, WithApi, WithMonitor};
use proctor::graph::{Connect, Graph, SinkShape, SourceShape, UniformFanInShape};
use proctor::phases::collection::{self, SubscriptionRequirements, TelemetrySubscription};
use proctor::phases::policy_phase::PolicyPhase;
use proctor::AppData;
use proctor::ProctorContext;
use serde_test::{assert_tokens, Token};
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
pub struct TestPolicyPhaseContext {
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

#[async_trait]
impl ProctorContext for TestPolicyPhaseContext {
    type Error = ProctorError;

    fn custom(&self) -> telemetry::Table {
        self.custom.clone()
    }
}

impl SubscriptionRequirements for TestPolicyPhaseContext {
    fn required_fields() -> HashSet<collection::Str> {
        maplit::hashset! {
            "cluster.location_code".into(),
            "cluster.is_deploying".into(),
            "cluster.last_deployment".into(),
        }
    }

    fn optional_fields() -> HashSet<collection::Str> {
        maplit::hashset! { "task.last_failure".into(), }
    }
}

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TestTaskStatus {
    #[serde(default)]
    #[serde(
        rename = "task.last_failure",
        serialize_with = "proctor::serde::date::serialize_optional_datetime_map",
        deserialize_with = "proctor::serde::date::deserialize_optional_datetime"
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
    #[serde(rename = "cluster.location_code")]
    pub location_code: u32,

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
    static ref DT_1: DateTime<Utc> = DateTime::parse_from_str("2021-05-05T17:11:07.246310806Z", "%+")
        .unwrap()
        .with_timezone(&Utc);
    static ref DT_1_STR: String = format!("{}", DT_1.format("%+"));
    static ref DT_1_TS: i64 = DT_1.timestamp();
}

#[serde_as]
#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestItem {
    #[polar(attribute)]
    #[serde(flatten)]
    pub flow: TestFlowMetrics,

    #[polar(attribute)]
    pub inbox_lag: i32, // todo: TelemetryValue cannot deser U32!!!!  why!?

    #[serde_as(as = "TimestampSeconds")]
    // #[serde(with = "proctor::serde")]
    pub timestamp: DateTime<Utc>,
}

impl TestItem {
    pub fn new(input_messages_per_sec: f64, inbox_lag: i32, ts: DateTime<Utc>) -> Self {
        Self {
            flow: TestFlowMetrics { input_messages_per_sec },
            inbox_lag,
            timestamp: ts,
        }
    }

    pub fn within_seconds(&self, secs: i64) -> bool {
        let now = (*DT_1).clone();
        let boundary = now - chrono::Duration::seconds(secs);
        boundary < self.timestamp
    }

    pub fn lag_duration_secs_f64(&self, message_lag: u32) -> f64 {
        (message_lag as f64) / self.flow.input_messages_per_sec
    }
}

#[test]
#[ignore = "intermittent (false?) error wrt timestamp map order and key resolution"]
fn test_context_serde() {
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_context_serde");
    let _ = main_span.enter();

    let context = TestPolicyPhaseContext {
        task_status: TestTaskStatus { last_failure: None },
        cluster_status: TestClusterStatus {
            location_code: 3,
            is_deploying: false,
            last_deployment: DT_1.clone(),
        },
        custom: HashMap::new(),
    };

    let mut expected = vec![
        Token::Map { len: None },
        Token::Str("task.last_failure"),
        Token::None,
        Token::Str("cluster.location_code"),
        Token::U32(3),
        Token::Str("cluster.is_deploying"),
        Token::Bool(false),
        Token::Str("cluster.last_deployment"),
        // Token::Str(&DT_1_STR),
        Token::Map { len: Some(2) },
        Token::Str("secs"),
        Token::I64(1620234667),
        Token::Str("nanos"),
        Token::I64(246310806),
        Token::MapEnd,
        Token::MapEnd,
    ];

    tracing::error!("DMR... A");
    let mut result = std::panic::catch_unwind(|| {
        tracing::error!("DMR... B1");
        assert_tokens(&context, expected.as_slice());
        tracing::error!("DMR... B2");
    });

    tracing::error!("DMR... C");
    if result.is_err() {
        tracing::error!(?expected, "DMR... D0");
        expected.swap(9, 11);
        expected.swap(10, 12);
        tracing::error!(?expected, "DMR... D1");
        result = std::panic::catch_unwind(|| {
            assert_tokens(&context, expected.as_slice());
        });
        tracing::error!("DMR... D2");
    }
    tracing::error!("DMR... E");

    assert_ok!(result);
}

fn make_test_policy<D>(settings: &PolicySettings) -> impl Policy<D, TestPolicyPhaseContext, (D, TestPolicyPhaseContext)>
where
    D: AppData + ToPolar + Clone,
{
    TestPolicyA::new(&settings.policies)
}

#[derive(Debug)]
struct TestPolicyA<D> {
    custom_fields: Option<HashSet<String>>,
    policies: Vec<PolicySource>,
    data_marker: PhantomData<D>,
}

impl<D> TestPolicyA<D> {
    pub fn new(policies: &Vec<PolicySource>) -> Self {
        policies.iter().for_each(|p| p.validate().expect("failed to parse policy"));
        Self {
            custom_fields: None,
            policies: policies.clone(),
            data_marker: PhantomData,
        }
    }

    #[allow(dead_code)]
    pub fn with_custom(self, custom_fields: HashSet<String>) -> Self {
        Self { custom_fields: Some(custom_fields), ..self }
    }
}

impl<D: AppData> PolicySubscription for TestPolicyA<D> {
    type Requirements = TestPolicyPhaseContext;

    fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
        tracing::warn!(
            "DMR: TestPolicy::subscription: before custom subscription:{:?}",
            subscription
        );

        let result = if let Some(ref custom_fields) = self.custom_fields {
            tracing::error!("DMR: TestPolicy::subscription: custom_fields:{:?}", custom_fields);
            subscription.with_optional_fields(custom_fields.clone())
        } else {
            subscription
        };

        tracing::warn!("DMR: TestPolicy::subscription: after custom subscription:{:?}", result);
        result
    }
}

impl<D: AppData + ToPolar + Clone> QueryPolicy for TestPolicyA<D> {
    type Item = D;
    type Context = TestPolicyPhaseContext;
    type Args = (Self::Item, Self::Context);

    fn policy_sources(&self) -> Vec<PolicySource> {
        self.policies.clone()
    }

    fn replace_sources(&mut self, sources: Vec<PolicySource>) {
        self.policies = sources;
    }

    fn initialize_policy_engine(&mut self, oso: &mut Oso) -> Result<(), PolicyError> {
        oso.register_class(
            TestPolicyPhaseContext::get_polar_class_builder()
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

    fn make_query_args(&self, item: &Self::Item, context: &Self::Context) -> Self::Args {
        (item.clone(), context.clone())
    }

    fn query_policy(&self, oso: &Oso, args: Self::Args) -> Result<QueryResult, PolicyError> {
        let q = oso.query_rule("eligible", args)?;
        QueryResult::from_query(q)
    }
}

#[allow(dead_code)]
struct TestFlow<D, C> {
    pub graph_handle: JoinHandle<()>,
    pub tx_data_source_api: stage::ActorSourceApi<Telemetry>,
    pub tx_context_source_api: stage::ActorSourceApi<Telemetry>,
    pub tx_clearinghouse_api: collection::ClearinghouseApi,
    pub tx_eligibility_api: elements::PolicyFilterApi<C>,
    pub rx_eligibility_monitor: elements::PolicyFilterMonitor<D, C>,
    pub tx_sink_api: stage::FoldApi<Vec<PolicyOutcome<D, C>>>,
    pub rx_sink: Option<oneshot::Receiver<Vec<PolicyOutcome<D, C>>>>,
}

impl<D, C> TestFlow<D, C>
where
    D: AppData + Clone + DeserializeOwned + ToPolar,
    C: ProctorContext,
{
    pub async fn new(
        telemetry_subscription: TelemetrySubscription, policy: impl Policy<D, C, (D, C)> + 'static,
    ) -> anyhow::Result<Self> {
        let telemetry_source = stage::ActorSource::<Telemetry>::new("telemetry_source");
        let tx_data_source_api = telemetry_source.tx_api();

        let ctx_source = stage::ActorSource::<Telemetry>::new("context_source");
        let tx_context_source_api = ctx_source.tx_api();

        let merge = stage::MergeN::new("source_merge", 2);

        let mut clearinghouse = collection::Clearinghouse::new("clearinghouse");
        let tx_clearinghouse_api = clearinghouse.tx_api();

        let context_subscription = policy.subscription("eligibility_context");
        let context_channel = collection::SubscriptionChannel::<C>::new("eligibility_context").await?;

        let telemetry_channel = collection::SubscriptionChannel::<D>::new("data_channel").await?;
        let eligibility = PolicyPhase::carry_policy_outcome("test_eligibility", policy).await;

        let tx_eligibility_api = eligibility.tx_api();
        let rx_eligibility_monitor = eligibility.rx_monitor();

        let mut sink = stage::Fold::<_, PolicyOutcome<D, C>, _>::new("sink", Vec::new(), |mut acc, item| {
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
            .subscribe(telemetry_subscription, &telemetry_channel.subscription_receiver)
            .await;
        clearinghouse
            .subscribe(context_subscription, &context_channel.subscription_receiver)
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

    pub async fn push_telemetry(&self, telemetry: Telemetry) -> anyhow::Result<()> {
        let (cmd, ack) = stage::ActorSourceCmd::push(telemetry);
        self.tx_data_source_api.send(cmd)?;
        ack.await.map_err(|err| err.into())
    }

    pub async fn push_context<'a, I>(&self, context_data: I) -> anyhow::Result<()>
    where
        I: IntoIterator<Item = (&'a str, TelemetryValue)>,
    {
        let telemetry = context_data.into_iter().collect();
        let (cmd, ack) = stage::ActorSourceCmd::push(telemetry);
        self.tx_context_source_api.send(cmd)?;
        ack.await.map_err(|err| err.into())
    }

    pub async fn tell_policy(
        &self, command_rx: (elements::PolicyFilterCmd<C>, oneshot::Receiver<proctor::Ack>),
    ) -> anyhow::Result<proctor::Ack> {
        self.tx_eligibility_api.send(command_rx.0)?;
        command_rx.1.await.map_err(|err| err.into())
    }

    pub async fn recv_policy_event(&mut self) -> anyhow::Result<elements::PolicyFilterEvent<D, C>> {
        self.rx_eligibility_monitor.recv().await.map_err(|err| err.into())
    }

    pub async fn inspect_policy_context(&self) -> anyhow::Result<elements::PolicyFilterDetail<C>> {
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

    pub async fn inspect_sink(&self) -> anyhow::Result<Vec<PolicyOutcome<D, C>>> {
        let (cmd, acc) = stage::FoldCmd::get_accumulation();
        self.tx_sink_api.send(cmd)?;
        acc.await
            .map(|a| {
                tracing::info!(accumulation=?a, "inspected sink accumulation");
                a
            })
            .map_err(|err| err.into())
    }

    #[tracing::instrument(level = "info", skip(self, check_size))]
    pub async fn check_sink_accumulation(
        &self, label: &str, timeout: Duration, mut check_size: impl FnMut(Vec<PolicyOutcome<D, C>>) -> bool,
    ) -> anyhow::Result<bool> {
        use std::time::Instant;
        let deadline = Instant::now() + timeout;
        let step = Duration::from_millis(50);
        let mut result = false;

        loop {
            if Instant::now() < deadline {
                let acc = self.inspect_sink().await;
                if acc.is_ok() {
                    let acc = acc?;
                    tracing::info!(?acc, len=?acc.len(), "inspecting sink");
                    result = check_size(acc);
                    if !result {
                        tracing::warn!(
                            ?result,
                            "sink length failed check predicate - retrying after {:?}.",
                            step
                        );
                        tokio::time::sleep(step).await;
                    } else {
                        tracing::info!(?result, "sink length passed check predicate.");
                        break;
                    }
                } else {
                    tracing::error!(?acc, "failed to inspect sink");
                    break;
                }
            } else {
                tracing::error!(?timeout, "check timeout exceeded - stopping check.");
                break;
            }
        }

        Ok(result)
    }

    #[tracing::instrument(level = "warn", skip(self))]
    pub async fn close(mut self) -> anyhow::Result<Vec<PolicyOutcome<D, C>>> {
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
    // let policy = TestPolicy::new(PolicySource::String(
    //     r#"eligible(item, environment) if environment.location_code == 33;"#.to_string(),
    // ));
    let policy = make_test_policy(&PolicySettings {
        required_subscription_fields: HashSet::default(),
        optional_subscription_fields: HashSet::default(),
        policies: vec![PolicySource::String(
            r#"eligible(_item, environment) if environment.location_code == 33;"#.to_string(),
        )],
    });
    let mut flow: TestFlow<Data, TestPolicyPhaseContext> =
        TestFlow::new(TelemetrySubscription::new("all_data"), policy).await?;
    tracing::warn!("test_eligibility_before_context_baseline_B");
    let now_utc = Utc::now();

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

    match assert_ok!(flow.rx_eligibility_monitor.recv().await) {
        PolicyFilterEvent::ItemBlocked(blocked) => {
            tracing::warn!("receive item blocked notification re: {:?}", blocked);
            assert_eq!(blocked, data);
        }
        PolicyFilterEvent::ContextChanged(ctx) => {
            panic!("unexpected context change:{:?}", ctx);
        }
        PolicyFilterEvent::ItemPassed => panic!("unexpected data passed policy"),
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

    // let policy = TestPolicy::new(PolicySource::String(
    //     r#"eligible(_, context) if context.cluster_status.is_deploying == false;"#.to_string(),
    // ));
    let policy = make_test_policy(&PolicySettings {
        required_subscription_fields: HashSet::default(),
        optional_subscription_fields: HashSet::default(),
        policies: vec![PolicySource::String(
            r#"eligible(_, context) if context.cluster_status.is_deploying == false;"#.to_string(),
        )],
    });

    let mut flow: TestFlow<MeasurementData, TestPolicyPhaseContext> = TestFlow::new(
        TelemetrySubscription::new("measurements").with_required_fields(maplit::hashset! {"measurement"}),
        policy,
    )
    .await?;

    tracing::warn!("DMR: 01. Make sure empty env...");

    let detail = flow.inspect_policy_context().await?;
    assert!(detail.context.is_none());

    tracing::warn!("DMR: 02. Push environment...");

    let now = Utc::now();
    let t1 = now - chrono::Duration::days(1);
    let t1_rep = format!("{}", t1.format("%+"));

    let context_data = maplit::hashmap! {
        "cluster.location_code" => 3.to_telemetry(),
        "cluster.is_deploying" => false.to_telemetry(),
        "cluster.last_deployment" => t1_rep.to_telemetry(),
    };
    let context_telemetry: Telemetry = context_data.clone().into_iter().collect();
    let context: TestPolicyPhaseContext = context_telemetry.try_into()?;
    flow.push_context(context_data).await?;

    tracing::warn!("DMR: 03. Verify environment set...");

    match assert_ok!(flow.rx_eligibility_monitor.recv().await) {
        PolicyFilterEvent::ContextChanged(Some(ctx)) => {
            tracing::warn!("notified of eligibility context change: {:?}", ctx);
            assert_eq!(
                ctx,
                TestPolicyPhaseContext {
                    task_status: TestTaskStatus { last_failure: None },
                    cluster_status: TestClusterStatus {
                        location_code: 3,
                        is_deploying: false,
                        last_deployment: t1
                    },
                    custom: HashMap::new(),
                }
            );
        }
        PolicyFilterEvent::ContextChanged(None) => panic!("did not expect to clear context"),
        PolicyFilterEvent::ItemBlocked(item) => panic!("unexpected item receipt - blocked: {:?}", item),
        PolicyFilterEvent::ItemPassed => panic!("unexpected data passed policy"),
    };
    tracing::warn!("DMR: 04. environment change verified.");

    let detail = flow.inspect_policy_context().await?;
    tracing::warn!("DMR: policy detail = {:?}", detail);
    assert!(detail.context.is_some());

    tracing::warn!("DMR: 05. Push Item...");

    let t1: Telemetry = maplit::hashmap! {"measurement" => std::f64::consts::PI.to_telemetry()}
        .into_iter()
        .collect();
    assert_eq!(
        MeasurementData { measurement: std::f64::consts::PI },
        t1.try_into::<MeasurementData>()?
    );

    flow.push_telemetry(maplit::hashmap! {"measurement".to_string() => std::f64::consts::PI.to_telemetry()}.into())
        .await?;

    tracing::warn!("DMR: 06. Look for Item in sink...");

    assert!(
        flow.check_sink_accumulation("first", Duration::from_secs(2), |acc| acc.len() == 1)
            .await?
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
            PolicyOutcome::new(
                MeasurementData { measurement: std::f64::consts::PI },
                context.clone(),
                QueryResult::passed_without_bindings()
            ),
            PolicyOutcome::new(
                MeasurementData { measurement: std::f64::consts::TAU },
                context.clone(),
                QueryResult::passed_without_bindings()
            ),
        ]
    );
    Ok(())
}

#[test]
fn test_item_serde() {
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_item_serde");
    let _ = main_span.enter();

    let item = TestItem::new(std::f64::consts::PI, 3, DT_1.clone());
    let json = serde_json::to_string(&item).unwrap();
    assert_eq!(
        json,
        format!(
            r#"{{"input_messages_per_sec":{},"inbox_lag":3,"timestamp":{}}}"#,
            std::f64::consts::PI,
            *DT_1_TS
        )
    );

    let telemetry = Telemetry::try_from(&item).unwrap();
    assert_eq!(
        telemetry,
        maplit::hashmap! {
            "input_messages_per_sec" => std::f64::consts::PI.to_telemetry(),
            "inbox_lag" => 3.to_telemetry(),
            "timestamp" => DT_1_TS.to_telemetry(),
        }
        .into_iter()
        .collect()
    );

    // dmr commented out since full round trip not possible due to lost timestamp precision with
    // dmr timestamp serde approach.
    // assert_tokens(
    //     &item,
    //     &vec![
    //         Token::Map { len: None },
    //         Token::Str("input_messages_per_sec"),
    //         Token::F64(std::f64::consts::PI),
    //         Token::Str("inbox_lag"),
    //         Token::I32(3),
    //         Token::Str("timestamp"),
    //         Token::I64(*DT_1_TS),
    //         Token::MapEnd,
    //     ],
    // );
}

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestFlowMetrics {
    #[polar(attribute)]
    pub input_messages_per_sec: f64,
}

#[derive(Debug)]
struct TestPolicyB {
    policy: String,
    subscription_extension: HashSet<String>,
}

impl TestPolicyB {
    pub fn new<S: Into<String>>(policy: S) -> Self {
        Self::new_with_extension(policy, HashSet::<String>::new())
    }

    pub fn new_with_extension<S0, S1>(policy: S0, subscription_extension: HashSet<S1>) -> Self
    where
        S0: Into<String>,
        S1: Into<String>,
    {
        let policy = policy.into();
        let subscription_extension = subscription_extension.into_iter().map(|s| s.into()).collect();

        let polar = polar_core::polar::Polar::new();
        polar.load_str(policy.as_str()).expect("failed to parse policy text");
        Self { policy, subscription_extension }
    }
}

impl PolicySubscription for TestPolicyB {
    type Requirements = TestPolicyPhaseContext;

    fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
        subscription.with_optional_fields(self.subscription_extension.clone())
    }
}

impl QueryPolicy for TestPolicyB {
    type Args = (Self::Item, Self::Context);
    type Context = TestPolicyPhaseContext;
    type Item = TestItem;

    fn initialize_policy_engine(&mut self, oso: &mut Oso) -> Result<(), PolicyError> {
        oso.register_class(
            TestItem::get_polar_class_builder()
                .name("TestMetricCatalog")
                .add_method("lag_duration", TestItem::lag_duration_secs_f64)
                .add_method("within_seconds", TestItem::within_seconds)
                .build(),
        )?;

        oso.register_class(
            TestPolicyPhaseContext::get_polar_class_builder()
                .name("PhaseContext")
                .add_method("custom", ProctorContext::custom)
                // .add_method("custom", |ctx: &TestPhaseContext| ctx.custom())
                .build(),
        )?;

        Ok(())
    }

    fn make_query_args(&self, item: &Self::Item, context: &Self::Context) -> Self::Args {
        (item.clone(), context.clone())
    }

    fn query_policy(&self, oso: &Oso, args: Self::Args) -> Result<QueryResult, PolicyError> {
        QueryResult::from_query(oso.query_rule("eligible", args)?)
    }

    fn policy_sources(&self) -> Vec<PolicySource> {
        vec![PolicySource::from_string(self.policy.clone())]
    }

    fn replace_sources(&mut self, sources: Vec<PolicySource>) {
        let mut new_policy = String::new();
        for s in sources {
            let source_policy: String = s.into();
            new_policy.push_str(source_policy.as_str());
        }
        self.policy = new_policy;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_eligibility_w_pass_and_blocks() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_eligibility_w_pass_and_blocks");
    let _ = main_span.enter();

    let subscription = TelemetrySubscription::new("measurements").with_required_fields(maplit::hashset! {
        "input_messages_per_sec",
        "inbox_lag",
        "timestamp",
    });

    let policy = TestPolicyB::new(r#"eligible(_item, environment) if environment.cluster_status.location_code == 33;"#);

    let mut flow = TestFlow::new(subscription, policy).await?;

    flow.push_context(maplit::hashmap! {
        "cluster.location_code" => 33.to_telemetry(),
        "cluster.is_deploying" => false.to_telemetry(),
        "cluster.last_deployment" => DT_1_STR.as_str().to_telemetry(),
    })
    .await?;

    let event = flow.recv_policy_event().await?;
    claim::assert_matches!(event, elements::PolicyFilterEvent::ContextChanged(_));
    tracing::warn!(?event, "DMR-A: environment changed confirmed");

    let ts = *DT_1 + chrono::Duration::hours(1);
    let item = TestItem::new(std::f64::consts::PI, 1, ts);
    tracing::warn!(?item, "DMR-A.1: created item to push.");
    let telemetry = Telemetry::try_from(&item);
    tracing::warn!(?item, ?telemetry, "DMR-A.2: converted item to telemetry and pushing...");
    flow.push_telemetry(telemetry?).await?;
    let event = flow.recv_policy_event().await?;
    claim::assert_matches!(event, elements::PolicyFilterEvent::ItemPassed);

    tracing::info!("waiting for item to reach sink...");
    assert!(
        flow.check_sink_accumulation("first", Duration::from_secs(2), |acc| acc.len() == 1)
            .await?
    );

    tracing::warn!("DMR-A.3: pushed telemetry and now pushing context update...");

    flow.push_context(maplit::hashmap! {
        "cluster.location_code" => 19.to_telemetry(),
        // "cluster.is_deploying" => false.to_telemetry(),
        // "cluster.last_deployment" => t1_rep.to_telemetry(),
    })
    .await?;
    let event = flow.recv_policy_event().await?;
    claim::assert_matches!(event, elements::PolicyFilterEvent::ContextChanged(_));
    tracing::warn!(?event, "DMR-B: environment changed confirmed");

    let item = TestItem::new(std::f64::consts::E, 2, ts);
    let telemetry = Telemetry::try_from(&item);
    flow.push_telemetry(telemetry?).await?;
    let event = flow.recv_policy_event().await?;
    claim::assert_matches!(event, elements::PolicyFilterEvent::ItemBlocked(_));
    tracing::warn!(?event, "DMR-C: item dropped confirmed");

    let item = TestItem::new(std::f64::consts::FRAC_1_PI, 3, ts);
    let telemetry = Telemetry::try_from(&item)?;
    flow.push_telemetry(telemetry).await?;
    let event = flow.recv_policy_event().await?;
    claim::assert_matches!(event, elements::PolicyFilterEvent::ItemBlocked(_));
    tracing::warn!(?event, "DMR-D: item dropped confirmed");

    let item = TestItem::new(std::f64::consts::FRAC_1_SQRT_2, 4, ts);
    let telemetry = Telemetry::try_from(&item)?;
    flow.push_telemetry(telemetry).await?;
    let event = flow.recv_policy_event().await?;
    claim::assert_matches!(event, elements::PolicyFilterEvent::ItemBlocked(_));
    tracing::warn!(?event, "DMR-E: item dropped confirmed");

    flow.push_context(maplit::hashmap! { "cluster.location_code" => 33.to_telemetry(), })
        .await?;

    let event = flow.recv_policy_event().await?;
    tracing::info!(?event, "DMR-E.1: policy event received.");
    assert_eq!(
        event,
        elements::PolicyFilterEvent::ContextChanged(Some(TestPolicyPhaseContext {
            task_status: TestTaskStatus { last_failure: None },
            cluster_status: TestClusterStatus {
                location_code: 33,
                is_deploying: false,
                last_deployment: DT_1.clone(),
            },
            custom: HashMap::new(),
        }))
    );
    claim::assert_matches!(event, elements::PolicyFilterEvent::ContextChanged(_));
    tracing::warn!(?event, "DMR-F: environment changed confirmed");

    let item = TestItem::new(std::f64::consts::LN_2, 5, ts);
    let telemetry = Telemetry::try_from(&item)?;
    flow.push_telemetry(telemetry).await?;

    tracing::info!("waiting for item to reach sink...");
    assert!(
        flow.check_sink_accumulation("second", Duration::from_secs(2), |acc| acc.len() == 2)
            .await?
    );

    let actual: Vec<PolicyOutcome<TestItem, TestPolicyPhaseContext>> = flow.close().await?;

    tracing::warn!(?actual, "DMR: 08. Verify final accumulation...");
    let actual_vals: Vec<(f64, i32)> = actual
        .into_iter()
        .map(|a| (a.item.flow.input_messages_per_sec, a.item.inbox_lag))
        .collect();

    assert_eq!(
        actual_vals,
        vec![(std::f64::consts::PI, 1,), (std::f64::consts::LN_2, 5,),]
    );
    Ok(())
}

// #[tokio::test(flavor="multi_thread", worker_threads = 4)]
#[tokio::test]
async fn test_eligibility_w_custom_fields() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_eligibility_w_custom_fields");
    let _ = main_span.enter();

    let subscription = TelemetrySubscription::new("measurements").with_required_fields(maplit::hashset! {
        "input_messages_per_sec",
        "inbox_lag",
        "timestamp",
    });

    let policy = TestPolicyB::new_with_extension(
        r#"eligible(_item, environment) if
            c = environment.custom() and
            c.cat = "Otis";"#,
        maplit::hashset! {"cat"},
    );

    let mut flow = TestFlow::new(subscription, policy).await?;

    // TestEnvironment::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_string()}),
    flow.push_context(maplit::hashmap! {
        "cluster.location_code" => 33.to_telemetry(),
        "cluster.is_deploying" => false.to_telemetry(),
        "cluster.last_deployment" => DT_1_STR.as_str().to_telemetry(),
        "cat" => "Otis".to_telemetry(),
    })
    .await?;

    let event = flow.recv_policy_event().await?;
    tracing::info!(?event, "verifying environment update...");
    claim::assert_matches!(event, elements::PolicyFilterEvent::ContextChanged(_));

    let ts = Utc::now().into();
    let item = TestItem::new(std::f64::consts::PI, 1, ts);
    let telemetry = Telemetry::try_from(&item);
    tracing::info!(?telemetry, ?item, "pushing item telemetry");
    flow.push_telemetry(telemetry?).await?;

    let item = TestItem::new(std::f64::consts::TAU, 2, ts);
    let telemetry = Telemetry::try_from(&item);
    flow.push_telemetry(telemetry?).await?;

    let actual = flow.close().await?;
    tracing::info!(?actual, "verifying actual result...");

    let actual_vals: Vec<(f64, i32)> = actual
        .into_iter()
        .map(|a| (a.item.flow.input_messages_per_sec, a.item.inbox_lag))
        .collect();

    assert_eq!(
        actual_vals,
        vec![(std::f64::consts::PI, 1), (std::f64::consts::TAU, 2),],
    );
    Ok(())
}

#[tokio::test]
async fn test_eligibility_w_item_n_env() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_eligibility_w_item_n_env");
    let _ = main_span.enter();

    tracing::info!("DMR-A:create flow...");

    let subscription = TelemetrySubscription::new("data").with_required_fields(maplit::hashset! {
        "input_messages_per_sec",
        "inbox_lag",
        "timestamp",
    });

    let policy = TestPolicyB::new_with_extension(
        r#"eligible(item, env) if proper_cat(item, env) and lag_2(item, env);
proper_cat(_, env) if env.custom.cat = "Otis";
lag_2(_item: TestMetricCatalog{ inbox_lag: 2 }, _);"#,
        maplit::hashset! { "cat" },
    );

    // another form for policy that works
    // r#"
    //     eligible(item, env) if proper_cat(item, env) and lag_2(item, env);
    //     proper_cat(_, env) if env.custom.cat = "Otis";
    //     lag_2(item, _) if item.inbox_lag = 2;
    // "#,
    let flow = TestFlow::new(subscription, policy).await?;

    tracing::info!("DMR-B:push env...");
    flow.push_context(maplit::hashmap! {
        "cluster.location_code" => 23.to_telemetry(),
        "cluster.is_deploying" => false.to_telemetry(),
        "cluster.last_deployment" => DT_1_STR.as_str().to_telemetry(),
        "cat" => "Otis".to_telemetry(),
    })
    .await?;
    tracing::info!("DMR-C:verify context...");

    let ts = Utc::now().into();
    let item = TestItem::new(std::f64::consts::PI, 1, ts);
    let telemetry = Telemetry::try_from(&item);
    flow.push_telemetry(telemetry?).await?;

    let item = TestItem::new(std::f64::consts::TAU, 2, ts);
    let telemetry = Telemetry::try_from(&item);
    flow.push_telemetry(telemetry?).await?;

    let actual = flow.close().await?;
    tracing::info!(?actual, "verifying actual result...");
    let actual_vals: Vec<(f64, i32)> = actual
        .into_iter()
        .map(|a| (a.item.flow.input_messages_per_sec, a.item.inbox_lag))
        .collect();

    assert_eq!(actual_vals, vec![(std::f64::consts::TAU, 2),]);
    Ok(())
}

#[tokio::test]
async fn test_eligibility_replace_policy() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_eligibility_replace_policy");
    let _ = main_span.enter();

    let boundary_age_secs = 60 * 60;
    let good_ts = chrono::NaiveDateTime::from_timestamp(*DT_1_TS, 0);
    let good_ts = chrono::DateTime::from_utc(good_ts, chrono::Utc);
    let too_old_ts = good_ts.clone() - chrono::Duration::seconds(boundary_age_secs + 5);

    let data_subscription = TelemetrySubscription::new("data").with_required_fields(maplit::hashset! {
        "input_messages_per_sec",
        "inbox_lag",
        "timestamp",
    });

    let policy_1 = r#"eligible(_, env: PhaseContext) if env.custom.cat = "Otis";"#;
    let policy_2 = format!("eligible(metrics, _) if metrics.within_seconds({});", boundary_age_secs);
    tracing::info!(?policy_2, "DMR: policy with timestamp");

    let policy = TestPolicyB::new_with_extension(policy_1, maplit::hashset! {"cat"});

    let mut flow = TestFlow::new(data_subscription, policy).await?;

    let context_data = maplit::hashmap! {
        "cluster.location_code" => 23.to_telemetry(),
        "cluster.is_deploying" => false.to_telemetry(),
        "cluster.last_deployment" => DT_1_STR.as_str().to_telemetry(),
        "cat" => "Otis".to_telemetry(),
    };
    let context_telemetry: Telemetry = context_data.clone().into_iter().collect();
    let context: TestPolicyPhaseContext = context_telemetry.try_into()?;
    flow.push_context(context_data).await?;

    let event = flow.recv_policy_event().await?;
    claim::assert_matches!(event, elements::PolicyFilterEvent::ContextChanged(_));
    tracing::warn!(?event, "DMR-A: environment changed confirmed");

    let item_1 = TestItem::new(std::f64::consts::PI, 1, too_old_ts);
    let telemetry_1 = Telemetry::try_from(&item_1)?;
    flow.push_telemetry(telemetry_1.clone()).await?;

    let item_2 = TestItem::new(17.327, 2, good_ts);
    let telemetry_2 = Telemetry::try_from(&item_2)?;
    flow.push_telemetry(telemetry_2.clone()).await?;

    tracing::info!("replace policy and re-send");
    let cmd_rx = elements::PolicyFilterCmd::replace_policy(elements::PolicySource::String(policy_2.to_string()));
    flow.tell_policy(cmd_rx).await?;

    tracing::info!("after policy change, pushing telemetry data...");
    flow.push_telemetry(telemetry_1).await?;
    flow.push_telemetry(telemetry_2).await?;

    let actual = flow.close().await?;
    tracing::info!(?actual, "verifying actual result...");
    assert_eq!(
        actual,
        vec![
            PolicyOutcome::new(
                TestItem::new(std::f64::consts::PI, 1, too_old_ts),
                context.clone(),
                QueryResult::passed_without_bindings()
            ),
            PolicyOutcome::new(
                TestItem::new(17.327, 2, good_ts),
                context.clone(),
                QueryResult::passed_without_bindings()
            ),
            PolicyOutcome::new(
                TestItem::new(17.327, 2, good_ts),
                context.clone(),
                QueryResult::passed_without_bindings()
            ),
        ]
    );
    Ok(())
}
