use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use ::serde::de::DeserializeOwned;
use ::serde::{Deserialize, Serialize};
use ::serde_with::{serde_as, TimestampSeconds};
use async_trait::async_trait;
use chrono::*;
use claim::*;
use once_cell::sync::Lazy;
use oso::{Oso, PolarClass, ToPolar};
use pretty_assertions::assert_eq;
use pretty_snowflake::{AlphabetCodec, Id, IdPrettifier, Label, PrettyIdGenerator};
use proctor::elements::telemetry::{TableValue, ToTelemetry};
use proctor::elements::{
    self, telemetry, Policy, PolicyFilterEvent, PolicyOutcome, PolicyRegistry, PolicySettings, PolicySource,
    PolicySubscription, QueryPolicy, QueryResult, Telemetry, TelemetryValue, Timestamp,
};
use proctor::error::{PolicyError, ProctorError};
use proctor::graph::stage::{self, WithApi, WithMonitor};
use proctor::graph::{Connect, Graph, SinkShape, SourceShape, UniformFanInShape};
use proctor::phases::policy_phase::PolicyPhase;
use proctor::phases::sense::clearinghouse::TelemetryCacheSettings;
use proctor::phases::sense::{
    self, CorrelationGenerator, SubscriptionRequirements, TelemetrySubscription, SUBSCRIPTION_CORRELATION,
    SUBSCRIPTION_TIMESTAMP,
};
use proctor::phases::DataSet;
use proctor::AppData;
use proctor::ProctorContext;
use serde_test::{assert_tokens, Token};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

#[derive(PolarClass, Label, Debug, Clone, Serialize, Deserialize)]
pub struct Data {
    pub input_messages_per_sec: f64,
    pub inbox_lag: i64,
}

impl PartialEq for Data {
    fn eq(&self, other: &Self) -> bool {
        self.input_messages_per_sec.eq(&other.input_messages_per_sec) && self.inbox_lag.eq(&other.inbox_lag)
    }
}

#[derive(PolarClass, Label, Debug, Clone, Serialize, Deserialize)]
pub struct TestPolicyPhaseContext {
    #[polar(attribute)]
    #[serde(flatten)]
    pub task_status: TestTaskStatus,
    #[polar(attribute)]
    #[serde(flatten)]
    pub cluster_status: TestClusterStatus,

    #[polar(attribute)]
    #[serde(flatten)]
    pub custom: telemetry::TableValue,
}

impl PartialEq for TestPolicyPhaseContext {
    fn eq(&self, other: &Self) -> bool {
        self.task_status == other.task_status && self.cluster_status == other.cluster_status
    }
}

#[async_trait]
impl ProctorContext for TestPolicyPhaseContext {
    type ContextData = Self;
    type Error = ProctorError;

    fn custom(&self) -> telemetry::TableType {
        (&*self.custom).clone()
    }
}

impl SubscriptionRequirements for TestPolicyPhaseContext {
    fn required_fields() -> HashSet<String> {
        maplit::hashset! {
            "cluster.location_code".into(),
            "cluster.is_deploying".into(),
            "cluster.last_deployment".into(),
        }
    }

    fn optional_fields() -> HashSet<String> {
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

static DT_1: Lazy<DateTime<Utc>> = Lazy::new(|| {
    DateTime::parse_from_str("2021-05-05T17:11:07.246310806Z", "%+")
        .unwrap()
        .with_timezone(&Utc)
});
static DT_1_STR: Lazy<String> = Lazy::new(|| format!("{}", DT_1.format("%+")));
static DT_1_TS: Lazy<i64> = Lazy::new(|| DT_1.timestamp());

#[serde_as]
#[derive(PolarClass, Label, Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestItem {
    #[polar(attribute)]
    #[serde(flatten)]
    pub flow: TestFlowMetrics,

    #[polar(attribute)]
    pub inbox_lag: i32, // todo: TelemetryValue cannot deser U32!!!!  why!?

    #[serde_as(as = "TimestampSeconds")]
    pub my_timestamp: DateTime<Utc>,
}

impl TestItem {
    pub fn new(input_messages_per_sec: f64, inbox_lag: i32, ts: DateTime<Utc>) -> Self {
        Self {
            flow: TestFlowMetrics { input_messages_per_sec },
            inbox_lag,
            my_timestamp: ts,
        }
    }

    pub fn within_seconds(&self, secs: i64) -> bool {
        let now = (*DT_1).clone();
        let boundary = now - chrono::Duration::seconds(secs);
        boundary < self.my_timestamp
    }

    pub fn lag_duration_secs_f64(&self, message_lag: u32) -> f64 {
        (message_lag as f64) / self.flow.input_messages_per_sec
    }
}

#[test]
#[ignore = "intermittent (false?) error wrt timestamp map order and key resolution"]
fn test_context_serde() {
    Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_context_serde");
    let _ = main_span.enter();

    let context = TestPolicyPhaseContext {
        task_status: TestTaskStatus { last_failure: None },
        cluster_status: TestClusterStatus {
            location_code: 3,
            is_deploying: false,
            last_deployment: DT_1.clone(),
        },
        custom: TableValue::new(),
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
        Token::Map { len: Some(2) },
        Token::Str("secs"),
        Token::I64(1620234667),
        Token::Str("nanos"),
        Token::I64(246310806),
        Token::MapEnd,
        Token::MapEnd,
    ];

    tracing::error!("A");
    let mut result = std::panic::catch_unwind(|| {
        tracing::error!("B1");
        assert_tokens(&context, expected.as_slice());
        tracing::error!("B2");
    });

    tracing::error!("C");
    if result.is_err() {
        tracing::error!(?expected, "D0");
        expected.swap(9, 11);
        expected.swap(10, 12);
        tracing::error!(?expected, "D1");
        result = std::panic::catch_unwind(|| {
            assert_tokens(&context, expected.as_slice());
        });
        tracing::error!("D2");
    }
    tracing::error!("E");

    assert_ok!(result);
}

fn make_test_policy<T>(
    settings: &PolicySettings<PolicyData>,
) -> impl Policy<DataSet<T>, DataSet<TestPolicyPhaseContext>, (T, TestPolicyPhaseContext), TemplateData = PolicyData>
where
    T: AppData + Label + ToPolar + Serialize + DeserializeOwned,
{
    let data = settings.template_data.clone();
    TestPolicyA::new(settings, data)
}

const POLICY_A_TEMPLATE_NAME: &'static str = "policy_a";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PolicyData {
    pub location_code: u32,
}

#[derive(Debug)]
struct TestPolicyA<T> {
    custom_fields: Option<HashSet<String>>,
    policies: Vec<PolicySource>,
    policy_template_data: Option<PolicyData>,
    data_marker: PhantomData<T>,
}

impl<T> TestPolicyA<T> {
    pub fn new(settings: &PolicySettings<PolicyData>, policy_template_data: Option<PolicyData>) -> Self {
        Self {
            custom_fields: None,
            policies: settings.policies.clone(),
            policy_template_data,
            data_marker: PhantomData,
        }
    }

    #[allow(dead_code)]
    pub fn with_custom(self, custom_fields: HashSet<String>) -> Self {
        Self { custom_fields: Some(custom_fields), ..self }
    }
}

impl<T> PolicySubscription for TestPolicyA<T>
where
    T: AppData + Label + oso::ToPolar,
{
    type Requirements = DataSet<TestPolicyPhaseContext>;

    fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
        tracing::warn!(
            "TestPolicy::subscription: before custom subscription:{:?}",
            subscription
        );

        let result = if let Some(ref custom_fields) = self.custom_fields {
            tracing::error!("TestPolicy::subscription: custom_fields:{:?}", custom_fields);
            subscription.with_optional_fields(custom_fields.clone())
        } else {
            subscription
        };

        tracing::warn!("TestPolicy::subscription: after custom subscription:{:?}", result);
        result
    }
}

impl<T: AppData + ToPolar + Clone> QueryPolicy for TestPolicyA<T>
where
    T: Label,
{
    type Args = (T, TestPolicyPhaseContext);
    type Context = DataSet<TestPolicyPhaseContext>;
    type Item = DataSet<T>;
    type TemplateData = PolicyData;

    fn base_template_name() -> &'static str {
        POLICY_A_TEMPLATE_NAME
    }

    fn policy_template_data(&self) -> Option<&Self::TemplateData> {
        self.policy_template_data.as_ref()
    }

    fn policy_template_data_mut(&mut self) -> Option<&mut Self::TemplateData> {
        self.policy_template_data.as_mut()
    }

    fn sources(&self) -> &[PolicySource] {
        self.policies.as_slice()
    }

    fn sources_mut(&mut self) -> &mut Vec<PolicySource> {
        &mut self.policies
    }

    fn initialize_policy_engine(&self, oso: &mut Oso) -> Result<(), PolicyError> {
        assert_ok!(oso.register_class(
            TestPolicyPhaseContext::get_polar_class_builder()
                .name("TestEnvironment")
                .add_method("custom", ProctorContext::custom)
                .build(),
        ));

        assert_ok!(oso.register_class(
            TestTaskStatus::get_polar_class_builder()
                .name("TestTaskStatus")
                .add_method(
                    "last_failure_within_seconds",
                    TestTaskStatus::last_failure_within_seconds,
                )
                .build(),
        ));

        Ok(())
    }

    fn make_query_args(&self, item: &Self::Item, context: &Self::Context) -> Self::Args {
        (item.as_ref().clone(), context.as_ref().clone())
    }

    fn query_policy(&self, oso: &Oso, args: Self::Args) -> Result<QueryResult, PolicyError> {
        let q = assert_ok!(oso.query_rule("eligible", args));
        QueryResult::from_query(q)
    }
}

#[allow(dead_code)]
struct TestFlow<T, C, D>
where
    T: Label,
    C: Label,
{
    pub id_generator: CorrelationGenerator,
    pub graph_handle: JoinHandle<()>,
    pub tx_data_source_api: stage::ActorSourceApi<Telemetry>,
    pub tx_context_source_api: stage::ActorSourceApi<Telemetry>,
    pub tx_clearinghouse_api: sense::ClearinghouseApi,
    pub tx_eligibility_api: elements::PolicyFilterApi<DataSet<C>, D>,
    pub rx_eligibility_monitor: elements::PolicyFilterMonitor<DataSet<T>, DataSet<C>>,
    pub tx_sink_api: stage::FoldApi<Vec<PolicyOutcome<DataSet<T>, DataSet<C>>>>,
    pub rx_sink: Option<oneshot::Receiver<Vec<PolicyOutcome<DataSet<T>, DataSet<C>>>>>,
}

impl<T, C, D> TestFlow<T, C, D>
where
    T: AppData + Label + DeserializeOwned + ToPolar,
    C: ProctorContext + Label + PartialEq,
    D: Debug + Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub async fn new<P>(telemetry_subscription: TelemetrySubscription, policy: P) -> anyhow::Result<Self>
    where
        P: Policy<DataSet<T>, DataSet<C>, (T, C)>
            + QueryPolicy<Item = DataSet<T>, Context = DataSet<C>, TemplateData = D>
            + 'static,
    {
        let telemetry_source = stage::ActorSource::<Telemetry>::new("telemetry_source");
        let tx_data_source_api = telemetry_source.tx_api();

        let ctx_source = stage::ActorSource::<Telemetry>::new("context_source");
        let tx_context_source_api = ctx_source.tx_api();

        let merge = stage::MergeN::new("source_merge", 2);

        let id_generator = PrettyIdGenerator::single_node(IdPrettifier::<AlphabetCodec>::default());
        proctor::phases::set_sensor_data_id_generator(id_generator.clone()).await;
        let mut clearinghouse = sense::Clearinghouse::new("clearinghouse", &TelemetryCacheSettings::default());
        let tx_clearinghouse_api = clearinghouse.tx_api();

        // todo: expand testing to include settings-based reqd/opt subscription fields
        let settings: PolicySettings<D> = PolicySettings::default();
        let context_subscription = policy.subscription("eligibility_context", &settings);
        let context_channel = assert_ok!(sense::SubscriptionChannel::<C>::new("eligibility_context".into()).await);

        let telemetry_channel = assert_ok!(sense::SubscriptionChannel::<T>::new("data_channel".into()).await);
        let eligibility = assert_ok!(PolicyPhase::carry_policy_outcome("test_eligibility", policy).await);

        let tx_eligibility_api = eligibility.tx_api();
        let rx_eligibility_monitor = eligibility.rx_monitor();

        let mut sink =
            stage::Fold::<_, PolicyOutcome<DataSet<T>, DataSet<C>>, _>::new("sink", Vec::new(), |mut acc, item| {
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

        assert!(eligibility.context_inlet().is_attached().await);
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
            id_generator,
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
        stage::ActorSourceCmd::push(&self.tx_data_source_api, telemetry)
            .await
            .map_err(|err| err.into())
    }

    pub async fn push_context<'a, I>(&self, context_data: I) -> anyhow::Result<()>
    where
        I: IntoIterator<Item = (&'a str, TelemetryValue)>,
    {
        let telemetry = context_data.into_iter().collect();
        stage::ActorSourceCmd::push(&self.tx_context_source_api, telemetry)
            .await
            .map_err(|err| err.into())
    }

    #[allow(dead_code)]
    pub async fn tell_policy(
        &self,
        command_rx: (
            elements::PolicyFilterCmd<DataSet<C>, D>,
            oneshot::Receiver<proctor::Ack>,
        ),
    ) -> anyhow::Result<proctor::Ack> {
        self.tx_eligibility_api.send(DataSet::new(command_rx.0).await)?;
        command_rx.1.await.map_err(|err| err.into())
    }

    pub async fn recv_policy_event(
        &mut self,
    ) -> anyhow::Result<Arc<DataSet<PolicyFilterEvent<DataSet<T>, DataSet<C>>>>> {
        self.rx_eligibility_monitor.recv().await.map_err(|err| err.into())
    }

    pub async fn inspect_policy_context(&self) -> anyhow::Result<elements::PolicyFilterDetail<DataSet<C>, D>> {
        elements::PolicyFilterCmd::inspect(&self.tx_eligibility_api)
            .await
            .map(|d| {
                tracing::info!(detail=?d, "inspected policy.");
                d
            })
            .map_err(|err| err.into())
    }

    pub async fn inspect_sink(&self) -> anyhow::Result<Vec<PolicyOutcome<DataSet<T>, DataSet<C>>>> {
        stage::FoldCmd::get_accumulation(&self.tx_sink_api)
            .await
            .map(|a| {
                tracing::info!(accumulation=?a, "inspected sink accumulation");
                a
            })
            .map_err(|err| err.into())
    }

    #[tracing::instrument(level = "info", skip(self, check_size))]
    pub async fn check_sink_accumulation(
        &self, label: &str, timeout: Duration,
        mut check_size: impl FnMut(Vec<PolicyOutcome<DataSet<T>, DataSet<C>>>) -> bool,
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
    pub async fn close(mut self) -> anyhow::Result<Vec<PolicyOutcome<DataSet<T>, DataSet<C>>>> {
        assert_ok!(stage::ActorSourceCmd::stop(&self.tx_data_source_api).await);
        assert_ok!(stage::ActorSourceCmd::stop(&self.tx_context_source_api).await);
        assert_ok!(self.graph_handle.await);

        self.rx_sink.take().unwrap().await.map_err(|err| err.into())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_eligibility_before_context_baseline() -> anyhow::Result<()> {
    Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_eligibility_before_context_baseline");
    let _ = main_span.enter();

    tracing::warn!("test_eligibility_before_context_baseline_A");
    let policy = make_test_policy::<Data>(&PolicySettings {
        required_subscription_fields: HashSet::default(),
        optional_subscription_fields: HashSet::default(),
        policies: vec![assert_ok!(PolicySource::from_template_string(
            POLICY_A_TEMPLATE_NAME,
            r##"eligible(_item, environment) if environment.location_code == {{location_code}};"##,
        ))],
        template_data: Some(PolicyData { location_code: 33 }),
    });
    let mut flow: TestFlow<_, _, PolicyData> =
        assert_ok!(TestFlow::new(TelemetrySubscription::new("all_data"), policy).await);
    tracing::warn!("test_eligibility_before_context_baseline_B");

    let data = Data {
        input_messages_per_sec: std::f64::consts::PI,
        inbox_lag: 3,
    };

    let mut data_telemetry = assert_ok!(Telemetry::try_from(&data));
    data_telemetry.remove(SUBSCRIPTION_TIMESTAMP);
    data_telemetry.remove(SUBSCRIPTION_CORRELATION);
    tracing::warn!(?data_telemetry, "test_eligibility_before_context_baseline_C");

    assert_ok!(flow.push_telemetry(data_telemetry).await);
    tracing::warn!("test_eligibility_before_context_baseline_D");
    let actual = assert_ok!(flow.inspect_sink().await);
    tracing::warn!("test_eligibility_before_context_baseline_E");
    assert!(actual.is_empty());

    tracing::warn!("test_eligibility_before_context_baseline_F");

    match assert_ok!(flow.rx_eligibility_monitor.recv().await).as_ref().as_ref() {
        PolicyFilterEvent::ItemBlocked(blocked, _) => {
            tracing::warn!("receive item blocked notification re: {:?}", blocked);
            assert_eq!(blocked, &data);
        },
        PolicyFilterEvent::ContextChanged(ctx) => {
            panic!("unexpected context change:{:?}", ctx);
        },
        PolicyFilterEvent::ItemPassed(data, _) => panic!("unexpected data passed policy {:?}", data),
    }

    let actual = assert_ok!(flow.close().await);
    tracing::warn!("final accumulation:{:?}", actual);

    assert!(actual.is_empty());
    tracing::warn!("test_eligibility_before_context_baseline_G");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_eligibility_happy_context() -> anyhow::Result<()> {
    Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_eligibility_happy_context");
    let _ = main_span.enter();

    #[derive(PolarClass, Label, Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct MeasurementData {
        measurement: f64,
    }

    let policy = make_test_policy(&PolicySettings {
        required_subscription_fields: HashSet::default(),
        optional_subscription_fields: HashSet::default(),
        policies: vec![assert_ok!(PolicySource::from_complete_string(
            TestPolicyA::<MeasurementData>::base_template_name(),
            r##"eligible(_, context) if context.cluster_status.is_deploying == false;"##,
        ))],
        template_data: None,
    });

    let mut flow: TestFlow<MeasurementData, TestPolicyPhaseContext, PolicyData> = assert_ok!(
        TestFlow::new(
            TelemetrySubscription::new("measurements").with_required_fields(maplit::hashset! {"measurement"}),
            policy,
        )
        .await
    );

    tracing::warn!("01. Make sure empty env...");

    let detail = assert_ok!(flow.inspect_policy_context().await);
    assert!(detail.context.is_none());

    tracing::warn!("02. Push environment...");

    let now = Utc::now();
    let t1 = now - chrono::Duration::days(1);
    let t1_rep = format!("{}", t1.format("%+"));

    let context_data = maplit::hashmap! {
        "cluster.location_code" => 3.to_telemetry(),
        "cluster.is_deploying" => false.to_telemetry(),
        "cluster.last_deployment" => t1_rep.to_telemetry(),
    };
    let mut context_telemetry: Telemetry = context_data.clone().into_iter().collect();
    context_telemetry.insert(SUBSCRIPTION_TIMESTAMP.to_string(), Timestamp::now().into());
    context_telemetry.insert(SUBSCRIPTION_CORRELATION.to_string(), flow.id_generator.next_id().into());
    tracing::warn!(?context_telemetry, "converting context telemetry...");
    let context: TestPolicyPhaseContext = assert_ok!(context_telemetry.try_into());
    tracing::warn!(?context, "pushing context object...");
    assert_ok!(flow.push_context(context_data).await);

    tracing::warn!("03. Verify environment set...");

    match assert_ok!(flow.rx_eligibility_monitor.recv().await).as_ref().as_ref() {
        PolicyFilterEvent::ContextChanged(Some(ctx)) => {
            tracing::warn!("notified of eligibility context change: {:?}", ctx);
            assert_eq!(
                ctx,
                &TestPolicyPhaseContext {
                    task_status: TestTaskStatus { last_failure: None },
                    cluster_status: TestClusterStatus {
                        location_code: 3,
                        is_deploying: false,
                        last_deployment: t1
                    },
                    custom: TableValue::new(),
                }
            );
        },
        PolicyFilterEvent::ContextChanged(None) => panic!("did not expect to clear context"),
        PolicyFilterEvent::ItemBlocked(item, _) => panic!("unexpected item receipt - blocked: {:?}", item),
        PolicyFilterEvent::ItemPassed(data, _) => panic!("unexpected data passed policy: {:?}", data),
    };
    tracing::warn!("04. environment change verified.");

    let detail = assert_ok!(flow.inspect_policy_context().await);
    tracing::warn!("policy detail = {:?}", detail);
    assert!(detail.context.is_some());

    tracing::warn!("05. Push Item...");

    let t1: Telemetry = maplit::hashmap! {
        "measurement" => std::f64::consts::PI.to_telemetry()
    }
    .into_iter()
    .collect();
    assert_eq!(
        MeasurementData { measurement: std::f64::consts::PI },
        assert_ok!(t1.try_into::<MeasurementData>())
    );

    assert_ok!(
        flow.push_telemetry(maplit::hashmap! {"measurement".to_string() => std::f64::consts::PI.to_telemetry()}.into())
            .await
    );

    tracing::warn!("06. Look for Item in sink...");

    assert!(assert_ok!(
        flow.check_sink_accumulation("first", Duration::from_secs(2), |acc| acc.len() == 1)
            .await
    ));

    tracing::warn!("07. Push another Item...");

    let tau_correlation_id: Id<MeasurementData> = Id::direct("MeasurementData", 821, "TAU");

    assert_ok!(
        flow.push_telemetry(
            maplit::hashmap! {
            "correlation_id".to_string() => tau_correlation_id.clone().into(),
            "measurement".to_string() => std::f64::consts::TAU.to_telemetry()}
            .into()
        )
        .await
    );

    tracing::warn!("08. Close flow...");

    let actual = assert_ok!(flow.close().await);

    tracing::warn!(?actual, "09. Verify final accumulation...");

    assert_eq!(
        actual,
        vec![
            PolicyOutcome::new(
                DataSet::new(MeasurementData { measurement: std::f64::consts::PI }).await,
                DataSet::new(context.clone()).await,
                QueryResult::passed_without_bindings()
            ),
            PolicyOutcome::new(
                DataSet::new(MeasurementData { measurement: std::f64::consts::TAU },).await,
                DataSet::new(context.clone()).await,
                QueryResult::passed_without_bindings()
            ),
        ]
    );
    Ok(())
}

#[test]
fn test_item_serde() {
    Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_item_serde");
    let _ = main_span.enter();

    let item = TestItem::new(std::f64::consts::PI, 3, DT_1.clone());
    let json = serde_json::to_string(&item).unwrap();
    assert_eq!(
        json,
        format!(
            r#"{{"input_messages_per_sec":{},"inbox_lag":3,"my_timestamp":{}}}"#,
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
            "my_timestamp" => DT_1_TS.to_telemetry(),
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
    sources: Vec<PolicySource>,
    data: HashMap<String, String>,
    subscription_extension: HashSet<String>,
}

impl TestPolicyB {
    pub fn new<S: AsRef<str>>(policy: S, data: HashMap<String, String>) -> Self {
        Self::new_with_extension(policy, data, HashSet::<String>::new())
    }

    #[tracing::instrument(
        level = "info",
        name = "TestPolicyB::new_with_extension",
        skip(policy, subscription_extension)
    )]
    pub fn new_with_extension<S0, S1>(
        policy: S0, data: HashMap<String, String>, subscription_extension: HashSet<S1>,
    ) -> Self
    where
        S0: AsRef<str>,
        S1: Into<String>,
    {
        let mut registry = PolicyRegistry::new();
        let source = assert_ok!(PolicySource::from_template_string(
            TestPolicyB::base_template_name(),
            policy
        ));
        let template_name = source.name();
        let policy_template: String = assert_ok!((&source).try_into());
        assert_ok!(registry.register_template_string(template_name.as_ref(), policy_template));
        let rendered_policy = assert_ok!(registry.render(template_name.as_ref(), &data));
        let polar = polar_core::polar::Polar::new();
        assert_ok!(polar.load_str(&rendered_policy));

        let subscription_extension = subscription_extension.into_iter().map(|s| s.into()).collect();
        Self {
            sources: vec![source],
            data,
            subscription_extension,
        }
    }
}

impl PolicySubscription for TestPolicyB {
    type Requirements = DataSet<TestPolicyPhaseContext>;

    fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
        subscription.with_optional_fields(self.subscription_extension.clone())
    }
}

impl QueryPolicy for TestPolicyB {
    type Args = (TestItem, TestPolicyPhaseContext);
    type Context = DataSet<TestPolicyPhaseContext>;
    type Item = DataSet<TestItem>;
    type TemplateData = HashMap<String, String>;

    fn base_template_name() -> &'static str {
        "policy_b"
    }

    fn policy_template_data(&self) -> Option<&Self::TemplateData> {
        Some(&self.data)
    }

    fn policy_template_data_mut(&mut self) -> Option<&mut Self::TemplateData> {
        Some(&mut self.data)
    }

    fn sources(&self) -> &[PolicySource] {
        self.sources.as_slice()
    }

    fn sources_mut(&mut self) -> &mut Vec<PolicySource> {
        &mut self.sources
    }

    fn initialize_policy_engine(&self, oso: &mut Oso) -> Result<(), PolicyError> {
        assert_ok!(oso.register_class(
            TestItem::get_polar_class_builder()
                .name("TestMetricCatalog")
                .add_method("lag_duration", TestItem::lag_duration_secs_f64)
                .add_method("within_seconds", TestItem::within_seconds)
                .build(),
        ));

        assert_ok!(oso.register_class(
            TestPolicyPhaseContext::get_polar_class_builder()
                .name("PhaseContext")
                .add_method("custom", ProctorContext::custom)
                .build(),
        ));

        Ok(())
    }

    fn make_query_args(&self, item: &Self::Item, context: &Self::Context) -> Self::Args {
        (item.as_ref().clone(), context.as_ref().clone())
    }

    fn query_policy(&self, oso: &Oso, args: Self::Args) -> Result<QueryResult, PolicyError> {
        QueryResult::from_query(assert_ok!(oso.query_rule("eligible", args)))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_eligibility_w_pass_and_blocks() -> anyhow::Result<()> {
    Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_eligibility_w_pass_and_blocks");
    let _ = main_span.enter();

    let subscription = TelemetrySubscription::new("measurements").with_required_fields(maplit::hashset! {
        "input_messages_per_sec",
        "inbox_lag",
        "my_timestamp",
    });

    let policy = TestPolicyB::new(
        r##"eligible(_item, environment) if environment.cluster_status.location_code == {{location_code}};"##,
        maplit::hashmap! { "location_code".to_string() => "33".to_string(), },
    );

    let mut flow = TestFlow::new(subscription, policy).await?;

    flow.push_context(maplit::hashmap! {
        "cluster.location_code" => 33.to_telemetry(),
        "cluster.is_deploying" => false.to_telemetry(),
        "cluster.last_deployment" => DT_1_STR.as_str().to_telemetry(),
    })
    .await?;

    let event = &*flow.recv_policy_event().await?;
    claim::assert_matches!(event.as_ref(), &elements::PolicyFilterEvent::ContextChanged(_));
    tracing::warn!(?event, "A: environment changed confirmed");

    let ts = *DT_1 + chrono::Duration::hours(1);
    let item = TestItem::new(std::f64::consts::PI, 1, ts);
    tracing::warn!(?item, "A.1: created item to push.");
    let telemetry = Telemetry::try_from(&item);
    tracing::warn!(?item, ?telemetry, "A.2: converted item to telemetry and pushing...");
    flow.push_telemetry(telemetry?).await?;
    let event = &*flow.recv_policy_event().await?;
    claim::assert_matches!(event.as_ref(), &elements::PolicyFilterEvent::ItemPassed(_, _));

    tracing::info!("waiting for item to reach sink...");
    assert!(
        flow.check_sink_accumulation("first", Duration::from_secs(2), |acc| acc.len() == 1)
            .await?
    );

    tracing::warn!("A.3: pushed telemetry and now pushing context update...");

    flow.push_context(maplit::hashmap! {
        "cluster.location_code" => 19.to_telemetry(),
    })
    .await?;
    let event = &*flow.recv_policy_event().await?;
    claim::assert_matches!(event.as_ref(), &elements::PolicyFilterEvent::ContextChanged(_));
    tracing::warn!(?event, "B: environment changed confirmed");

    let item = TestItem::new(std::f64::consts::E, 2, ts);
    let telemetry = Telemetry::try_from(&item);
    flow.push_telemetry(telemetry?).await?;
    let event = &*flow.recv_policy_event().await?;
    claim::assert_matches!(event.as_ref(), &elements::PolicyFilterEvent::ItemBlocked(_, _));
    tracing::warn!(?event, "C: item dropped confirmed");

    let item = TestItem::new(std::f64::consts::FRAC_1_PI, 3, ts);
    let telemetry = Telemetry::try_from(&item)?;
    flow.push_telemetry(telemetry).await?;
    let event = &*flow.recv_policy_event().await?;
    claim::assert_matches!(event.as_ref(), &elements::PolicyFilterEvent::ItemBlocked(_, _));
    tracing::warn!(?event, "D: item dropped confirmed");

    let item = TestItem::new(std::f64::consts::FRAC_1_SQRT_2, 4, ts);
    let telemetry = Telemetry::try_from(&item)?;
    flow.push_telemetry(telemetry).await?;
    let event = &*flow.recv_policy_event().await?;
    claim::assert_matches!(event.as_ref(), &elements::PolicyFilterEvent::ItemBlocked(_, _));
    tracing::warn!(?event, "E: item dropped confirmed");

    flow.push_context(maplit::hashmap! { "cluster.location_code" => 33.to_telemetry(), })
        .await?;

    let event = &*flow.recv_policy_event().await?;
    tracing::info!(?event, "E.1: policy event received.");
    assert_eq!(
        event.as_ref(),
        &elements::PolicyFilterEvent::ContextChanged(Some(
            DataSet::new(TestPolicyPhaseContext {
                task_status: TestTaskStatus { last_failure: None },
                cluster_status: TestClusterStatus {
                    location_code: 33,
                    is_deploying: false,
                    last_deployment: DT_1.clone(),
                },
                custom: TableValue::new(),
            })
            .await
        ))
    );
    claim::assert_matches!(event.as_ref(), &elements::PolicyFilterEvent::ContextChanged(_));
    tracing::warn!(?event, "F: environment changed confirmed");

    let item = TestItem::new(std::f64::consts::LN_2, 5, ts);
    let telemetry = Telemetry::try_from(&item)?;
    flow.push_telemetry(telemetry).await?;

    tracing::info!("waiting for item to reach sink...");
    assert!(
        flow.check_sink_accumulation("second", Duration::from_secs(2), |acc| acc.len() == 2)
            .await?
    );

    let actual: Vec<PolicyOutcome<DataSet<TestItem>, DataSet<TestPolicyPhaseContext>>> = flow.close().await?;

    tracing::warn!(?actual, "08. Verify final accumulation...");
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
    Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_eligibility_w_custom_fields");
    let _ = main_span.enter();

    let subscription = TelemetrySubscription::new("measurements").with_required_fields(maplit::hashset! {
        "input_messages_per_sec",
        "inbox_lag",
        "my_timestamp",
    });

    let policy = TestPolicyB::new_with_extension(
        r##"
                |eligible(_item, environment) if
                |   c = environment.custom() and
                |   c.cat = "{{good_cat}}";
            "##,
        maplit::hashmap! { "good_cat".to_string() => "Otis".to_string(), },
        maplit::hashset! {"cat"},
    );

    let mut flow = TestFlow::new(subscription, policy).await?;

    flow.push_context(maplit::hashmap! {
        "cluster.location_code" => 33.to_telemetry(),
        "cluster.is_deploying" => false.to_telemetry(),
        "cluster.last_deployment" => DT_1_STR.as_str().to_telemetry(),
        "cat" => "Otis".to_telemetry(),
    })
    .await?;

    let event = &*flow.recv_policy_event().await?;
    tracing::info!(?event, "verifying environment update...");
    claim::assert_matches!(event.as_ref(), &elements::PolicyFilterEvent::ContextChanged(_));

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
    Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_eligibility_w_item_n_env");
    let _ = main_span.enter();

    tracing::info!("A:create flow...");

    let subscription = TelemetrySubscription::new("data").with_required_fields(maplit::hashset! {
        "input_messages_per_sec",
        "inbox_lag",
        "my_timestamp",
    });

    let policy = TestPolicyB::new_with_extension(
        r##"
        |eligible(item, env) if proper_cat(item, env) and lag_2(item, env);
        |   proper_cat(_, env) if env.custom.cat = "{{good_cat}}";
        |   lag_2(_item: TestMetricCatalog{ inbox_lag: {{lag}} }, _);
        "##,
        maplit::hashmap! {
            "good_cat".to_string() => "Otis".to_string(),
            "lag".to_string() => 2.to_string(),
        },
        maplit::hashset! { "cat" },
    );

    // another form for policy that works
    // r#"
    //     eligible(item, env) if proper_cat(item, env) and lag_2(item, env);
    //     proper_cat(_, env) if env.custom.cat = "Otis";
    //     lag_2(item, _) if item.inbox_lag = 2;
    // "#,
    let flow = TestFlow::new(subscription, policy).await?;

    tracing::info!("B:push env...");
    flow.push_context(maplit::hashmap! {
        "cluster.location_code" => 23.to_telemetry(),
        "cluster.is_deploying" => false.to_telemetry(),
        "cluster.last_deployment" => DT_1_STR.as_str().to_telemetry(),
        "cat" => "Otis".to_telemetry(),
    })
    .await?;
    tracing::info!("C:verify context...");

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
    Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_eligibility_replace_policy");
    let _ = main_span.enter();

    let boundary_age_secs = 60 * 60;
    let good_ts = chrono::NaiveDateTime::from_timestamp(*DT_1_TS, 0);
    let good_ts = chrono::DateTime::from_utc(good_ts, chrono::Utc);
    let too_old_ts = good_ts.clone() - chrono::Duration::seconds(boundary_age_secs + 5);

    let data_subscription = TelemetrySubscription::new("data").with_required_fields(maplit::hashset! {
        "input_messages_per_sec",
        "inbox_lag",
        "my_timestamp",
    });

    let policy_1 = r##"eligible(_, env: PhaseContext) if env.custom.cat = "{{kitty}}";"##;
    let policy_2 = r##"eligible(metrics, _) if metrics.within_seconds({{boundary}});"##;
    tracing::info!(?policy_2, "policy with timestamp");

    let policy = TestPolicyB::new_with_extension(
        policy_1,
        maplit::hashmap! {
            "kitty".to_string() => "Otis".to_string(),
            "boundary".to_string() => boundary_age_secs.to_string(),
        },
        maplit::hashset! {"cat"},
    );

    let mut flow = TestFlow::new(data_subscription, policy).await?;

    let context_data = maplit::hashmap! {
        "cluster.location_code" => 23.to_telemetry(),
        "cluster.is_deploying" => false.to_telemetry(),
        "cluster.last_deployment" => DT_1_STR.as_str().to_telemetry(),
        "cat" => "Otis".to_telemetry(),
    };
    let mut context_telemetry: Telemetry = context_data.clone().into_iter().collect();
    context_telemetry.insert(SUBSCRIPTION_TIMESTAMP.to_string(), Timestamp::new(0, 0).into());
    context_telemetry.insert(
        SUBSCRIPTION_CORRELATION.to_string(),
        Id::<Telemetry>::direct("EligibilityReplace", 0, "".to_string()).into(),
    );
    let context: TestPolicyPhaseContext = context_telemetry.try_into()?;
    flow.push_context(context_data).await?;

    let event = &*flow.recv_policy_event().await?;
    claim::assert_matches!(event.as_ref(), &elements::PolicyFilterEvent::ContextChanged(_));
    tracing::warn!(?event, "A: environment changed confirmed");

    let item_1 = TestItem::new(std::f64::consts::PI, 1, too_old_ts);
    tracing::warn!(?item_1, "A-1: item passes...");
    let telemetry_1 = Telemetry::try_from(&item_1)?;
    flow.push_telemetry(telemetry_1.clone()).await?;

    let item_2 = TestItem::new(17.327, 2, good_ts);
    tracing::warn!(?item_2, "A-2: item passes...");
    let telemetry_2 = Telemetry::try_from(&item_2)?;
    flow.push_telemetry(telemetry_2.clone()).await?;

    tracing::warn!("replace policy and re-send");
    elements::PolicyFilterCmd::replace_policies(
        &flow.tx_eligibility_api,
        vec![elements::PolicySource::from_template_string(
            TestPolicyB::base_template_name(),
            policy_2.to_string(),
        )?],
        None,
    )
    .await?;

    tracing::warn!("B: after policy change, pushing telemetry data...");
    tracing::warn!(?telemetry_1, "B-1: item blocked...");
    flow.push_telemetry(telemetry_1).await?;
    tracing::warn!(?telemetry_2, "B-2: item passes...");
    flow.push_telemetry(telemetry_2).await?;

    let actual = flow.close().await?;
    tracing::warn!(?actual, "verifying actual result...");

    assert_eq!(
        actual,
        vec![
            PolicyOutcome::new(
                DataSet::new(TestItem::new(std::f64::consts::PI, 1, too_old_ts)).await,
                DataSet::new(TestPolicyPhaseContext { ..context.clone() }).await,
                QueryResult::passed_without_bindings()
            ),
            PolicyOutcome::new(
                DataSet::new(TestItem::new(17.327, 2, good_ts)).await,
                DataSet::new(TestPolicyPhaseContext { ..context.clone() }).await,
                QueryResult::passed_without_bindings()
            ),
            PolicyOutcome::new(
                DataSet::new(TestItem::new(17.327, 2, good_ts)).await,
                DataSet::new(TestPolicyPhaseContext { ..context.clone() }).await,
                QueryResult::passed_without_bindings()
            ),
        ]
    );
    Ok(())
}
