use std::collections::{BTreeMap, HashSet};
use std::f64::consts;

use ::serde::{Deserialize, Serialize};
use async_trait::async_trait;
use chrono::*;
use claim::*;
use oso::{Oso, PolarClass, PolarValue};
use pretty_assertions::assert_eq;
use proctor::elements::telemetry::ToTelemetry;
use proctor::elements::{
    self, telemetry, PolicyOutcome, PolicySettings, PolicySubscription, QueryPolicy, QueryResult, TelemetryValue,
};
use proctor::elements::{PolicyRegistry, PolicySource};
use proctor::error::PolicyError;
use proctor::graph::stage::{self, WithApi, WithMonitor};
use proctor::graph::{Connect, Graph, SinkShape, SourceShape};
use proctor::phases::collection::SubscriptionRequirements;
use proctor::{ProctorContext, SharedString};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use trim_margin::MarginTrimmable;

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
struct TestContext {
    #[polar(attribute)]
    pub location_code: u32,
    custom: telemetry::TableValue,
}

impl TestContext {
    pub fn new(location_code: u32) -> Self {
        Self {
            location_code,
            custom: telemetry::TableValue::default(),
        }
    }

    pub fn with_custom(self, custom: telemetry::TableValue) -> Self {
        Self { custom, ..self }
    }
}

#[async_trait]
impl proctor::ProctorContext for TestContext {
    type Error = PolicyError;

    fn custom(&self) -> telemetry::TableType {
        (&*self.custom).clone()
    }
}

impl SubscriptionRequirements for TestContext {
    fn required_fields() -> HashSet<SharedString> {
        maplit::hashset! { "location_code".into(), "input_messages_per_sec".into(), }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct PolicyTemplateData {
    pub location_code: u32,
    #[serde(default)]
    pub lag: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cat: Option<String>,
    #[serde(flatten, skip_serializing_if = "BTreeMap::is_empty")]
    pub custom: BTreeMap<String, String>,
}

#[derive(Debug)]
struct TestPolicy {
    sources: Vec<PolicySource>,
    template_data: PolicyTemplateData,
    query: String,
}

impl TestPolicy {
    #[tracing::instrument(
        level="info",
        name="TestPolicy::with_query",
        skip(policy, query),
        fields(policy=%policy.as_ref())
    )]
    pub fn with_query(policy: impl AsRef<str>, template_data: PolicyTemplateData, query: impl Into<String>) -> Self {
        let polar = polar_core::polar::Polar::new();
        let mut registry = PolicyRegistry::new();
        let source = assert_ok!(PolicySource::from_template_string(
            TestPolicy::base_template_name(),
            policy
        ));
        let template_name = source.name();
        tracing::info!(%template_name, ?source, ?template_data, "creating test policy");
        let policy_template: String = assert_ok!((&source).try_into());
        tracing::info!(%policy_template, "made template from source");
        assert_ok!(registry.register_template_string(template_name.as_ref(), policy_template));
        assert!(registry.has_template(template_name.as_ref()));
        let rendered_policy = assert_ok!(registry.render(template_name.as_ref(), &template_data));
        tracing::info!(%rendered_policy, "rendered policy");
        assert_ok!(polar.load_str(&rendered_policy));
        Self {
            sources: vec![source],
            template_data,
            query: query.into(),
        }
    }
}

impl PolicySubscription for TestPolicy {
    type Requirements = TestContext;

    // todo test optional fields
    // fn subscription_fields(&self) -> HashSet<String> {
    //     Self::Context::subscription_fields_nucleus()
    // }
}

impl QueryPolicy for TestPolicy {
    type Args = (TestItem, TestContext, PolarValue);
    type Context = TestContext;
    type Item = TestItem;
    type TemplateData = PolicyTemplateData;

    fn base_template_name() -> &'static str {
        "test_policy"
    }

    fn policy_template_data(&self) -> Option<&Self::TemplateData> {
        Some(&self.template_data)
    }

    fn policy_template_data_mut(&mut self) -> Option<&mut Self::TemplateData> {
        Some(&mut self.template_data)
    }

    fn sources(&self) -> &[PolicySource] {
        self.sources.as_slice()
    }

    fn sources_mut(&mut self) -> &mut Vec<PolicySource> {
        &mut self.sources
    }

    fn initialize_policy_engine(&mut self, oso: &mut Oso) -> Result<(), PolicyError> {
        oso.register_class(
            TestItem::get_polar_class_builder()
                .name("TestMetricCatalog")
                .add_method("input_messages_per_sec", TestItem::input_messages_per_sec)
                .add_method("within_seconds", TestItem::within_seconds)
                .build(),
        )?;

        oso.register_class(
            TestContext::get_polar_class_builder()
                .name("TestContext")
                .add_method("custom", ProctorContext::custom)
                // .add_method("custom", |env: &TestContext| env.custom() )
                .build(),
        )?;

        Ok(())
    }

    fn make_query_args(&self, item: &Self::Item, context: &Self::Context) -> Self::Args {
        (
            item.clone(),
            context.clone(),
            PolarValue::Variable("custom".to_string()),
        )
    }

    fn query_policy(&self, engine: &Oso, args: Self::Args) -> Result<QueryResult, PolicyError> {
        let q = engine.query_rule(self.query.as_str(), args)?;
        let result = QueryResult::from_query(q)?;
        tracing::info!(?result, "DMR: query policy results!");
        Ok(result)
    }
}

struct TestFlow {
    pub graph_handle: JoinHandle<()>,
    pub tx_item_source_api: stage::ActorSourceApi<TestItem>,
    pub tx_env_source_api: stage::ActorSourceApi<TestContext>,
    pub tx_policy_api: elements::PolicyFilterApi<TestContext, PolicyTemplateData>,
    pub rx_policy_monitor: elements::PolicyFilterMonitor<TestItem, TestContext>,
    pub tx_sink_api: stage::FoldApi<Vec<PolicyOutcome<TestItem, TestContext>>>,
    pub rx_sink: Option<oneshot::Receiver<Vec<PolicyOutcome<TestItem, TestContext>>>>,
}

impl TestFlow {
    pub async fn new(policy: impl AsRef<str>, data: PolicyTemplateData) -> Result<Self, PolicyError> {
        Self::with_query(policy, data, "eligible").await
    }

    pub async fn with_query(
        policy: impl AsRef<str>, data: PolicyTemplateData, query: impl Into<String>,
    ) -> Result<Self, PolicyError> {
        let item_source = stage::ActorSource::<TestItem>::new("item_source");
        let tx_item_source_api = item_source.tx_api();

        let env_source = stage::ActorSource::<TestContext>::new("env_source");
        let tx_env_source_api = env_source.tx_api();

        let policy = TestPolicy::with_query(policy, data, query);
        tracing::info!(?policy, "created policy");
        let policy_filter = elements::PolicyFilter::new("eligibility", policy)?;
        tracing::info!(?policy_filter, "created policy filter");
        let tx_policy_api = policy_filter.tx_api();
        let rx_policy_monitor = policy_filter.rx_monitor();

        let mut sink =
            stage::Fold::<_, PolicyOutcome<TestItem, TestContext>, _>::new("sink", Vec::new(), |mut acc, item| {
                acc.push(item);
                acc
            });
        let tx_sink_api = sink.tx_api();
        let rx_sink = sink.take_final_rx();

        (item_source.outlet(), policy_filter.inlet()).connect().await;
        (env_source.outlet(), policy_filter.context_inlet()).connect().await;
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

        Ok(Self {
            graph_handle,
            tx_item_source_api,
            tx_env_source_api,
            tx_policy_api,
            rx_policy_monitor,
            tx_sink_api,
            rx_sink,
        })
    }

    pub async fn push_item(&self, item: TestItem) -> anyhow::Result<()> {
        let (cmd, ack) = stage::ActorSourceCmd::push(item);
        self.tx_item_source_api.send(cmd)?;
        ack.await.map_err(|err| err.into())
    }

    pub async fn push_context(&self, env: TestContext) -> anyhow::Result<()> {
        let (cmd, ack) = stage::ActorSourceCmd::push(env);
        self.tx_env_source_api.send(cmd)?;
        ack.await.map_err(|err| err.into())
    }

    pub async fn tell_policy(
        &self,
        command_rx: (
            elements::PolicyFilterCmd<TestContext, PolicyTemplateData>,
            oneshot::Receiver<proctor::Ack>,
        ),
    ) -> anyhow::Result<proctor::Ack> {
        self.tx_policy_api.send(command_rx.0)?;
        command_rx.1.await.map_err(|err| err.into())
    }

    pub async fn recv_policy_event(&mut self) -> anyhow::Result<elements::PolicyFilterEvent<TestItem, TestContext>> {
        self.rx_policy_monitor.recv().await.map_err(|err| err.into())
    }

    pub async fn inspect_filter_context(
        &self,
    ) -> anyhow::Result<elements::PolicyFilterDetail<TestContext, PolicyTemplateData>> {
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

    pub async fn inspect_sink(&self) -> anyhow::Result<Vec<PolicyOutcome<TestItem, TestContext>>> {
        let (cmd, acc) = stage::FoldCmd::get_accumulation();
        self.tx_sink_api.send(cmd)?;
        acc.await
            .map(|a| {
                tracing::info!(accumulation=?a, "inspected sink accumulation");
                a
            })
            .map_err(|err| err.into())
    }

    pub async fn close(mut self) -> anyhow::Result<Vec<PolicyOutcome<TestItem, TestContext>>> {
        let (stop, _) = stage::ActorSourceCmd::stop();
        self.tx_item_source_api.send(stop)?;

        let (stop, _) = stage::ActorSourceCmd::stop();
        self.tx_env_source_api.send(stop)?;

        self.graph_handle.await?;

        self.rx_sink.take().unwrap().await.map_err(|err| err.into())
    }
}

#[test]
fn test_policy_serde_and_render() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct TemplateData {
        pub basis: String,
        pub health_lag: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub max_healthy_cpu_load: Option<f64>,
        #[serde(flatten, skip_serializing_if = "BTreeMap::is_empty")]
        pub custom: BTreeMap<String, String>,
    }

    impl Default for TemplateData {
        fn default() -> Self {
            Self {
                basis: "policy_basis".to_string(),
                health_lag: 30_000,
                max_healthy_cpu_load: None,
                custom: BTreeMap::default(),
            }
        }
    }

    let settings = PolicySettings {
        required_subscription_fields: maplit::hashset! { "foo".to_string() },
        optional_subscription_fields: HashSet::default(),
        policies: vec![assert_ok!(PolicySource::from_template_string(
            "policy_basis",
            r##"eligible(_item, context, _) if context.location_code == {{location_code}};"##
        ))],
        template_data: Some(TemplateData {
            health_lag: 23_333,
            custom: maplit::btreemap! {"location_code".to_string() => 33.to_string(),},
            ..TemplateData::default()
        }),
    };

    let settings_rep = assert_ok!(ron::ser::to_string_pretty(&settings, ron::ser::PrettyConfig::default()));
    assert_eq!(
        settings_rep,
        r##"|(
        |    required_subscription_fields: [
        |        "foo",
        |    ],
        |    policies: [
        |        (
        |            source: "string",
        |            policy: (
        |                name: "policy_basis",
        |                polar: "eligible(_item, context, _) if context.location_code == {{location_code}};",
        |                is_template: true,
        |            ),
        |        ),
        |    ],
        |    template_data: {
        |        "basis": "policy_basis",
        |        "health_lag": 23333,
        |        "location_code": "33",
        |    },
        |)"##
            .trim_margin_with("|")
            .unwrap()
    );

    let json_rep = r##"|{
    |  "required_subscription_fields": [
    |    "foo"
    |  ],
    |  "policies": [
    |    {
    |      "source": "string",
    |      "policy": {
    |        "name": "policy_basis",
    |        "polar": "eligible(_item, context, _) if context.location_code == {{location_code}};",
    |        "is_template": true
    |      }
    |    }
    |  ],
    |  "template_data": {
    |    "basis": "policy_basis",
    |    "health_lag": 23333,
    |    "location_code": "33"
    |  }
    |}"##
        .trim_margin_with("|")
        .unwrap();

    let hydrated: PolicySettings<TemplateData> = assert_ok!(serde_json::from_str(&json_rep));
    assert_eq!(hydrated, settings);

    let mut registry = PolicyRegistry::new();
    let policy = hydrated.policies.first().unwrap();
    let policy_name = policy.name();
    let template: String = assert_ok!(policy.try_into());
    let actual = assert_ok!(registry.render_template(&template, settings.template_data.as_ref().unwrap()));
    assert_eq!(actual, "eligible(_item, context, _) if context.location_code == 33;");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_policy_filter_before_context_baseline() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_policy_filter_before_context_baseline");
    let _ = main_span.enter();

    let flow = TestFlow::new(
        r##"eligible(_item, context) if context.location_code == {{location_code}};"##,
        PolicyTemplateData {
            location_code: 33,
            lag: None,
            cat: None,
            ..PolicyTemplateData::default()
        },
    )
    .await?;
    let item = TestItem {
        flow: TestFlowMetrics { input_messages_per_sec: 3.1415926535 },
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
async fn test_policy_filter_happy_context() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_policy_filter_happy_context");
    let _ = main_span.enter();

    let flow = TestFlow::new(
        r##"eligible(_item, context, _) if context.location_code == {{location_code}};"##,
        PolicyTemplateData { location_code: 33, ..PolicyTemplateData::default() },
    )
    .await?;

    tracing::info!("DMR: 01. Make sure empty env...");

    let detail = flow.inspect_filter_context().await?;
    assert_none!(detail.context);

    tracing::info!("DMR: 02. Push context...");

    flow.push_context(TestContext::new(33)).await?;

    tracing::info!("DMR: 03. Verify context set...");

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let detail = flow.inspect_filter_context().await?;
    assert_some!(detail.context);

    tracing::info!("DMR: 04. Push Item...");

    let ts = Utc::now().into();
    let item = TestItem::new(consts::PI, ts, 1);
    flow.push_item(item).await?;

    tracing::info!("DMR: 05. Look for Item in sink...");

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let actual = flow.inspect_sink().await?;
    assert_eq!(
        actual,
        vec![PolicyOutcome::new(
            TestItem::new(consts::PI, ts, 1),
            TestContext::new(33),
            QueryResult::passed_without_bindings()
        ),]
    );

    tracing::info!("DMR: 06. Push another Item...");

    let item = TestItem::new(consts::TAU, ts, 2);
    flow.push_item(item).await?;

    tracing::info!("DMR: 07. Close flow...");

    let actual = flow.close().await?;

    tracing::info!(?actual, "DMR: 08. Verify final accumulation...");

    assert_eq!(
        actual,
        vec![
            PolicyOutcome::new(
                TestItem::new(consts::PI, ts, 1),
                TestContext::new(33),
                QueryResult::passed_without_bindings()
            ),
            PolicyOutcome::new(
                TestItem::new(consts::TAU, ts, 2),
                TestContext::new(33),
                QueryResult::passed_without_bindings()
            )
        ]
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_policy_filter_w_pass_and_blocks() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_policy_filter_w_pass_and_blocks");
    let _ = main_span.enter();

    let mut flow = TestFlow::new(
        r##"eligible(_item, context, _) if context.location_code == {{location_code}};"##,
        PolicyTemplateData { location_code: 33, ..PolicyTemplateData::default() },
    )
    .await?;
    flow.push_context(TestContext::new(33)).await?;
    let event = flow.recv_policy_event().await?;
    claim::assert_matches!(event, elements::PolicyFilterEvent::ContextChanged(_));
    tracing::info!(?event, "DMR-A: context changed confirmed");

    let ts = Utc::now().into();
    let item = TestItem::new(consts::PI, ts, 1);
    flow.push_item(item).await?;
    let event = flow.recv_policy_event().await?;
    claim::assert_matches!(event, elements::PolicyFilterEvent::ItemPassed(_));

    flow.push_context(TestContext::new(19)).await?;
    let event = flow.recv_policy_event().await?;
    claim::assert_matches!(event, elements::PolicyFilterEvent::ContextChanged(_));
    tracing::info!(?event, "DMR-B: context changed confirmed");

    let item = TestItem::new(consts::E, ts, 2);
    flow.push_item(item).await?;
    let event = flow.recv_policy_event().await?;
    claim::assert_matches!(event, elements::PolicyFilterEvent::ItemBlocked(_));
    tracing::info!(?event, "DMR-C: item dropped confirmed");

    let item = TestItem::new(consts::FRAC_1_PI, ts, 3);
    flow.push_item(item).await?;
    let event = flow.recv_policy_event().await?;
    claim::assert_matches!(event, elements::PolicyFilterEvent::ItemBlocked(_));
    tracing::info!(?event, "DMR-D: item dropped confirmed");

    let item = TestItem::new(consts::FRAC_1_SQRT_2, ts, 4);
    flow.push_item(item).await?;
    let event = flow.recv_policy_event().await?;
    claim::assert_matches!(event, elements::PolicyFilterEvent::ItemBlocked(_));
    tracing::info!(?event, "DMR-E: item dropped confirmed");

    flow.push_context(TestContext::new(33)).await?;
    let event = flow.recv_policy_event().await?;
    claim::assert_matches!(event, elements::PolicyFilterEvent::ContextChanged(_));
    tracing::info!(?event, "DMR-F: context changed confirmed");

    let item = TestItem::new(consts::LN_2, ts, 5);
    flow.push_item(item).await?;
    let event = flow.recv_policy_event().await?;
    claim::assert_matches!(event, elements::PolicyFilterEvent::ItemPassed(_));

    let actual = flow.close().await?;

    tracing::info!(?actual, "DMR: 08. Verify final accumulation...");
    assert_eq!(
        actual,
        vec![
            PolicyOutcome::new(
                TestItem::new(consts::PI, ts, 1),
                TestContext::new(33),
                QueryResult::passed_without_bindings()
            ),
            PolicyOutcome::new(
                TestItem::new(consts::LN_2, ts, 5),
                TestContext::new(33),
                QueryResult::passed_without_bindings()
            )
        ]
    );
    Ok(())
}

// #[tokio::test(flavor="multi_thread", worker_threads = 4)]
#[tokio::test]
async fn test_policy_w_custom_fields() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_policy_w_custom_fields");
    let _ = main_span.enter();

    let mut flow = TestFlow::new(
        r##"
        | eligible(_item, context, c) if
        |   c = context.custom() and
        |   c.cat = "{{cat}}";
        "##,
        PolicyTemplateData {
            location_code: 33_333,
            lag: Some(10),
            cat: Some("Otis".to_string()),
            ..PolicyTemplateData::default()
        },
    )
    .await?;

    flow.push_context(
        TestContext::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_telemetry()}.into()),
    )
    .await?;
    let event = flow.recv_policy_event().await?;
    tracing::info!(?event, "verifying context update...");
    claim::assert_matches!(event, elements::PolicyFilterEvent::ContextChanged(_));

    let ts = Utc::now().into();
    let item = TestItem::new(consts::PI, ts, 1);
    flow.push_item(item).await?;

    let item = TestItem::new(consts::TAU, ts, 2);
    flow.push_item(item).await?;

    let actual = flow.close().await?;
    tracing::info!(?actual, "verifying actual result...");
    assert_eq!(
        actual,
        vec![
            PolicyOutcome::new(
                TestItem::new(consts::PI, ts, 1),
                TestContext::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_telemetry()}.into()),
                QueryResult {
                    passed: true,
                    bindings: maplit::hashmap! {
                        "custom".to_string() => vec![TelemetryValue::Table(maplit::hashmap! {
                            "cat".to_string() => "Otis".to_telemetry(),
                        }.into())],
                    }
                    .into()
                }
            ),
            PolicyOutcome::new(
                TestItem::new(consts::TAU, ts, 2),
                TestContext::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_telemetry()}.into()),
                QueryResult {
                    passed: true,
                    bindings: maplit::hashmap! {
                        "custom".to_string() => vec![TelemetryValue::Table(maplit::hashmap! {
                            "cat".to_string() => "Otis".to_telemetry(),
                        }.into())],
                    }
                    .into()
                }
            )
        ],
    );
    Ok(())
}

#[tokio::test]
async fn test_policy_w_binding() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_policy_w_custom_fields");
    let _ = main_span.enter();

    let mut flow = TestFlow::with_query(
        r##"
        | eligible(_, _context, length) if length = {{lag}};
        |
        | eligible(_item, context, c) if
        |   c = context.custom() and
        |   c.cat = "{{cat}}" and
        |   cut;
        "##,
        PolicyTemplateData {
            location_code: 1_000,
            lag: Some(13),
            cat: Some("Otis".to_string()),
            ..PolicyTemplateData::default()
        },
        "eligible",
    )
    .await?;

    flow.push_context(
        TestContext::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_telemetry()}.into()),
    )
    .await?;
    let event = flow.recv_policy_event().await?;
    tracing::info!(?event, "verifying context update...");
    claim::assert_matches!(event, elements::PolicyFilterEvent::ContextChanged(_));

    let ts = Utc::now().into();
    let item = TestItem::new(consts::PI, ts, 1);
    flow.push_item(item).await?;

    let item = TestItem::new(consts::TAU, ts, 2);
    flow.push_item(item).await?;

    let actual = flow.close().await?;
    tracing::info!(?actual, "verifying actual result...");
    assert_eq!(
        actual,
        vec![
            PolicyOutcome::new(
                TestItem::new(consts::PI, ts, 1),
                TestContext::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_telemetry()}.into()),
                QueryResult {
                    passed: true,
                    bindings: maplit::hashmap! {
                        "custom".to_string() => vec![
                            TelemetryValue::Integer(13),
                            TelemetryValue::Table(maplit::hashmap! { "cat".to_string() => "Otis".to_telemetry(), }.into())
                        ],
                    }
                }
            ),
            PolicyOutcome::new(
                TestItem::new(consts::TAU, ts, 2),
                TestContext::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_telemetry()}.into()),
                QueryResult {
                    passed: true,
                    bindings: maplit::hashmap! {
                        "custom".to_string() => vec![
                            TelemetryValue::Integer(13),
                            TelemetryValue::Table(maplit::hashmap! { "cat".to_string() => "Otis".to_telemetry(), }.into())
                        ],
                    }
                }
            ),
        ],
    );
    Ok(())
}

#[tokio::test]
async fn test_policy_w_item_n_env() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_policy_w_item_n_env");
    let _ = main_span.enter();

    tracing::info!("DMR-A:create flow...");
    // another form for policy that works
    // r#"
    //     eligible(item, env) if proper_cat(item, env) and lag_2(item, env);
    //     proper_cat(_, env) if env.custom.cat = "Otis";
    //     lag_2(item, _) if item.inbox_lag = 2;
    // "#,
    let flow = TestFlow::new(
        r##"
        | eligible(item, env, c) if proper_cat(item, env, c) and lag_2(item, env);
        |   proper_cat(_, env, c) if
        |   c = env.custom() and
        |   c.cat = "{{cat}}";
        |
        | lag_2(_item: TestMetricCatalog{ inbox_lag: {{lag}} }, _);
        "##,
        PolicyTemplateData {
            location_code: 33,
            lag: Some(2),
            cat: Some("Otis".to_string()),
            ..PolicyTemplateData::default()
        },
    )
    .await?;

    tracing::info!("DMR-B:push env...");
    flow.push_context(
        TestContext::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_telemetry()}.into()),
    )
    .await?;
    tracing::info!("DMR-C:verify enviornment...");

    let ts = Utc::now().into();
    let item = TestItem::new(consts::PI, ts, 1);
    flow.push_item(item).await?;

    let item = TestItem::new(consts::TAU, ts, 2);
    flow.push_item(item).await?;

    let actual = flow.close().await?;
    tracing::info!(?actual, "verifying actual result...");
    assert_eq!(
        actual,
        vec![PolicyOutcome::new(
            TestItem::new(consts::TAU, ts, 2),
            TestContext::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_telemetry()}.into()),
            QueryResult {
                passed: true,
                bindings: maplit::hashmap! { "custom".to_string() => vec![TelemetryValue::Table(maplit::hashmap! {
                    "cat".to_string() => "Otis".to_telemetry(),
                }.into())]}
            }
        ),]
    );
    Ok(())
}

#[tokio::test]
async fn test_policy_w_method() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_policy_w_method");
    let _ = main_span.enter();

    let flow = TestFlow::new(
        r##"
        | eligible(item, _env, _) if
        |   34 < item.input_messages_per_sec(item.inbox_lag)
        |   and item.input_messages_per_sec(item.inbox_lag) < {{lag}};
        "##,
        PolicyTemplateData {
            location_code: 33,
            lag: Some(36),
            ..PolicyTemplateData::default()
        },
    )
    .await?;

    flow.push_context(
        TestContext::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_telemetry()}.into()),
    )
    .await?;

    let ts = Utc::now().into();
    let item = TestItem::new(consts::PI, ts, 1);
    flow.push_item(item).await?;

    let item = TestItem::new(17.327, ts, 2);
    flow.push_item(item).await?;

    let actual = flow.close().await?;
    tracing::info!(?actual, "verifying actual result...");
    assert_eq!(
        actual,
        vec![PolicyOutcome::new(
            TestItem::new(17.327, ts, 2),
            TestContext::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_telemetry()}.into()),
            QueryResult::passed_without_bindings()
        )]
    );
    Ok(())
}

#[tokio::test]
async fn test_replace_policy() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_replace_policy");
    let _ = main_span.enter();

    let boundary_age_secs = 60 * 60;
    let good_ts = Utc::now();
    let too_old_ts = Utc::now() - chrono::Duration::seconds(boundary_age_secs + 5);

    let policy_1 = r##"eligible(_, env: TestContext, c) if c = env.custom() and c.cat = "{{cat}}";"##;
    let policy_2 = format!("eligible(item, _, _) if item.within_seconds({});", boundary_age_secs);
    tracing::info!(?policy_2, "DMR: policy with timestamp");

    let flow = TestFlow::new(
        policy_1,
        PolicyTemplateData {
            location_code: 33,
            cat: Some("Otis".to_string()),
            ..PolicyTemplateData::default()
        },
    )
    .await?;

    flow.push_context(
        TestContext::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_telemetry()}.into()),
    )
    .await?;

    let item_1 = TestItem::new(consts::PI, too_old_ts, 1);
    flow.push_item(item_1.clone()).await?;

    let item_2 = TestItem::new(17.327, good_ts, 2);
    flow.push_item(item_2.clone()).await?;

    tracing::info!("replace policy and re-send");
    let cmd_rx = elements::PolicyFilterCmd::replace_policies(
        Some(elements::PolicySource::from_template_string(
            TestPolicy::base_template_name(),
            policy_2.to_string(),
        )?),
        None,
    );
    flow.tell_policy(cmd_rx).await?;

    flow.push_item(item_1).await?;
    flow.push_item(item_2).await?;

    let actual = flow.close().await?;
    tracing::info!(?actual, "verifying actual result...");
    assert_eq!(
        actual,
        vec![
            PolicyOutcome::new(
                TestItem::new(consts::PI, too_old_ts, 1),
                TestContext::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_telemetry()}.into()),
                QueryResult {
                    passed: true,
                    bindings: maplit::hashmap! {"custom".to_string() => vec![TelemetryValue::Table(maplit::hashmap! {
                        "cat".to_string() => "Otis".to_telemetry(),
                    }.into())]}
                    .into()
                }
            ),
            PolicyOutcome::new(
                TestItem::new(17.327, good_ts, 2),
                TestContext::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_telemetry()}.into()),
                QueryResult {
                    passed: true,
                    bindings: maplit::hashmap! {"custom".to_string() => vec![TelemetryValue::Table(maplit::hashmap! {
                        "cat".to_string() => "Otis".to_telemetry(),
                    }.into())]}
                    .into()
                }
            ),
            PolicyOutcome::new(
                TestItem::new(17.327, good_ts, 2),
                TestContext::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_telemetry()}.into()),
                QueryResult::passed_without_bindings()
            ),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn test_append_policy() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_append_policy");
    let _ = main_span.enter();

    let policy_1 = r##"eligible(_item: TestMetricCatalog{ inbox_lag: {{lag}} }, _, _);"##;
    let policy_2 = r##"eligible(_, env: TestContext, c) if c = env.custom() and c.cat = "{{cat}}";"##;

    let flow = TestFlow::new(
        policy_1,
        PolicyTemplateData {
            location_code: 33,
            lag: Some(2),
            cat: Some("Otis".to_string()),
            ..PolicyTemplateData::default()
        },
    )
    .await?;

    flow.push_context(
        TestContext::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_telemetry()}.into()),
    )
    .await?;

    let ts = Utc::now().into();
    let item = TestItem::new(consts::PI, ts, 1);
    flow.push_item(item).await?;

    let item = TestItem::new(17.327, ts, 2);
    flow.push_item(item).await?;

    tracing::info!("add to policy and re-send");
    let cmd_rx = elements::PolicyFilterCmd::append_policy(
        elements::PolicySource::from_template_string(TestPolicy::base_template_name(), policy_2.to_string())?,
        None,
    );
    flow.tell_policy(cmd_rx).await?;

    let item = TestItem::new(consts::SQRT_2, ts, 1);
    flow.push_item(item).await?;

    let item = TestItem::new(34.18723, ts, 2);
    flow.push_item(item).await?;

    let actual = flow.close().await?;
    tracing::info!(?actual, "verifying actual result...");
    assert_eq!(
        actual,
        vec![
            PolicyOutcome::new(
                TestItem::new(17.327, ts, 2),
                TestContext::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_telemetry()}.into()),
                QueryResult::passed_without_bindings()
            ),
            PolicyOutcome::new(
                TestItem::new(consts::SQRT_2, ts, 1),
                TestContext::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_telemetry()}.into()),
                QueryResult {
                    passed: true,
                    bindings: maplit::hashmap! {
                        "custom".to_string() => vec![
                            TelemetryValue::Table(maplit::hashmap!{ "cat".to_string() => "Otis".to_telemetry(), }.into())
                        ]
                    }
                }
            ),
            PolicyOutcome::new(
                TestItem::new(34.18723, ts, 2),
                TestContext::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_telemetry()}.into()),
                QueryResult {
                    passed: true,
                    bindings: maplit::hashmap! {
                        "custom".to_string() => vec![
                            TelemetryValue::Table(maplit::hashmap!{ "cat".to_string() => "Otis".to_telemetry(), }.into())
                        ]
                    }.into()
                }
            ),
        ]
    );
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_reset_policy() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_reset_policy");
    let _ = main_span.enter();

    let policy_1 = r##"eligible(_item: TestMetricCatalog{ inbox_lag: {{lag}} }, _, _);"##;
    let policy_2 = r##"eligible(_, env, c) if c = env.custom() and c.cat = "{{cat}}";"##;

    let flow = TestFlow::new(
        policy_1,
        PolicyTemplateData {
            location_code: 37,
            lag: Some(2),
            cat: Some("Otis".to_string()),
            ..PolicyTemplateData::default()
        },
    )
    .await?;

    flow.push_context(
        TestContext::new(23).with_custom(maplit::hashmap! {"cat".to_string() => "Otis".to_telemetry()}.into()),
    )
    .await?;

    let ts = Utc::now().into();
    let item = TestItem::new(consts::PI, ts, 1);
    flow.push_item(item).await?;

    let item = TestItem::new(consts::E, ts, 2);
    flow.push_item(item).await?;

    tracing::info!("add to policy and resend");
    let cmd_rx = elements::PolicyFilterCmd::append_policy(
        elements::PolicySource::from_template_string(TestPolicy::base_template_name(), policy_2.to_string())?,
        None,
    );
    flow.tell_policy(cmd_rx).await?;

    let item = TestItem::new(consts::TAU, ts, 1);
    flow.push_item(item).await?;

    let item = TestItem::new(consts::LN_2, ts, 2);
    flow.push_item(item).await?;

    // tracing::info!("and reset policy and resend");
    // let cmd_rx = elements::PolicyFilterCmd::reset_policy();
    // flow.tell_policy(cmd_rx).await?;
    //
    // let item = TestItem::new(consts::FRAC_1_SQRT_2, ts, 1);
    // flow.push_item(item).await?;
    //
    // let item = TestItem::new(consts::SQRT_2, ts, 2);
    // flow.push_item(item).await?;
    //
    // let actual = flow.close().await?;
    // tracing::info!(?actual, "verifying actual result...");
    // assert_eq!(
    //     actual,
    //     vec![
    //         PolicyOutcome::new(
    //             TestItem::new(consts::E, ts, 2),
    //             TestContext::new(23).with_custom(maplit::hashmap! {"cat".to_string() =>
    // "Otis".to_telemetry()}),             QueryResult::passed_without_bindings(),
    //         ),
    //         PolicyOutcome::new(
    //             TestItem::new(consts::TAU, ts, 1),
    //             TestContext::new(23).with_custom(maplit::hashmap! {"cat".to_string() =>
    // "Otis".to_telemetry()}),             QueryResult {
    //                 passed: true,
    //                 bindings: maplit::hashmap! {
    //                     "custom".to_string() => vec![
    //                         TelemetryValue::Table(maplit::hashmap! { "cat".to_string() =>
    // "Otis".to_telemetry(), })                     ]
    //                 }
    //             }
    //         ),
    //         PolicyOutcome::new(
    //             TestItem::new(consts::LN_2, ts, 2),
    //             TestContext::new(23).with_custom(maplit::hashmap! {"cat".to_string() =>
    // "Otis".to_telemetry()}),             QueryResult {
    //                 passed: true,
    //                 bindings: maplit::hashmap! {
    //                     "custom".to_string() => vec![
    //                         TelemetryValue::Table(maplit::hashmap! { "cat".to_string() =>
    // "Otis".to_telemetry(), })                     ]
    //                 }
    //             }
    //         ),
    //         PolicyOutcome::new(
    //             TestItem::new(consts::SQRT_2, ts, 2),
    //             TestContext::new(23).with_custom(maplit::hashmap! {"cat".to_string() =>
    // "Otis".to_telemetry()}),             QueryResult::passed_without_bindings()
    //         ),
    //     ]
    // );
    Ok(())
}
