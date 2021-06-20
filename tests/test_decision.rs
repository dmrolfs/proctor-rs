mod fixtures;

use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use oso::{PolarValue, ToPolar};
use pretty_assertions::assert_eq;
use proctor::elements;
use proctor::elements::{
    Policy, PolicyOutcome, PolicySettings, PolicySource, PolicySubscription, Telemetry, TelemetryValue, ToTelemetry,
};
use proctor::flink::decision::context::FlinkDecisionContext;
use proctor::flink::decision::policy::DecisionPolicy;
use proctor::flink::decision::result::{make_decision_transform, DecisionResult};
use proctor::flink::{FlowMetrics, MetricCatalog, UtilizationMetrics};
use proctor::graph::stage::{self, ThroughStage, WithApi, WithMonitor};
use proctor::graph::{Connect, Graph, Inlet, SinkShape, SourceShape, UniformFanInShape};
use proctor::phases::collection;
use proctor::phases::collection::TelemetrySubscription;
use proctor::phases::decision::Decision;
use proctor::{AppData, ProctorContext};
use serde::de::DeserializeOwned;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

type Args<T, C> = (T, C, PolarValue);

lazy_static! {
    static ref DT_1: DateTime<Utc> = DateTime::parse_from_str("2021-05-05T17:11:07.246310806Z", "%+")
        .unwrap()
        .with_timezone(&Utc);
    static ref DT_1_STR: String = format!("{}", DT_1.format("%+"));
    static ref DT_1_TS: i64 = DT_1.timestamp();
}

struct TestSettings {
    pub required_subscription_fields: HashSet<String>,
    pub optional_subscription_fields: HashSet<String>,
    pub source: PolicySource,
}

impl PolicySettings for TestSettings {
    fn required_subscription_fields(&self) -> HashSet<String> {
        self.required_subscription_fields.clone()
    }

    fn optional_subscription_fields(&self) -> HashSet<String> {
        self.optional_subscription_fields.clone()
    }

    fn source(&self) -> PolicySource {
        self.source.clone()
    }
}

fn make_test_policy(
    settings: &impl PolicySettings,
) -> impl Policy<MetricCatalog, FlinkDecisionContext, Args<MetricCatalog, FlinkDecisionContext>> {
    DecisionPolicy::new(settings)
}

// enum DecisionHandler<In, Out> {
//     StripPolicyResult,
//     CarryPolicyResult,
//     WithTransform(Box<dyn ThroughStage<In, Out>>),
// }
//
// impl<In, Out> DecisionHandler<In, Out>
// where
//     In: AppData + Clone + DeserializeOwned + ToPolar,
// {
//     pub async fn make_phase_stage<C>(
//         &self, policy: impl QueryPolicy<Item = In, Context = C, Args = Args<In, C>> + 'static,
//     ) -> impl ThroughStage<In, Out>
//     where
//         C: ProctorContext,
//     {
//         match self {
//             DecisionHandler::StripPolicyResult => Decision::<In, In, C>::basic("basic_decision", policy).await,
//
//             DecisionHandler::CarryPolicyResult => {
//                 Decision::<In, PolicyResult<In>, C>::carry_policy_result("carried_decision", policy).await
//             }
//
//             DecisionHandler::WithTransform(xform) => {
//                 Decision::<In, Out, C>::with_transform("transformed_decision", policy, xform).await
//             }
//         }
//     }
// }

struct TestFlow<In, Out, C> {
    pub graph_handle: JoinHandle<()>,
    pub tx_data_source_api: stage::ActorSourceApi<Telemetry>,
    pub tx_context_source_api: stage::ActorSourceApi<Telemetry>,
    pub tx_clearinghouse_api: collection::ClearinghouseApi,
    pub tx_decision_api: elements::PolicyFilterApi<C>,
    pub rx_decision_monitor: elements::PolicyFilterMonitor<In, C>,
    pub tx_sink_api: stage::FoldApi<Vec<Out>>,
    pub rx_sink: Option<oneshot::Receiver<Vec<Out>>>,
}

impl<In, Out, C> TestFlow<In, Out, C>
where
    In: AppData + Clone + DeserializeOwned + ToPolar,
    Out: AppData + Clone,
    C: ProctorContext,
{
    pub async fn new(
        telemetry_subscription: TelemetrySubscription, context_subscription: TelemetrySubscription,
        decision_stage: impl ThroughStage<In, Out>, decision_context_inlet: Inlet<C>,
        tx_decision_api: elements::PolicyFilterApi<C>, rx_decision_monitor: elements::PolicyFilterMonitor<In, C>,
    ) -> anyhow::Result<Self> {
        let telemetry_source = stage::ActorSource::<Telemetry>::new("telemetry_source");
        let tx_data_source_api = telemetry_source.tx_api();

        let ctx_source = stage::ActorSource::<Telemetry>::new("context_source");
        let tx_context_source_api = ctx_source.tx_api();

        let merge = stage::MergeN::new("source_merge", 2);

        let mut clearinghouse = collection::Clearinghouse::new("clearinghouse");
        let tx_clearinghouse_api = clearinghouse.tx_api();

        // let context_subscription = policy.subscription("decision_context");
        let context_channel = collection::SubscriptionChannel::<C>::new("decision_context").await?;

        let telemetry_channel = collection::SubscriptionChannel::<In>::new("data_channel").await?;

        let mut sink = stage::Fold::<_, Out, _>::new("sink", Vec::new(), |mut acc, item| {
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
        (context_channel.outlet(), decision_context_inlet.clone()).connect().await;
        (telemetry_channel.outlet(), decision_stage.inlet()).connect().await;
        (decision_stage.outlet(), sink.inlet()).connect().await;

        assert!(decision_context_inlet.is_attached().await);
        assert!(decision_stage.inlet().is_attached().await);
        assert!(decision_stage.outlet().is_attached().await);

        let mut graph = Graph::default();
        graph.push_back(Box::new(telemetry_source)).await;
        graph.push_back(Box::new(ctx_source)).await;
        graph.push_back(Box::new(merge)).await;
        graph.push_back(Box::new(clearinghouse)).await;
        graph.push_back(Box::new(context_channel)).await;
        graph.push_back(Box::new(telemetry_channel)).await;
        graph.push_back(Box::new(decision_stage)).await;
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
            tx_decision_api,
            rx_decision_monitor,
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

    #[allow(dead_code)]
    pub async fn tell_policy(
        &self, command_rx: (elements::PolicyFilterCmd<C>, oneshot::Receiver<proctor::Ack>),
    ) -> anyhow::Result<proctor::Ack> {
        self.tx_decision_api.send(command_rx.0)?;
        command_rx.1.await.map_err(|err| err.into())
    }

    pub async fn recv_policy_event(&mut self) -> anyhow::Result<elements::PolicyFilterEvent<In, C>> {
        self.rx_decision_monitor.recv().await.map_err(|err| err.into())
    }

    #[allow(dead_code)]
    pub async fn inspect_policy_context(&self) -> anyhow::Result<elements::PolicyFilterDetail<C>> {
        let (cmd, detail) = elements::PolicyFilterCmd::inspect();
        self.tx_decision_api.send(cmd)?;
        detail
            .await
            .map(|d| {
                tracing::info!(detail=?d, "inspected policy.");
                d
            })
            .map_err(|err| err.into())
    }

    pub async fn inspect_sink(&self) -> anyhow::Result<Vec<Out>> {
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
        &self, label: &str, timeout: Duration, mut check_size: impl FnMut(Vec<Out>) -> bool,
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
    pub async fn close(mut self) -> anyhow::Result<Vec<Out>> {
        let (stop, _) = stage::ActorSourceCmd::stop();
        self.tx_data_source_api.send(stop)?;

        let (stop, _) = stage::ActorSourceCmd::stop();
        self.tx_context_source_api.send(stop)?;

        self.graph_handle.await?;

        self.rx_sink.take().unwrap().await.map_err(|err| err.into())
    }
}

fn make_test_item(timestamp: DateTime<Utc>, input_messages_per_sec: f64, inbox_lag: f64) -> MetricCatalog {
    MetricCatalog {
        timestamp,
        flow: FlowMetrics {
            input_messages_per_sec,
            input_consumer_lag: inbox_lag,
            records_out_per_sec: 0.0,
            max_message_latency: 0.0,
            net_in_utilization: 0.0,
            net_out_utilization: 0.0,
            sink_health_metrics: 0.0,
            task_nr_records_in_per_sec: 0.0,
            task_nr_records_out_per_sec: 0.0,
        },
        utilization: UtilizationMetrics { task_cpu_load: 0.0, network_io_utilization: 0.0 },
        custom: std::collections::HashMap::default(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_decision_carry_policy_result() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_decision_carry_policy_result");
    let _ = main_span.enter();

    let telemetry_subscription = TelemetrySubscription::new("measurements")
        .with_required_fields(proctor::flink::metric_catalog::METRIC_CATALOG_REQ_SUBSCRIPTION_FIELDS.clone())
        .with_optional_fields(maplit::hashset! {
            "all_sinks_healthy",
            "nr_task_managers",
        });

    let policy = make_test_policy(&TestSettings {
        required_subscription_fields: HashSet::new(),
        optional_subscription_fields: HashSet::new(),
        source: PolicySource::String(
            r#"scale_up(item, context, _) if 3.0 < item.flow.input_messages_per_sec;
            scale_down(item, context, _) if item.flow.input_messages_per_sec < 1.0;"#
                .to_string(),
        ),
    });

    let context_subscription = policy.subscription("decision_context");

    let decision_stage = Decision::<
        MetricCatalog,
        PolicyOutcome<MetricCatalog, FlinkDecisionContext>,
        FlinkDecisionContext,
    >::carry_policy_result("carry_policy_decision", policy)
    .await;
    let decision_context_inlet = decision_stage.context_inlet();
    let tx_decision_api: elements::PolicyFilterApi<FlinkDecisionContext> = decision_stage.tx_api();
    let rx_decision_monitor: elements::PolicyFilterMonitor<MetricCatalog, FlinkDecisionContext> =
        decision_stage.rx_monitor();

    let mut flow = TestFlow::new(
        telemetry_subscription,
        context_subscription,
        decision_stage,
        decision_context_inlet,
        tx_decision_api,
        rx_decision_monitor,
    )
    .await?;

    flow.push_context(maplit::hashmap! {
        "all_sinks_healthy" => true.to_telemetry(),
        "nr_task_managers" => 4.to_telemetry(),
    })
    .await?;

    let event = flow.recv_policy_event().await?;
    assert!(matches!(event, elements::PolicyFilterEvent::ContextChanged(_)));

    let ts = *DT_1 + chrono::Duration::hours(1);
    let item = make_test_item(ts, std::f64::consts::PI, 1.0);
    tracing::warn!(?item, "DMR-A.1: created item to push.");
    let telemetry = Telemetry::try_from(&item);
    tracing::warn!(?item, ?telemetry, "DMR-A.2: converted item to telemetry and pushing...");
    flow.push_telemetry(telemetry?).await?;
    tracing::info!("waiting for item to reach sink...");
    assert!(
        flow.check_sink_accumulation("first", Duration::from_millis(250), |acc| acc.len() == 1)
            .await?
    );

    let item = make_test_item(ts, std::f64::consts::E, 2.0);
    let telemetry = Telemetry::try_from(&item);
    flow.push_telemetry(telemetry?).await?;
    let event = flow.recv_policy_event().await?;
    assert!(matches!(event, elements::PolicyFilterEvent::ItemBlocked(_)));
    tracing::warn!(?event, "DMR-C: item dropped confirmed");

    let item = make_test_item(ts, std::f64::consts::LN_2, 1.0);
    tracing::warn!(?item, "DMR-D.1: created item to push.");
    let telemetry = Telemetry::try_from(&item);
    tracing::warn!(?item, ?telemetry, "DMR-D.2: converted item to telemetry and pushing...");
    flow.push_telemetry(telemetry?).await?;
    tracing::info!("waiting for item to reach sink...");
    assert!(
        flow.check_sink_accumulation("first", Duration::from_millis(250), |acc| acc.len() == 2)
            .await?
    );

    let actual: Vec<PolicyOutcome<MetricCatalog, FlinkDecisionContext>> = flow.close().await?;
    tracing::warn!(?actual, "DMR: 08. Verify final accumulation...");
    let actual_vals: Vec<(f64, Option<String>)> = actual
        .into_iter()
        .map(|a| {
            let direction = a
                .bindings
                .get("direction")
                .cloned()
                .map(|d| String::try_from(d).expect("failed to get string from telemetry value"));

            (a.item.flow.input_messages_per_sec, direction)
        })
        .collect();

    assert_eq!(
        actual_vals,
        vec![
            (std::f64::consts::PI, Some("up".to_string())),
            (std::f64::consts::LN_2, Some("down".to_string())),
        ]
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_decision_common() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_decision_basic");
    let _ = main_span.enter();

    let telemetry_subscription = TelemetrySubscription::new("measurements")
        .with_required_fields(proctor::flink::metric_catalog::METRIC_CATALOG_REQ_SUBSCRIPTION_FIELDS.clone())
        .with_optional_fields(maplit::hashset! {
            "all_sinks_healthy",
            "nr_task_managers",
        });

    let policy = make_test_policy(&TestSettings {
        required_subscription_fields: HashSet::new(),
        optional_subscription_fields: HashSet::new(),
        source: PolicySource::String(
            r#"scale_up(item, context, _) if 3.0 < item.flow.input_messages_per_sec;
            scale_down(item, context, _) if item.flow.input_messages_per_sec < 1.0;"#
                .to_string(),
        ),
    });

    let context_subscription = policy.subscription("decision_context");

    let decision_stage =
        Decision::<MetricCatalog, DecisionResult<MetricCatalog>, FlinkDecisionContext>::with_transform(
            "common_decision",
            policy,
            make_decision_transform("common_decision_transform"),
        )
        .await;
    let decision_context_inlet = decision_stage.context_inlet();
    let tx_decision_api: elements::PolicyFilterApi<FlinkDecisionContext> = decision_stage.tx_api();
    let rx_decision_monitor: elements::PolicyFilterMonitor<MetricCatalog, FlinkDecisionContext> =
        decision_stage.rx_monitor();

    let mut flow = TestFlow::new(
        telemetry_subscription,
        context_subscription,
        decision_stage,
        decision_context_inlet,
        tx_decision_api,
        rx_decision_monitor,
    )
    .await?;

    flow.push_context(maplit::hashmap! {
        "all_sinks_healthy" => true.to_telemetry(),
        "nr_task_managers" => 4.to_telemetry(),
    })
    .await?;

    let event = flow.recv_policy_event().await?;
    assert!(matches!(event, elements::PolicyFilterEvent::ContextChanged(_)));

    let ts = *DT_1 + chrono::Duration::hours(1);
    let item = make_test_item(ts, std::f64::consts::PI, 1.0);
    tracing::warn!(?item, "DMR-A.1: created item to push.");
    let telemetry = Telemetry::try_from(&item);
    tracing::warn!(?item, ?telemetry, "DMR-A.2: converted item to telemetry and pushing...");
    flow.push_telemetry(telemetry?).await?;
    tracing::info!("waiting for item to reach sink...");
    assert!(
        flow.check_sink_accumulation("first", Duration::from_millis(500), |acc| acc.len() == 1)
            .await?
    );

    let item = make_test_item(ts, std::f64::consts::E, 2.0);
    let telemetry = Telemetry::try_from(&item);
    flow.push_telemetry(telemetry?).await?;
    let event = flow.recv_policy_event().await?;
    assert!(matches!(event, elements::PolicyFilterEvent::ItemBlocked(_)));
    tracing::warn!(?event, "DMR-C: item dropped confirmed");

    let item = make_test_item(ts, std::f64::consts::LN_2, 1.0);
    tracing::warn!(?item, "DMR-D.1: created item to push.");
    let telemetry = Telemetry::try_from(&item);
    tracing::warn!(?item, ?telemetry, "DMR-D.2: converted item to telemetry and pushing...");
    flow.push_telemetry(telemetry?).await?;
    tracing::info!("waiting for item to reach sink...");
    assert!(
        flow.check_sink_accumulation("first", Duration::from_millis(250), |acc| acc.len() == 2)
            .await?
    );

    let actual: Vec<DecisionResult<MetricCatalog>> = flow.close().await?;
    tracing::warn!(?actual, "DMR: 08. Verify final accumulation...");
    let actual_vals: Vec<(f64, &'static str)> = actual
        .into_iter()
        .map(|a| match a {
            DecisionResult::ScaleUp(item) => (item.flow.input_messages_per_sec, "up"),
            DecisionResult::ScaleDown(item) => (item.flow.input_messages_per_sec, "down"),
            DecisionResult::NoAction(item) => (item.flow.input_messages_per_sec, "no action"),
        })
        .collect();

    assert_eq!(
        actual_vals,
        vec![(std::f64::consts::PI, "up"), (std::f64::consts::LN_2, "down"),]
    );

    Ok(())
}
