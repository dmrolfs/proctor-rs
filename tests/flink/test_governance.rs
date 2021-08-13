use std::collections::HashSet;
use std::time::Duration;

use claim::*;
use proctor::elements::{self, PolicySource};
use proctor::flink::governance::{make_governance_transform, FlinkGovernanceContext, FlinkGovernancePolicy};
use proctor::flink::plan::{FlinkScalePlan, TimestampSeconds};
use proctor::graph::stage::{self, WithApi, WithMonitor};
use proctor::graph::{Connect, Graph, SinkShape, SourceShape};
use proctor::phases::governance::Governance;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::fixtures::TestSettings;

type Data = FlinkScalePlan;
type Context = FlinkGovernanceContext;

#[allow(dead_code)]
struct TestFlow {
    pub graph_handle: JoinHandle<()>,
    pub tx_data_source_api: stage::ActorSourceApi<Data>,
    pub tx_context_source_api: stage::ActorSourceApi<Context>,
    pub tx_governance_api: elements::PolicyFilterApi<Context>,
    pub rx_governance_monitor: elements::PolicyFilterMonitor<Data, Context>,
    pub tx_sink_api: stage::FoldApi<Vec<Data>>,
    pub rx_sink: Option<oneshot::Receiver<Vec<Data>>>,
}

impl TestFlow {
    pub async fn new(governance_stage: Governance<Data, Data, Context>) -> anyhow::Result<Self> {
        let data_source: stage::ActorSource<Data> = stage::ActorSource::new("plan_source");
        let tx_data_source_api = data_source.tx_api();

        let context_source: stage::ActorSource<Context> = stage::ActorSource::new("context_source");
        let tx_context_source_api = context_source.tx_api();

        let tx_governance_api = governance_stage.tx_api();
        let rx_governance_monitor = governance_stage.rx_monitor();

        let mut sink = stage::Fold::<_, Data, _>::new("sink", Vec::new(), |mut acc, item| {
            acc.push(item);
            acc
        });
        let tx_sink_api = sink.tx_api();
        let rx_sink = sink.take_final_rx();

        (data_source.outlet(), governance_stage.inlet()).connect().await;
        (context_source.outlet(), governance_stage.context_inlet()).connect().await;
        (governance_stage.outlet(), sink.inlet()).connect().await;
        assert!(governance_stage.inlet().is_attached().await);
        assert!(governance_stage.context_inlet().is_attached().await);
        assert!(governance_stage.outlet().is_attached().await);

        let mut graph = Graph::default();
        graph.push_back(Box::new(data_source)).await;
        graph.push_back(Box::new(context_source)).await;
        graph.push_back(Box::new(governance_stage)).await;
        graph.push_back(Box::new(sink)).await;

        let graph_handle = tokio::spawn(async move {
            graph
                .run()
                .await
                .map_err(|err| {
                    tracing::error!(error=?err, "graph run failed!");
                    err
                })
                .expect("graph run failed!")
        });

        Ok(Self {
            graph_handle,
            tx_data_source_api,
            tx_context_source_api,
            tx_governance_api,
            rx_governance_monitor,
            tx_sink_api,
            rx_sink,
        })
    }

    pub async fn push_data(&self, data: Data) -> anyhow::Result<()> {
        let (cmd, ack) = stage::ActorSourceCmd::push(data);
        self.tx_data_source_api.send(cmd)?;
        Ok(ack.await?)
    }

    pub async fn push_context(&self, context: Context) -> anyhow::Result<()> {
        let (cmd, ack) = stage::ActorSourceCmd::push(context);
        self.tx_context_source_api.send(cmd)?;
        Ok(ack.await?)
    }

    #[allow(dead_code)]
    pub async fn tell_policy(
        &self, command_rx: (elements::PolicyFilterCmd<Context>, oneshot::Receiver<proctor::Ack>),
    ) -> anyhow::Result<proctor::Ack> {
        self.tx_governance_api.send(command_rx.0)?;
        Ok(command_rx.1.await?)
    }

    pub async fn recv_policy_event(&mut self) -> anyhow::Result<elements::PolicyFilterEvent<Data, Context>> {
        Ok(self.rx_governance_monitor.recv().await?)
    }

    #[allow(dead_code)]
    pub async fn inspect_policy_context(&self) -> anyhow::Result<elements::PolicyFilterDetail<Context>> {
        let (cmd, detail) = elements::PolicyFilterCmd::inspect();
        self.tx_governance_api.send(cmd)?;

        let result = detail.await.map(|d| {
            tracing::info!(detail=?d, "inspected policy.");
            d
        })?;

        Ok(result)
    }

    pub async fn inspect_sink(&self) -> anyhow::Result<Vec<Data>> {
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
        &self, _label: &str, timeout: Duration, mut check_size: impl FnMut(Vec<Data>) -> bool,
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
                    anyhow::bail!("failed to inspect sink");
                }
            } else {
                tracing::error!(?timeout, "check timeout exceeded - stopping check.");
                anyhow::bail!(format!("check {:?} timeout exceeded - stopping check.", timeout));
            }
        }

        Ok(result)
    }

    #[tracing::instrument(level = "warn", skip(self))]
    pub async fn close(mut self) -> anyhow::Result<Vec<Data>> {
        let (stop, _) = stage::ActorSourceCmd::stop();
        self.tx_data_source_api.send(stop)?;

        let (stop, _) = stage::ActorSourceCmd::stop();
        self.tx_context_source_api.send(stop)?;

        self.graph_handle.await?;

        let result = self.rx_sink.take().unwrap().await?;
        Ok(result)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flink_governance_flow_only_policy_preamble() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_governance_flow");
    let _ = main_span.enter();

    let policy = FlinkGovernancePolicy::new(&TestSettings {
        required_subscription_fields: HashSet::new(),
        optional_subscription_fields: HashSet::new(),
        source: PolicySource::NoPolicy,
    });

    let governance_stage = Governance::with_transform(
        "test_governance",
        policy,
        make_governance_transform("common_governance_transform"),
    )
    .await;

    let mut flow = TestFlow::new(governance_stage).await?;

    let min_cluster_size = 2;
    let max_cluster_size = 10;
    let max_scaling_step = 5;
    let context = FlinkGovernanceContext {
        min_cluster_size,
        max_cluster_size,
        max_scaling_step,
        custom: Default::default(),
    };
    tracing::info!(?context, "pushing test context...");
    assert_ok!(flow.push_context(context).await);

    let event = assert_ok!(flow.recv_policy_event().await);
    tracing::info!(?event, "received policy event.");
    claim::assert_matches!(event, elements::PolicyFilterEvent::ContextChanged(_));

    let timestamp = TimestampSeconds::new_secs(*super::fixtures::DT_1_TS);
    let plan = FlinkScalePlan {
        timestamp,
        target_nr_task_managers: 8,
        current_nr_task_managers: 4,
    };
    tracing::info!(?plan, "pushing happy plan...");
    assert_ok!(flow.push_data(plan).await);

    tracing::info!("waiting for plan to reach sink...");
    assert!(assert_ok!(
        flow.check_sink_accumulation("happy_1", Duration::from_millis(250), |acc| acc.len() == 1,)
            .await
    ));

    // todo test target below min cluster size
    // todo test target above max cluster size
    // todo test too big a scale step

    // todo test veto in subsequent test

    Ok(())
}
