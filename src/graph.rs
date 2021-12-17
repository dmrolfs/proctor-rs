mod node;
mod port;
mod shape;
pub mod stage;

use std::collections::VecDeque;
use std::fmt;

use once_cell::sync::Lazy;
use prometheus::{IntCounterVec, Opts};
use tracing::Instrument;

use self::node::Node;
pub use self::port::{connect_out_to_in, Connect};
pub use self::port::{Inlet, Outlet, Port, PORT_CONTEXT, PORT_DATA};
pub use self::port::{STAGE_EGRESS_COUNTS, STAGE_INGRESS_COUNTS};
pub use self::shape::*;
use self::stage::Stage;
use crate::error::{GraphError, MetricLabel, ProctorError};
use crate::ProctorResult;

pub static GRAPH_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "proctor_graph_errors",
            "Number of recoverable errors occurring in graph processing",
        ),
        &["stage", "error_type"],
    )
    .expect("failed creating proctor_graph_errors metric")
});

#[inline]
pub fn track_errors(stage: &str, error: &ProctorError) {
    GRAPH_ERRORS.with_label_values(&[stage, error.label().as_ref()]).inc()
}

/// A Graph represents a runnable stream processing graph.
///
/// A Graph has one or `Source` nodes, zero or more `Through` nodes and one or more `Sink` nodes.
/// Each node is connected to each other via their respective `Inlet` and `Outlet` `Ports`.
///
/// In order to use a `Graph`, its nodes must be registered and connected. Then the `Graph` may be
/// ran, which will spawn asynchronous tasks for each node (via `tokio::spawn`). Once run, the
/// underlying graph nodes will executed until they source(s) complete or the graph is aborted.
///
/// # Examples
///
/// ```
/// use proctor::graph::stage::{self, Stage};
/// use proctor::graph::{self, Connect, Graph};
/// use proctor::graph::{SinkShape, SourceShape, ThroughShape};
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let mut source = stage::Sequence::new("my_data", vec![1, 2, 3, 4, 5]);
///     let mut sq = stage::Map::new("square-values", |x| x * x);
///     let mut sum = stage::Fold::new("sum", 0, |acc, x| acc + x);
///     let mut rx_sum_sq = sum.take_final_rx().unwrap();
///
///     (source.outlet(), sq.inlet()).connect().await;
///     (sq.outlet(), sum.inlet()).connect().await;
///     let mut g = Graph::default();
///     g.push_back(Box::new(source)).await;
///     g.push_back(Box::new(sq)).await;
///     g.push_back(Box::new(sum)).await;
///
///     g.run().await?;
///
///     match rx_sum_sq.try_recv() {
///         Ok(sum_sq) => {
///             println!("sum of squares is {}", sum_sq);
///             assert_eq!(55, sum_sq)
///         },
///
///         Err(err) => panic!("sum of squares not calculated: {}", err),
///     }
///
///     Ok(())
/// }
/// ```
#[derive(Default, fmt::Debug)]
pub struct Graph {
    nodes: VecDeque<Node>,
}

impl Graph {
    pub async fn push_front(&mut self, stage: Box<dyn Stage>) {
        let node = Node::new(stage);
        self.nodes.push_front(node);
    }

    pub async fn push_back(&mut self, stage: Box<dyn Stage>) {
        let node = Node::new(stage);
        self.nodes.push_back(node);
    }

    fn node_names(&self) -> Vec<&str> {
        self.nodes.iter().map(|n| n.name.as_str()).collect()
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn check(&self) -> ProctorResult<()> {
        tracing::info!(nodes=?self.node_names(), "checking graph nodes.");
        for node in self.nodes.iter() {
            node.check().await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn run(self) -> ProctorResult<()> {
        self.check().await?;

        let tasks = self.nodes.into_iter().map(|node| node.run()).collect::<Vec<_>>();

        let results: Vec<ProctorResult<()>> = futures::future::try_join_all(tasks)
            .instrument(tracing::info_span!("graph_run_join_all"))
            .await
            .map_err(GraphError::JoinError)?;

        let results: ProctorResult<Vec<()>> = results.into_iter().collect();
        let _ = results?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use claim::*;
    use pretty_assertions::assert_eq;
    use prometheus::Registry;

    use super::*;
    use crate::error::{EligibilityError, GraphError, PortError};

    #[test]
    fn test_track_error_metric() {
        let registry_name = "test_track_error_metric";
        let registry = assert_ok!(Registry::new_custom(Some(registry_name.to_string()), None));
        assert_ok!(registry.register(Box::new(GRAPH_ERRORS.clone())));
        track_errors(
            "foo",
            &GraphError::PortError(PortError::Detached("detached foo".to_string())).into(),
        );
        track_errors("foo", &EligibilityError::DataNotFound("no foo".to_string()).into());
        track_errors("bar", &EligibilityError::ParseError("bad smell".to_string()).into());

        let metric_family = registry.gather();
        assert_eq!(metric_family.len(), 1);
        assert_eq!(
            metric_family[0].get_name(),
            &format!("{}_{}", registry_name, "proctor_graph_errors")
        );
        let metrics = metric_family[0].get_metric();
        assert_eq!(metrics.len(), 3);
        let error_types: Vec<&str> = metrics
            .iter()
            .flat_map(|m| {
                m.get_label()
                    .iter()
                    .filter(|l| l.get_name() == "error_type" )
                    .map(|l| l.get_value())
            })
            // .sorted()
            .collect();
        assert_eq!(
            error_types,
            vec![
                "proctor::eligibility::data_not_found",
                "proctor::eligibility::parse",
                "proctor::graph::port::detached",
            ]
        );
    }
}
