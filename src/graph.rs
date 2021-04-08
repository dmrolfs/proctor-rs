mod node;
mod port;
mod shape;
pub mod stage;

pub use self::port::{connect_out_to_in, Connect};
pub use self::port::{Inlet, Outlet, Port};
pub use self::shape::*;

use self::node::Node;
use self::stage::Stage;
use crate::error::GraphError;
use std::collections::VecDeque;
use std::fmt;
use tracing::Instrument;

pub type GraphResult<T> = Result<T, GraphError>;

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
/// use proctor::graph::{self, Graph, Connect};
/// use proctor::graph::stage::{self, Stage};
/// use proctor::graph::{SourceShape, ThroughShape, SinkShape};
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
///         }
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

    #[tracing::instrument(level = "info", name = "run graph", skip(self))]
    pub async fn run(self) -> GraphResult<()> {
        let tasks = self
            .nodes
            .into_iter()
            .map(|node| {
                let name = node.name.clone();
                node.run()
                    .instrument(tracing::info_span!("graph_node_run", graph=%name))
            })
            .collect::<Vec<_>>();

        futures::future::try_join_all(tasks)
            .instrument(tracing::info_span!("graph_join_all"))
            .await
            .map_err(|err| {
                tracing::error!(error=?err, "at least one graph node run failed.");
                err.into()
            })
            .map(|_| ())
    }
}
