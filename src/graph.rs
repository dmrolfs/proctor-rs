mod node;
mod port;
mod shape;
pub mod stage;

use std::collections::VecDeque;
use std::fmt;

use tracing::Instrument;

use self::node::Node;
pub use self::port::{connect_out_to_in, Connect};
pub use self::port::{Inlet, Outlet, Port};
pub use self::shape::*;
use self::stage::Stage;
use crate::error::GraphError;
use crate::ProctorResult;

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

    fn node_names(&self) -> Vec<&str> { self.nodes.iter().map(|n| n.name.as_str()).collect() }

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
            .map_err(|err| GraphError::JoinError(err))?;

        let bad_apple = results.into_iter().find(|result| result.is_err());

        match bad_apple {
            None => (),
            Some(bad) => bad?,
        };

        Ok(())
    }
}
