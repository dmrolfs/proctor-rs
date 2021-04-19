use crate::graph::{FanInShape2, GraphResult, Inlet, Outlet, Port, SourceShape, Stage};
use crate::AppData;
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use std::fmt::{self, Debug};

/// Merge multiple sources. Picks elements randomly if all sources has elements ready.
///
/// # Examples
///
/// ```
/// use tokio::sync::mpsc;
/// use proctor::graph::{Graph, Connect, Inlet, FanInShape2, ThroughShape, SinkShape, SourceShape};
/// use proctor::graph::stage::{self, Stage};
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let mut source_0 = stage::Sequence::new("first", (1..=3));
///     let mut source_1 = stage::Sequence::new("second", (10..=30).step_by(10));
///     let mut merge = stage::Merge::new("merge_streams");
///     let mut sum = stage::Fold::new("sum", 0, |acc, x| acc + x );
///     let rx_sum = sum.take_final_rx().unwrap();
///
///     (source_0.outlet(), merge.inlet_0()).connect().await;
///     (source_1.outlet(), merge.inlet_1()).connect().await;
///     (merge.outlet(), sum.inlet()).connect().await;
///
///     let mut g = Graph::default();
///     g.push_back(Box::new(source_0)).await;
///     g.push_back(Box::new(source_1)).await;
///     g.push_back(Box::new(merge)).await;
///     g.push_back(Box::new(sum)).await;
///     g.run().await?;
///
///     assert_eq!(66, rx_sum.await?);
///     Ok(())
/// }
/// ```
pub struct Merge<T> {
    name: String,
    inlet_0: Inlet<T>,
    inlet_1: Inlet<T>,
    outlet: Outlet<T>,
}

impl<T> Merge<T> {
    pub fn new<S: Into<String>>(name: S) -> Self {
        let name = name.into();
        let inlet_0 = Inlet::new(format!("{}_0", name));
        let inlet_1 = Inlet::new(format!("{}_1", name));
        let outlet = Outlet::new(name.clone());
        Self {
            name,
            inlet_0,
            inlet_1,
            outlet,
        }
    }
}

impl<T> FanInShape2 for Merge<T> {
    type In0 = T;
    type In1 = T;

    #[inline]
    fn inlet_0(&self) -> Inlet<Self::In0> {
        self.inlet_0.clone()
    }

    #[inline]
    fn inlet_1(&self) -> Inlet<Self::In1> {
        self.inlet_1.clone()
    }
}

impl<T> SourceShape for Merge<T> {
    type Out = T;
    #[inline]
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<T: AppData> Stage for Merge<T> {
    #[inline]
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> GraphResult<()> {
        self.inlet_0.check_attachment().await?;
        self.inlet_1.check_attachment().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run merge through", skip(self))]
    async fn run(&mut self) -> GraphResult<()> {
        let outlet = &self.outlet;
        let rx_0 = &mut self.inlet_0;
        let rx_1 = &mut self.inlet_1;
        // let noop = future::ok(());
        loop {
            tokio::select! {
                Some(t) = rx_0.recv() => {
                    tracing::info!(item=?t, "inlet_0 receiving");
                    let _ = outlet.send(t).await?;
                },

                Some(t) = rx_1.recv() => {
                    tracing::info!(item=?t, "inlet_1 receiving");
                    let _ = outlet.send(t).await?;
                },

                else => {
                    tracing::warn!("merge done - breaking...");
                    break;
                },
            }
        }

        Ok(())
    }

    async fn close(mut self: Box<Self>) -> GraphResult<()> {
        tracing::trace!("closing merge-through ports.");
        self.inlet_0.close().await;
        self.inlet_1.close().await;
        self.outlet.close().await;
        Ok(())
    }
}

impl<T> Debug for Merge<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Merge")
            .field("name", &self.name)
            .field("inlet_0", &self.inlet_0)
            .field("inlet_1", &self.inlet_1)
            .field("outlet", &self.outlet)
            .finish()
    }
}
