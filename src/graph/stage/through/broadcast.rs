use crate::graph::{GraphResult, Inlet, Outlet, OutletsShape, Port, Shape, SinkShape, Stage, UniformFanOutShape};
use crate::AppData;
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use std::fmt;

/// Fan-out the stream to several streams emitting each incoming upstream element to all downstream
/// consumers.
///
/// # Examples
///
/// ```rust
/// use proctor::graph::stage::{self, tick, Stage};
/// use proctor::graph::{Connect, Graph, SinkShape, SourceShape, UniformFanOutShape};
/// use proctor::telemetry::{get_subscriber, init_subscriber};
/// use serde::Deserialize;
/// use std::collections::HashMap;
/// use std::time::Duration;
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let subscriber = get_subscriber("proctor", "trace");
///     init_subscriber(subscriber);
///
///     let main_span = tracing::info_span!("main");
///     let _main_span_guard = main_span.enter();
///
///     let mut count_0 = stage::Fold::<_, i32, i32>::new("count", 0, |acc, _| acc + 1);
///     let rx_count = count_0.take_final_rx().unwrap();
///     let mut sum_1 = stage::Fold::<_, i32, i32>::new("sum", 0, |acc, i| acc + i);
///     let rx_sum = sum_1.take_final_rx().unwrap();
///     let mut concatenate_2 = stage::Fold::<_, i32, String>::new(
///         "concatenate",
///         String::new(),
///         |acc, i| {
///             if acc.is_empty() {
///                 i.to_string()
///             } else {
///                 format!("{}_{}", acc, i)
///             }
///         });
///     let rx_concatenate = concatenate_2.take_final_rx().unwrap();
///
///     let mut tick = stage::Tick::with_constraint(
///         "tick",
///         Duration::from_millis(0),
///         Duration::from_nanos(1),
///         3,
///         tick::Constraint::by_count(3),
///     );
///
///     let mut broadcast = stage::Broadcast::new("broadcast", 3);
///
///     (tick.outlet(), broadcast.inlet()).connect().await;
///     (broadcast.outlets().get_mut(0).unwrap(), count_0.inlet()).connect().await;
///     (broadcast.outlets().get_mut(1).unwrap(), sum_1.inlet()).connect().await;
///     (broadcast.outlets().get_mut(2).unwrap(), concatenate_2.inlet()).connect().await;
///
///     let mut g = Graph::default();
///     g.push_back(Box::new(tick)).await;
///     g.push_back(Box::new(broadcast)).await;
///     g.push_back(Box::new(count_0)).await;
///     g.push_back(Box::new(sum_1)).await;
///     g.push_back(Box::new(concatenate_2)).await;
///     g.run().await?;
///
///     assert_eq!(Ok(3), rx_count.await);
///     assert_eq!(Ok(9), rx_sum.await);
///     assert_eq!(Ok("3_3_3".to_string()), rx_concatenate.await);
///     Ok(())
/// }
/// ```
pub struct Broadcast<T: AppData + Clone> {
    name: String,
    inlet: Inlet<T>,
    outlets: Vec<Outlet<T>>,
}

impl<T: AppData + Clone> Broadcast<T> {
    pub fn new<S: Into<String>>(name: S, output_ports: usize) -> Self {
        let name = name.into();
        let inlet = Inlet::new(name.clone());
        let outlets = (0..output_ports).map(|pos| Outlet::new(format!("{}_{}", name, pos))).collect();

        Self { name, inlet, outlets }
    }
}

impl<T: AppData + Clone> Shape for Broadcast<T> {}

impl<T: AppData + Clone> UniformFanOutShape for Broadcast<T> {
    type Out = T;

    #[inline]
    fn outlets(&self) -> OutletsShape<Self::Out> {
        self.outlets.clone()
    }
}

impl<T: AppData + Clone> SinkShape for Broadcast<T> {
    type In = T;
    #[inline]
    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<T: AppData + Clone> Stage for Broadcast<T> {
    #[inline]
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(level = "info", name = "run broadcast through", skip(self))]
    async fn run(&mut self) -> GraphResult<()> {
        let outlets = &self.outlets;
        while let Some(item) = self.inlet.recv().await {
            for o in outlets.iter() {
                o.send(item.clone()).await?;
            }
        }

        Ok(())
    }

    async fn close(mut self: Box<Self>) -> GraphResult<()> {
        tracing::trace!("closing broadcast-through ports.");
        self.inlet.close().await;
        for o in self.outlets.iter_mut() {
            o.close().await;
        }
        Ok(())
    }
}

impl<T: AppData + Clone> fmt::Debug for Broadcast<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Broadcast")
            .field("name", &self.name)
            .field("inlet", &self.inlet)
            .field("outlets", &self.outlets)
            .finish()
    }
}

// /////////////////////////////////////////////////////
// // Unit Tests ///////////////////////////////////////
//
#[cfg(test)]
mod tests {
    // use super::*;
    // use tokio::sync::mpsc;
    // use tokio_test::block_on;
}
