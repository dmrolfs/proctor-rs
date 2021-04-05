use crate::graph::shape::{Shape, SinkShape, SourceShape, ThroughShape};
use crate::graph::{GraphResult, Inlet, Outlet, Port, Stage};
use crate::AppData;
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use futures::future::Future;
use std::fmt;

/// Transform this stream by applying the given function to each of the elements as they pass
/// through this processing step.
///
/// # Examples
///
/// ```
/// use tokio::sync::mpsc;
/// use proctor::graph::{Graph, Connect, Inlet, GraphResult};
/// use proctor::graph::stage::{self, Stage};
/// use proctor::graph::{SourceShape, ThroughShape, SinkShape};
/// use futures::future;
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let mut source = stage::Sequence::new("src", (4..=10));
///     let bar = "17".to_string();
///
///     let mut sq_plus = stage::AndThen::new(
///         "square values then add",
///         move |x| { future::ok(x * x + bar.parse::<i32>().expect("failed to parse bar.")) },
///     );
///     let mut fold = stage::Fold::<_, GraphResult<i32>, i32>::new("sum values", 0, |acc, x| acc + x.expect("error during calc") );
///     let rx_sum_sq = fold.take_final_rx().unwrap();
///
///     (source.outlet(), sq_plus.inlet()).connect().await;
///     (sq_plus.outlet(), fold.inlet()).connect().await;
///
///     let mut g = Graph::default();
///     g.push_back(Box::new(source)).await;
///     g.push_back(Box::new(sq_plus)).await;
///     g.push_back(Box::new(fold)).await;
///
///     g.run().await?;
///
///     let actual = rx_sum_sq.await;
///     assert_eq!(actual, Ok(490));
///     Ok(())
/// }
/// ```
pub struct AndThen<Op, Fut, In, Out>
where
    In: AppData,
    Out: AppData,
    Fut: Future<Output = Out> + Send,
    Op: FnMut(In) -> Fut + Send + Sync,
{
    name: String,
    operation: Op,
    inlet: Inlet<In>,
    outlet: Outlet<Out>,
}

impl<Op, Fut, In, Out> fmt::Debug for AndThen<Op, Fut, In, Out>
where
    In: AppData,
    Out: AppData,
    Fut: Future<Output = Out> + Send,
    Op: FnMut(In) -> Fut + Send + Sync,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncMap")
            .field("name", &self.name)
            .field("inlet", &self.inlet)
            .field("outlet", &self.outlet)
            .finish()
    }
}

impl<Op, Fut, In, Out> AndThen<Op, Fut, In, Out>
where
    In: AppData,
    Out: AppData,
    Fut: Future<Output = Out> + Send,
    Op: FnMut(In) -> Fut + Send + Sync,
{
    pub fn new<S>(name: S, operation: Op) -> Self
    where
        S: Into<String>,
    {
        let name = name.into();
        let inlet = Inlet::new(name.clone());
        let outlet = Outlet::new(name.clone());
        Self {
            name,
            operation,
            inlet,
            outlet,
        }
    }
}

impl<Op, Fut, In, Out> Shape for AndThen<Op, Fut, In, Out>
where
    In: AppData,
    Out: AppData,
    Fut: Future<Output = Out> + Send,
    Op: FnMut(In) -> Fut + Send + Sync,
{
}

impl<Op, Fut, In, Out> ThroughShape for AndThen<Op, Fut, In, Out>
where
    In: AppData,
    Out: AppData,
    Fut: Future<Output = Out> + Send,
    Op: FnMut(In) -> Fut + Send + Sync,
{
}

impl<Op, Fut, In, Out> SourceShape for AndThen<Op, Fut, In, Out>
where
    In: AppData,
    Out: AppData,
    Fut: Future<Output = Out> + Send,
    Op: FnMut(In) -> Fut + Send + Sync,
{
    type Out = Out;
    #[inline]
    fn outlet(&mut self) -> &mut Outlet<Self::Out> {
        &mut self.outlet
    }
}

impl<Op, Fut, In, Out> SinkShape for AndThen<Op, Fut, In, Out>
where
    In: AppData,
    Out: AppData,
    Fut: Future<Output = Out> + Send,
    Op: FnMut(In) -> Fut + Send + Sync,
{
    type In = In;
    #[inline]
    fn inlet(&mut self) -> &mut Inlet<Self::In> {
        &mut self.inlet
    }
}

#[dyn_upcast]
#[async_trait]
impl<Op, Fut, In, Out> Stage for AndThen<Op, Fut, In, Out>
where
    In: AppData,
    Out: AppData,
    Fut: Future<Output = Out> + Send + 'static,
    Op: FnMut(In) -> Fut + Send + Sync + 'static,
{
    #[inline]
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(level = "info", name = "run map through", skip(self))]
    async fn run(&mut self) -> GraphResult<()> {
        let outlet = &self.outlet;

        while let Some(input) = self.inlet.recv().await {
            let value: Out = (self.operation)(input).await;
            outlet.send(value).await?;
        }

        Ok(())
    }

    async fn close(mut self: Box<Self>) -> GraphResult<()> {
        tracing::trace!("closing map-through ports.");
        self.inlet.close().await;
        self.outlet.close().await;
        Ok(())
    }
}

// /////////////////////////////////////////////////////
// // Unit Tests ///////////////////////////////////////
//
#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::SourceShape;
    use futures::future;
    use tokio::sync::mpsc;
    use tokio_test::block_on;

    #[test]
    fn test_basic_usage() {
        let my_data = vec![1, 2, 3];
        let (tx_in, rx_in) = mpsc::channel::<i32>(8);
        let (tx_out, mut rx_out) = mpsc::channel::<GraphResult<i32>>(8);

        let bar = "17".to_string(); // important to type check closing over non-copy value
        let mut map = AndThen::new("square values", move |x| {
            future::ok(x * x + bar.parse::<i32>().expect("failed to parse bar."))
        });

        let mut actual = Vec::with_capacity(3);

        block_on(async {
            map.inlet.attach(rx_in).await;
            map.outlet.attach(tx_out).await;

            let map_handle = tokio::spawn(async move {
                map.run().await.expect("failed to run and_then stage");
            });

            let source_handle = tokio::spawn(async move {
                for x in my_data {
                    tx_in.send(x).await.expect("failed to send data");
                }
            });

            source_handle.await.unwrap();
            map_handle.await.unwrap();
            while let Some(x) = rx_out.recv().await {
                actual.push(x.expect("error during and_then mapping"));
            }
        });

        assert_eq!(vec![18, 21, 26], actual);
    }

    #[test]
    fn test_graph_usage() {
        use crate::graph::stage;
        use crate::graph::{Connect, Graph};

        let mut source = stage::Sequence::new("src", 4..=10);

        let bar = "17".to_string(); // important to type check closing over non-copy value
        let mut calc = AndThen::new("square values", move |x| {
            future::ok(x * x + bar.parse::<i32>().expect("failed to parse bar."))
        });

        let (tx_out, mut rx_out) = mpsc::channel::<GraphResult<i32>>(8);

        let mut actual = Vec::with_capacity(7);

        block_on(async {
            (source.outlet(), calc.inlet()).connect().await;
            calc.outlet.attach(tx_out).await;

            let mut g = Graph::default();
            g.push_back(Box::new(source)).await;
            g.push_back(Box::new(calc)).await;
            g.run().await.expect("failed to close graph.");

            while let Some(x) = rx_out.recv().await {
                actual.push(x.expect("error during and_then mapping"));
            }
        });

        assert_eq!(vec![33, 42, 53, 66, 81, 98, 117], actual);
    }
}
