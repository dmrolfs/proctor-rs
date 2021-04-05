use crate::graph::shape::{Shape, SinkShape, SourceShape, ThroughShape};
use crate::graph::{GraphResult, Inlet, Outlet, Port, Stage};
use crate::AppData;
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use std::fmt;

/// Filter the incoming elements using a predicate.
///
/// # Examples
///
/// ```rust
/// use tokio::sync::mpsc;
/// use proctor::graph::{Connect, Inlet};
/// use proctor::graph::stage::{self, Stage};
/// use proctor::graph::{ThroughShape, SinkShape, SourceShape};
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let my_data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
///     let (tx, rx) = mpsc::channel(8);
///
///     let mut filter = stage::Filter::new("even values", |x| x % 2 == 0);
///     let mut fold = stage::Fold::new("sum even values", 0, |acc, x| acc + x );
///     let mut rx_sum_sq = fold.take_final_rx().unwrap();
///
///     filter.inlet().attach(rx).await;
///     (filter.outlet(), fold.inlet()).connect().await;
///
///     let filter_handle = tokio::spawn(async move { filter.run().await; });
///     let fold_handle = tokio::spawn(async move { fold.run().await; });
///     let source_handle = tokio::spawn(async move {
///         for x in my_data { tx.send(x).await.expect("failed to send data"); }
///     });
///
///     source_handle.await?;
///     filter_handle.await?;
///     fold_handle.await?;
///
///     let actual = rx_sum_sq.await?;
///     assert_eq!(actual, 30);
///     Ok(())
/// }
/// ```
pub struct Filter<P, T>
where
    P: FnMut(&T) -> bool + Send + 'static,
    T: AppData,
{
    name: String,
    predicate: P,
    inlet: Inlet<T>,
    outlet: Outlet<T>,
    log_blocks: bool,
}

impl<P, T> Filter<P, T>
where
    P: FnMut(&T) -> bool + Send + 'static,
    T: AppData,
{
    pub fn new<S: Into<String>>(name: S, predicate: P) -> Self {
        let name = name.into();
        let inlet = Inlet::new(name.clone());
        let outlet = Outlet::new(name.clone());
        Self {
            name,
            predicate,
            inlet,
            outlet,
            log_blocks: false,
        }
    }

    pub fn with_block_logging(self) -> Self {
        Self { log_blocks: true, ..self }
    }
}

impl<P, T> Shape for Filter<P, T>
where
    P: FnMut(&T) -> bool + Send + 'static,
    T: AppData,
{
}

impl<P, T> ThroughShape for Filter<P, T>
where
    P: FnMut(&T) -> bool + Send + 'static,
    T: AppData,
{
}

impl<P, T> SourceShape for Filter<P, T>
where
    P: FnMut(&T) -> bool + Send + 'static,
    T: AppData,
{
    type Out = T;

    #[inline]
    fn outlet(&mut self) -> &mut Outlet<Self::Out> {
        &mut self.outlet
    }
}

impl<P, T> SinkShape for Filter<P, T>
where
    P: FnMut(&T) -> bool + Send + 'static,
    T: AppData,
{
    type In = T;

    #[inline]
    fn inlet(&mut self) -> &mut Inlet<Self::In> {
        &mut self.inlet
    }
}

#[dyn_upcast]
#[async_trait]
impl<P, T> Stage for Filter<P, T>
where
    P: FnMut(&T) -> bool + Send + 'static,
    T: AppData,
{
    #[inline]
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(level = "info", name = "run filter through", skip(self))]
    async fn run(&mut self) -> GraphResult<()> {
        let outlet = &self.outlet;
        while let Some(item) = self.inlet.recv().await {
            let filter_span = tracing::info_span!("filter on item", ?item);
            let _filter_span_guard = filter_span.enter();

            if (self.predicate)(&item) {
                outlet.send(item).await?;
            } else if self.log_blocks {
                tracing::error!(?item, "filter blocking item.");
            }
        }

        Ok(())
    }

    async fn close(mut self: Box<Self>) -> GraphResult<()> {
        tracing::trace!("closing filter-through ports.");
        self.inlet.close().await;
        self.outlet.close().await;
        Ok(())
    }
}

impl<P, T> fmt::Debug for Filter<P, T>
where
    P: FnMut(&T) -> bool + Send + 'static,
    T: AppData,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Filter")
            .field("name", &self.name)
            .field("inlet", &self.inlet)
            .field("outlet", &self.outlet)
            .finish()
    }
}

/////////////////////////////////////////////////////
// Unit Tests ///////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use tokio_test::block_on;

    #[test]
    fn test_basic_usage() {
        let my_data = vec![1, 2, 3, 4, 5];
        let (tx_in, rx_in) = mpsc::channel(8);
        let (tx_out, mut rx_out) = mpsc::channel(8);

        let mut filter = Filter::new("odd values", |x| x % 2 == 1);

        let mut actual = Vec::with_capacity(3);

        block_on(async {
            filter.inlet.attach(rx_in).await;
            filter.outlet.attach(tx_out).await;

            let filter_handle = tokio::spawn(async move {
                filter.run().await.expect("failed on filter run");
            });

            let source_handle = tokio::spawn(async move {
                for x in my_data {
                    tx_in.send(x).await.expect("failed to send data");
                }
            });

            source_handle.await.unwrap();
            filter_handle.await.unwrap();
            while let Some(x) = rx_out.recv().await {
                actual.push(x);
            }
        });

        assert_eq!(actual, vec![1, 3, 5]);
    }
}
