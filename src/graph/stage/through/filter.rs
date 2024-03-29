use std::fmt::{self, Debug};

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use tracing::Instrument;

use crate::graph::shape::{SinkShape, SourceShape};
use crate::graph::{Inlet, Outlet, Port, Stage, PORT_DATA};
use crate::{AppData, ProctorResult};

/// Filter the incoming elements using a predicate.
///
/// # Examples
///
/// ```rust
/// use proctor::graph::stage::{self, Stage};
/// use proctor::graph::{Connect, Inlet};
/// use proctor::graph::{SinkShape, SourceShape, ThroughShape};
/// use tokio::sync::mpsc;
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let my_data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
///     let (tx, rx) = mpsc::channel(8);
///
///     let mut filter = stage::Filter::new("even values", |x| x % 2 == 0);
///     let mut fold = stage::Fold::new("sum even values", 0, |acc, x| acc + x);
///     let mut rx_sum_sq = fold.take_final_rx().unwrap();
///
///     filter.inlet().attach("test_channel".into(), rx).await;
///     (filter.outlet(), fold.inlet()).connect().await;
///
///     let filter_handle = tokio::spawn(async move {
///         filter.run().await;
///     });
///     let fold_handle = tokio::spawn(async move {
///         fold.run().await;
///     });
///     let source_handle = tokio::spawn(async move {
///         for x in my_data {
///             tx.send(x).await.expect("failed to send data");
///         }
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
    P: FnMut(&T) -> bool,
{
    name: String,
    predicate: P,
    inlet: Inlet<T>,
    outlet: Outlet<T>,
    log_blocks: bool,
}

impl<P, T> Filter<P, T>
where
    P: FnMut(&T) -> bool,
{
    pub fn new<S: Into<String>>(name: S, predicate: P) -> Self {
        let name = name.into();
        let inlet = Inlet::new(name.clone(), PORT_DATA);
        let outlet = Outlet::new(name.clone(), PORT_DATA);
        Self { name, predicate, inlet, outlet, log_blocks: false }
    }

    #[allow(clippy::missing_const_for_fn)]
    pub fn with_block_logging(self) -> Self {
        Self { log_blocks: true, ..self }
    }
}

impl<P, T> SourceShape for Filter<P, T>
where
    P: FnMut(&T) -> bool,
{
    type Out = T;

    #[inline]
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

impl<P, T> SinkShape for Filter<P, T>
where
    P: FnMut(&T) -> bool,
{
    type In = T;

    #[inline]
    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<P, T> Stage for Filter<P, T>
where
    P: FnMut(&T) -> bool + Send + Sync + 'static,
    T: AppData,
{
    #[inline]
    fn name(&self) -> &str {
        &self.name
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.inlet.check_attachment().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", name = "run filter through", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        let outlet = &self.outlet;
        while let Some(item) = self.inlet.recv().await {
            let span = tracing::trace_span!("filter on item", ?item, stage=%self.name());
            let passed_filter = span.in_scope(|| (self.predicate)(&item));

            if passed_filter {
                outlet.send(item).instrument(span).await?;
            } else if self.log_blocks {
                span.in_scope(|| tracing::debug!("item did not pass filter"));
            }
        }

        Ok(())
    }

    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        tracing::trace!(stage=%self.name(), "closing filter-through ports.");
        self.inlet.close().await;
        self.outlet.close().await;
        Ok(())
    }
}

impl<P, T> Debug for Filter<P, T>
where
    P: FnMut(&T) -> bool,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
    use pretty_assertions::assert_eq;
    use tokio::sync::mpsc;
    use tokio_test::block_on;

    use super::*;

    #[test]
    fn test_basic_usage() {
        let my_data = vec![1, 2, 3, 4, 5];
        let (tx_in, rx_in) = mpsc::channel(8);
        let (tx_out, mut rx_out) = mpsc::channel(8);

        let mut filter = Filter::new("odd values", |x| x % 2 == 1);

        let mut actual = Vec::with_capacity(3);

        block_on(async {
            filter.inlet.attach("test_channel".into(), rx_in).await;
            filter.outlet.attach("test_channel".into(), tx_out).await;

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
