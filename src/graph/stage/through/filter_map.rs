use crate::graph::{Inlet, Outlet, Port, Stage};
use crate::graph::{SinkShape, SourceShape};
use crate::{AppData, ProctorResult};
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use std::fmt::{self, Debug};

/// The FilterMap stage both filters and maps on items.
///
/// The stage passes only the values for which the supplied closure returns Some(value).
///
/// FilterMap can be used to make chains of filter and map more concise.
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
///     let mut filter_map = stage::FilterMap::new(
///         "even values",
///         |x| {
///             if x % 2 == 0 {
///                 Some(x * x)
///             } else {
///                 None
///             }
///         }
///     );
///
///     let mut fold = stage::Fold::new("sum even sq values", 0, |acc, x| acc + x );
///     let mut rx_sum_sq = fold.take_final_rx().unwrap();
///
///     filter_map.inlet().attach("test_channel", rx).await;
///     (filter_map.outlet(), fold.inlet()).connect().await;
///
///     let filter_handle = tokio::spawn(async move { filter_map.run().await; });
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
///     assert_eq!(actual, 220);
///     Ok(())
/// }
/// ```
pub struct FilterMap<F, In, Out>
where
    F: FnMut(In) -> Option<Out>,
{
    name: String,
    filter_map: F,
    inlet: Inlet<In>,
    outlet: Outlet<Out>,
    log_blocks: bool,
}

impl<F, In, Out> FilterMap<F, In, Out>
where
    F: FnMut(In) -> Option<Out>,
{
    pub fn new<S: Into<String>>(name: S, f: F) -> Self {
        let name = name.into();
        let inlet = Inlet::new(name.clone());
        let outlet = Outlet::new(name.clone());
        Self {
            name,
            filter_map: f,
            inlet,
            outlet,
            log_blocks: false,
        }
    }

    pub fn with_block_logging(self) -> Self {
        Self {
            log_blocks: true,
            ..self
        }
    }
}

impl<F, In, Out> SourceShape for FilterMap<F, In, Out>
where
    F: FnMut(In) -> Option<Out>,
{
    type Out = Out;

    #[inline]
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

impl<F, In, Out> SinkShape for FilterMap<F, In, Out>
where
    F: FnMut(In) -> Option<Out>,
{
    type In = In;

    #[inline]
    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<F, In, Out> Stage for FilterMap<F, In, Out>
where
    F: FnMut(In) -> Option<Out> + Send + Sync + 'static,
    In: AppData,
    Out: AppData,
{
    #[inline]
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.inlet.check_attachment().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run filter_map through", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        let outlet = &self.outlet;
        while let Some(item) = self.inlet.recv().await {
            let filter_span = tracing::info_span!("filter on item", ?item);
            let _filter_span_guard = filter_span.enter();
            if let Some(value) = (self.filter_map)(item) {
                outlet.send(value).await?;
            } else if self.log_blocks {
                tracing::error!("filter_map blocking item.");
            }
        }

        Ok(())
    }

    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        tracing::trace!("closing filter_map-through ports.");
        self.inlet.close().await;
        self.outlet.close().await;
        Ok(())
    }
}

impl<F, In, Out> Debug for FilterMap<F, In, Out>
where
    F: FnMut(In) -> Option<Out>,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FilterMap")
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

        let mut filter_map = FilterMap::new("odd values", |x| if x % 2 == 1 { Some(x * x) } else { None });

        let mut actual = Vec::with_capacity(3);

        block_on(async {
            filter_map.inlet.attach("test_channel", rx_in).await;
            filter_map.outlet.attach("test_channel", tx_out).await;

            let filter_handle = tokio::spawn(async move {
                filter_map.run().await.expect("failed on filter_map run");
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

        assert_eq!(actual, vec![1, 9, 25]);
    }
}
