use std::fmt::{self, Debug};

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use tracing::Instrument;

use crate::graph::{stage, Inlet, Outlet, Port, Stage, PORT_DATA};
use crate::graph::{SinkShape, SourceShape};
use crate::{AppData, ProctorResult};

/// The FilterMap stage both filters and maps on items.
///
/// The stage passes only the values for which the supplied closure returns Some(value), and None
/// values are not passed through; i.e., blocked.
///
/// FilterMap can be used to make chains of filter and map more concise.
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
///     let mut filter_map = stage::FilterMap::new("even values", |x| if x % 2 == 0 { Some(x * x) } else { None });
///
///     let mut fold = stage::Fold::new("sum even sq values", 0, |acc, x| acc + x);
///     let mut rx_sum_sq = fold.take_final_rx().unwrap();
///
///     filter_map.inlet().attach("test_channel".into(), rx).await;
///     (filter_map.outlet(), fold.inlet()).connect().await;
///
///     let filter_handle = tokio::spawn(async move {
///         filter_map.run().await;
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
    pub fn new(name: impl Into<String>, f: F) -> Self {
        let name = name.into();
        let inlet = Inlet::new(name.clone(), PORT_DATA);
        let outlet = Outlet::new(name.clone(), PORT_DATA);
        Self {
            name,
            filter_map: f,
            inlet,
            outlet,
            log_blocks: false,
        }
    }

    pub fn with_block_logging(self) -> Self {
        Self { log_blocks: true, ..self }
    }
}

impl<F, In, Out> SourceShape for FilterMap<F, In, Out>
where
    F: FnMut(In) -> Option<Out>,
{
    type Out = Out;

    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

impl<F, In, Out> SinkShape for FilterMap<F, In, Out>
where
    F: FnMut(In) -> Option<Out>,
{
    type In = In;

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
    fn name(&self) -> &str {
        &self.name
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.inlet.check_attachment().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", name = "run filter_map through", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        let outlet = &self.outlet;
        while let Some(item) = self.inlet.recv().await {
            let span = tracing::debug_span!("filter on item", ?item, stage=%self.name());
            let _timer = stage::start_stage_eval_time(self.name.as_ref());
            let filter_passed = span.in_scope(|| (self.filter_map)(item));

            if let Some(value) = filter_passed {
                outlet.send(value).instrument(span).await?;
            } else if self.log_blocks {
                span.in_scope(|| tracing::debug!("{} blocking item.", self.name));
            }
        }

        Ok(())
    }

    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        tracing::trace!(stage=%self.name(), "closing filter_map-through ports.");
        self.inlet.close().await;
        self.outlet.close().await;
        Ok(())
    }
}

impl<F, In, Out> Debug for FilterMap<F, In, Out>
where
    F: FnMut(In) -> Option<Out>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
    use pretty_assertions::assert_eq;
    use tokio::sync::mpsc;
    use tokio_test::block_on;

    use super::*;

    #[test]
    fn test_basic_usage() {
        let my_data = vec![1, 2, 3, 4, 5];
        let (tx_in, rx_in) = mpsc::channel(8);
        let (tx_out, mut rx_out) = mpsc::channel(8);

        let mut filter_map = FilterMap::new("odd values", |x| if x % 2 == 1 { Some(x * x) } else { None });

        let mut actual = Vec::with_capacity(3);

        block_on(async {
            filter_map.inlet.attach("test_channel".into(), rx_in).await;
            filter_map.outlet.attach("test_channel".into(), tx_out).await;

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
