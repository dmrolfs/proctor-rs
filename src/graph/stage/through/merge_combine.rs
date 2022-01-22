use crate::error::ProctorError;
use crate::graph::stage::Stage;
use crate::graph::{stage, Inlet, InletsShape, Outlet, Port, SourceShape, UniformFanInShape, PORT_DATA};
use crate::{AppData, ProctorResult, SharedString};
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use frunk::{Monoid, Semigroup};
use futures_util::future::{self, BoxFuture, FutureExt};

#[derive(Debug)]
pub struct MergeCombine<T> {
    name: SharedString,
    inlets: InletsShape<T>,
    outlet: Outlet<T>,
}

impl<T: Send> MergeCombine<T> {
    pub fn new(name: impl Into<SharedString>, input_ports: usize) -> Self {
        let name = name.into();
        let outlet = Outlet::new(name.clone(), PORT_DATA);
        let inlets = InletsShape::new(
            (0..input_ports)
                .map(|pos| Inlet::new(name.clone(), format!("{PORT_DATA}_{pos}")))
                .collect(),
        );

        Self { name, inlets, outlet }
    }
}

impl<T> UniformFanInShape for MergeCombine<T> {
    type In = T;

    fn inlets(&self) -> InletsShape<Self::In> {
        self.inlets.clone()
    }
}

impl<T> SourceShape for MergeCombine<T> {
    type Out = T;

    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

type SourceItem<T> = (SharedString, Option<T>);
type ActivePull<'a, T> = BoxFuture<'a, SourceItem<T>>;

#[dyn_upcast]
#[async_trait]
impl<T> Stage for MergeCombine<T>
where
    T: AppData + Monoid,
{
    fn name(&self) -> SharedString {
        self.name.clone()
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        for inlet in self.inlets.0.lock().await.iter() {
            inlet.check_attachment().await?;
        }
        self.outlet.check_attachment().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run merge combine", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        loop {
            let active = self.start_batch().await;
            if active.is_empty() {
                tracing::info!("empty merge_combine start batch - stopping");
                break;
            }

            if let Err(err) = self.outlet.reserve_send(self.complete_batch(active)).await {
                tracing::error!(error=?err, "failed to send merge_combine:{} batch to outlet:{}", self.name, self.outlet.full_name());
                return Err(err);
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "info", name = "close MergeCombine through", skip(self))]
    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        self.inlets.close().await;
        self.outlet.close().await;
        Ok(())
    }
}

impl<'a, T> MergeCombine<T>
where
    T: AppData + Monoid,
{
    #[tracing::instrument(level = "trace", skip(self))]
    async fn start_batch(&self) -> Vec<ActivePull<'a, T>> {
        let inlets = self.inlets.0.lock().await;
        let mut active = Vec::with_capacity(inlets.len());

        for inlet in inlets.iter().cloned() {
            if inlet.is_attached().await {
                let pulled_item = Self::replenish_pull(inlet).boxed();
                active.push(pulled_item);
            }
        }

        active
    }

    #[tracing::instrument(level="trace", skip(inlet), fields(inlet_name=%inlet.full_name()))]
    async fn replenish_pull(mut inlet: Inlet<T>) -> SourceItem<T> {
        (inlet.full_name().into(), inlet.recv().await)
    }

    #[tracing::instrument(level = "info", skip(self, active), fields(nr_active=%active.len()))]
    async fn complete_batch(&self, mut active: Vec<ActivePull<'a, T>>) -> Result<T, ProctorError> {
        let _timer = stage::start_stage_eval_time(self.name.as_ref());

        let mut acc: Option<T> = None;
        let nr_pulls = active.len();

        while !active.is_empty() {
            let ((item_source, batch_item), pos, remaining) = future::select_all(active).await;
            acc = acc.combine(&batch_item);

            tracing::info!(
                ?acc, ?batch_item,
                %item_source,
                remaining_pos=%pos,
                nr_remaining=%format!("{}/{nr_pulls}", remaining.len()),
                "combine pulled item into batch."
            );

            active = remaining;
        }

        Ok(acc.unwrap_or_else(T::empty))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::stage::{ActorSourceApi, ActorSourceCmd, WithApi};
    use crate::graph::{Connect, Graph, SinkShape, SourceShape};
    use crate::Ack;
    use claim::*;
    use pretty_assertions::assert_eq;
    use std::fmt::Debug;
    use tokio_test::block_on;

    async fn push_source<T>(tx: &ActorSourceApi<T>, value: T) -> anyhow::Result<Ack>
    where
        T: 'static + Debug + Send + Sync,
    {
        let (cmd, rx) = ActorSourceCmd::push(value);
        tx.send(cmd)?;
        rx.await.map_err(|err| err.into())
    }

    async fn stop_source<T>(tx: &ActorSourceApi<T>) -> anyhow::Result<Ack>
    where
        T: 'static + Debug + Send + Sync,
    {
        let (cmd, rx) = ActorSourceCmd::stop();
        tx.send(cmd)?;
        rx.await.map_err(|err| err.into())
    }

    #[test]
    fn test_merge_combine_3() {
        once_cell::sync::Lazy::force(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_merge_combine_5");
        let _main_span_guard = main_span.enter();

        block_on(async {
            let src_a = stage::ActorSource::new("AAA");
            let tx_a = src_a.tx_api();

            let src_b = stage::ActorSource::new("BBB");
            let tx_b = src_b.tx_api();

            let src_c = stage::ActorSource::new("CCC");
            let tx_c = src_c.tx_api();

            let merge = MergeCombine::new("merge", 3);

            let merge_inlets = merge.inlets();
            (src_a.outlet(), assert_some!(merge_inlets.get(0).await)).connect().await;
            (src_b.outlet(), assert_some!(merge_inlets.get(1).await)).connect().await;
            (src_c.outlet(), assert_some!(merge_inlets.get(2).await)).connect().await;

            let mut gather = stage::Fold::new("sums", vec![], |mut acc, x| {
                acc.push(x);
                acc
            });
            let rx_sums = gather.take_final_rx().unwrap();
            (merge.outlet(), gather.inlet()).connect().await;

            let mut g = Graph::default();
            g.push_back(Box::new(src_a)).await;
            g.push_back(Box::new(src_b)).await;
            g.push_back(Box::new(src_c)).await;
            g.push_back(Box::new(merge)).await;
            g.push_back(Box::new(gather)).await;
            let run_handle = tokio::spawn(async move { assert_ok!(g.run().await) });

            assert_ok!(push_source(&tx_b, 2).await);
            assert_ok!(push_source(&tx_a, 1).await);
            assert_ok!(push_source(&tx_a, 10).await);
            assert_ok!(push_source(&tx_c, 3).await);
            assert_ok!(push_source(&tx_c, 30).await);
            assert_ok!(push_source(&tx_c, 300).await);
            assert_ok!(push_source(&tx_a, 100).await);
            assert_ok!(push_source(&tx_b, 20).await);
            assert_ok!(push_source(&tx_a, 1_000_000).await);
            assert_ok!(push_source(&tx_b, 200).await);

            assert_ok!(stop_source(&tx_b).await);
            assert_ok!(stop_source(&tx_c).await);
            assert_ok!(stop_source(&tx_a).await);

            assert_ok!(run_handle.await);
            let actual = assert_ok!(rx_sums.await);
            assert_eq!(actual, vec![1 + 2 + 3, 10 + 20 + 30, 100 + 200 + 300, 1_000_000, 0,]);
        });
    }
}
