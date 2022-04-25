use std::fmt::{self, Debug};

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use futures::future::{self, BoxFuture, FutureExt};
use tokio::sync::{mpsc, oneshot};
use tracing_futures::Instrument;

use crate::error::StageError;
use crate::graph::{stage, Inlet, InletsShape, Outlet, Port, SourceShape, Stage, UniformFanInShape, PORT_DATA};
use crate::{Ack, AppData, ProctorResult};

pub type MergeApi = mpsc::UnboundedSender<MergeCmd>;

#[derive(Debug)]
pub enum MergeCmd {
    Stop {
        tx: oneshot::Sender<Result<(), StageError>>,
    },
}

impl MergeCmd {
    const STAGE_NAME: &'static str = "merge_n";

    pub async fn stop(api: &MergeApi) -> Result<Ack, StageError> {
        let (tx, rx) = oneshot::channel();
        api.send(Self::Stop { tx })
            .map_err(|err| StageError::Api(Self::STAGE_NAME.to_string(), err.into()))?;
        rx.await
            .map_err(|err| StageError::Api(Self::STAGE_NAME.to_string(), err.into()))?
    }
}

/// MergeN multiple sources. Picks elements randomly if all sources has elements ready.
///
/// # Examples
///
/// ```rust
/// use rand::Rng;
/// use proctor::tracing::{get_subscriber, init_subscriber};
/// use proctor::graph::stage::{WithApi, MergeN, MergeCmd, Stage};
/// use std::fmt;
/// use std::time::Duration;
/// use tokio::sync::{mpsc, oneshot, Mutex};
/// use tokio::task::JoinHandle;
/// use std::sync::Arc;
/// use proctor::graph::{UniformFanInShape, SourceShape};
///
/// #[tokio::main]
/// async fn main() {
///     let subscriber = get_subscriber("merge_n doc test", "trace", std::io::stdout);
///     init_subscriber(subscriber);
///
///     let main_span = tracing::info_span!("main");
///     let _main_span_guard = main_span.enter();
///
///     let (tx_0, rx_0) = mpsc::channel(8);
///     let (tx_1, rx_1) = mpsc::channel(8);
///     let (tx_2, rx_2) = mpsc::channel(8);
///     let mut merge_inlets = vec![rx_0, rx_1, rx_2];
///
///     let mut merge = MergeN::new("merge", merge_inlets.len());
///     let tx_merge_api = merge.tx_api();
///
///     for idx in 0..merge_inlets.len() {
///         if let Some(rx_inlet) = merge_inlets.pop() {
///             if let Some(mut inlet) = merge.inlets().get(idx).await {
///                 inlet.attach("src_channel".into(), rx_inlet).await;
///             }
///         }
///     }
///
///     let (tx_merge, mut rx_merge) = mpsc::channel(8);
///     merge.outlet().attach("merge_channel".into(), tx_merge).await;
///
///     let m = tokio::spawn(async move {
///         merge.run().await;
///     });
///
///     let stop = tokio::spawn(async move {
///         tokio::time::sleep(Duration::from_millis(500)).await;
///         let (tx, rx) = oneshot::channel();
///         tx_merge_api.send(MergeCmd::Stop { tx }).expect("failed to send stop to merge");
///         let _ = rx.await;
///         tracing::warn!("STOPPED MERGE");
///     });
///
///     let h0 = spawn_transmission("ONES", 1..9, tx_0);
///     let h1 = spawn_transmission("TENS", 11..99, tx_1);
///     let h2 = spawn_transmission("HUNDREDS", 101..=999, tx_2);
///
///     let count = Arc::new(Mutex::new(0));
///     let r_count = Arc::clone(&count);
///     let r = tokio::spawn(async move {
///         while let Some(item) = rx_merge.recv().await {
///             let mut tally = r_count.lock().await;
///             *tally += 1;
///             tracing::info!(%item, nr_received=%tally, "Received item");
///         }
///     });
///
///     h0.await.unwrap();
///     h1.await.unwrap();
///     h2.await.unwrap();
///
///     m.await.unwrap();
///     r.await.unwrap();
///     stop.await.unwrap();
///
///     let tally: i32 = *count.lock().await;
///     tracing::info!(%tally, "Done!");
///     assert!(5 < tally);
///     assert!(tally < 17);
/// }
///
/// fn spawn_transmission<S, I, T>(name: S, data: I, tx: mpsc::Sender<T>) -> JoinHandle<()>
/// where
///     T: fmt::Debug + Send + 'static,
///     S: AsRef<str> + Send + 'static,
///     I: IntoIterator<Item = T> + Send + 'static,
///     <I as IntoIterator>::IntoIter: Send,
/// {
///     tokio::spawn(async move {
///         for item in data.into_iter() {
///             let delay = Duration::from_millis(rand::thread_rng().gen_range(25..=250));
///             tokio::time::sleep(delay).await;
///
///             let send_span = tracing::info_span!(
///                 "introducing item to channel",
///                 ?item,
///                 ?delay,
///                 channel=%name.as_ref()
///             );
///             let _send_span_guard = send_span.enter();
///
///             match tx.send(item).await {
///                 Ok(_) => tracing::info!("successfully introduced item to channel."),
///                 Err(err) => {
///                     tracing::error!(error=?err, "failed to introduce item to channel.");
///                     break;
///                 }
///             };
///         }
///     })
/// }
/// ```
pub struct MergeN<T> {
    name: String,
    inlets: InletsShape<T>,
    outlet: Outlet<T>,
    tx_api: MergeApi,
    rx_api: mpsc::UnboundedReceiver<MergeCmd>,
}

impl<T: Send> MergeN<T> {
    pub fn new(name: impl Into<String>, input_ports: usize) -> Self {
        let name = name.into();
        let outlet = Outlet::new(name.clone(), PORT_DATA);
        let (tx_api, rx_api) = mpsc::unbounded_channel();
        let inlets = InletsShape::new(
            (0..input_ports)
                .map(|pos| Inlet::new(name.clone(), format!("{}_{}", PORT_DATA, pos)))
                .collect(),
        );

        Self { name, inlets, outlet, tx_api, rx_api }
    }
}

impl<T> UniformFanInShape for MergeN<T> {
    type In = T;

    fn inlets(&self) -> InletsShape<T> {
        self.inlets.clone()
    }
}

impl<T> SourceShape for MergeN<T> {
    type Out = T;

    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<T: AppData> Stage for MergeN<T> {
    fn name(&self) -> &str {
        &self.name
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        for inlet in self.inlets.0.lock().await.iter() {
            inlet.check_attachment().await?;
        }
        self.outlet.check_attachment().await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", name = "run merge through", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        let mut active_inlets = Self::initialize_active_inlets(self.inlets()).await;
        let outlet = &self.outlet;
        let inlets = self.inlets.clone();
        let rx_api = &mut self.rx_api;

        while !active_inlets.is_empty() {
            let _timer = stage::start_stage_eval_time(self.name.as_ref());
            let available_inlets = active_inlets;
            tracing::debug!(nr_available_inlets=%available_inlets.len(), "selecting from active inlets");

            tokio::select! {
                ((inlet_idx, value), target_idx, remaining) = future::select_all(available_inlets) => {
                    let remaining_inlets = Self::handle_selected_pull(
                        value,
                        inlet_idx,
                        target_idx,
                        remaining,
                        inlets.clone(),
                        outlet
                    ).await;

                    match remaining_inlets {
                        Ok(remaining_inlets) => active_inlets = remaining_inlets,
                        Err(err) => {
                            tracing::warn!(stage=%self.name(), error=?err, "failed in handling selected pull - stopping MergeN");
                            break;
                        },
                    }
                },

                Some(msg) = rx_api.recv() => match msg {
                    MergeCmd::Stop { tx } => {
                        tracing::trace!("handling request to stop MergeN.");
                        let _ = tx.send(Ok(()));
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", name = "close MergeN through", skip(self))]
    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        self.inlets.close().await;
        self.outlet.close().await;
        Ok(())
    }
}

impl<'a, T: AppData> MergeN<T> {
    #[tracing::instrument(level = "trace", skip(inlets))]
    async fn initialize_active_inlets(inlets: InletsShape<T>) -> Vec<BoxFuture<'a, (usize, Option<T>)>> {
        let inlets = inlets.0.lock().await;
        let mut active_inlets = Vec::with_capacity(inlets.len());

        for (idx, inlet) in inlets.iter().enumerate() {
            if inlet.is_attached().await {
                let rep = Self::replenish_inlet_pull(idx, inlet.clone()).boxed();
                active_inlets.push(rep);
            }
        }

        active_inlets
    }

    #[tracing::instrument(level="trace", skip(inlet), fields(inlet_idx=%idx))]
    async fn replenish_inlet_pull(idx: usize, mut inlet: Inlet<T>) -> (usize, Option<T>) {
        (idx, inlet.recv().await)
    }

    #[tracing::instrument(
        level="trace",
        skip(remaining, inlets, outlet),
        fields(nr_remaining=%remaining.len(),),
    )]
    async fn handle_selected_pull(
        value: Option<T>, inlet_idx: usize, pulled_idx: usize, remaining: Vec<BoxFuture<'a, (usize, Option<T>)>>,
        inlets: InletsShape<T>, outlet: &Outlet<T>,
    ) -> Result<Vec<BoxFuture<'a, (usize, Option<T>)>>, StageError> {
        let mut remaining_inlets = remaining;
        let is_active = value.is_some();

        if let Some(item) = value {
            let span = tracing::trace_span!("3.send item via outlet", ?item);
            let _ = outlet.send(item).instrument(span).await?;
        }

        tracing::trace!(nr_remaining=%remaining_inlets.len(), %is_active, "after send");

        if is_active {
            let run_active_span = tracing::trace_span!(
                "4.replenish active pulls",
                nr_available_inlets=%remaining_inlets.len()
            );
            let inlet = inlets.get(inlet_idx).instrument(run_active_span.clone()).await;

            if let Some(inlet) = inlet {
                run_active_span.in_scope(|| {
                    let rep = Self::replenish_inlet_pull(inlet_idx, inlet).boxed();
                    remaining_inlets.push(rep);
                    tracing::trace!(nr_available_inlets=%remaining_inlets.len(), "4.1.active_inlets replenished.");
                })
            }
        }

        Ok(remaining_inlets)
    }
}

impl<T> stage::WithApi for MergeN<T> {
    type Sender = MergeApi;

    fn tx_api(&self) -> Self::Sender {
        self.tx_api.clone()
    }
}

impl<T> Debug for MergeN<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MergeN")
            .field("name", &self.name)
            .field("inlets", &self.inlets)
            .field("outlet", &self.outlet)
            .finish()
    }
}
