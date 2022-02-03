use std::fmt::{self, Debug};
use std::sync::Arc;

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use tokio::sync::Mutex;
use tokio::sync::{mpsc, oneshot};

use crate::error::StageError;
use crate::graph::shape::SinkShape;
use crate::graph::{stage, Inlet, Port, Stage, PORT_DATA};
use crate::{AppData, ProctorResult, SharedString};

pub type FoldApi<Acc> = mpsc::UnboundedSender<FoldCmd<Acc>>;

#[derive(Debug)]
pub enum FoldCmd<Acc> {
    GetAcc(oneshot::Sender<Acc>),
    GetAndReset(oneshot::Sender<Acc>),
}

impl<Acc> FoldCmd<Acc> {
    pub fn get_accumulation() -> (Self, oneshot::Receiver<Acc>) {
        let (tx, rx) = oneshot::channel();
        (Self::GetAcc(tx), rx)
    }

    pub fn get_and_reset_accumulation() -> (Self, oneshot::Receiver<Acc>) {
        let (tx, rx) = oneshot::channel();
        (Self::GetAndReset(tx), rx)
    }
}

/// A Sink that will invoke the given function for every received element, giving it its previous
/// output (or the given zero value) and the element as input.
///
/// The Fold sink is created along with a oneshot rx. The final function evaluation is sent to the
/// oneshot rx upon completion of stream processing.
///
/// # Examples
///
/// ```rust
/// use proctor::graph::stage::{self, Stage};
/// use proctor::graph::Inlet;
/// use proctor::graph::SinkShape;
/// use tokio::sync::mpsc;
/// use tokio::sync::oneshot::error::TryRecvError;
///
/// #[tokio::main]
/// async fn main() {
///     let my_data = vec![
///         "I am serious.".to_string(),
///         "And don't call me".to_string(),
///         "Shirley!".to_string(),
///     ];
///     let (tx, rx) = mpsc::channel::<String>(3);
///
///     let mut fold = stage::Fold::new("concatenate", "".to_string(), |acc, s: String| {
///         let mut result = if !acc.is_empty() { acc + " " } else { acc };
///         result + s.as_str()
///     });
///     let mut rx_sum = fold.take_final_rx().unwrap();
///
///     fold.inlet().attach("test_channel".into(), rx).await;
///
///     let sink_handle = tokio::spawn(async move {
///         fold.run().await;
///     });
///
///
///     match rx_sum.try_recv() {
///         Err(err) => assert_eq!(err, TryRecvError::Empty),
///         Ok(sum) => panic!("Not expecting string concatenation before sending: {}", sum),
///     }
///
///     let source_handle = tokio::spawn(async move {
///         for x in my_data {
///             tx.send(x).await.expect("failed to send data");
///         }
///     });
///
///     source_handle.await.unwrap();
///     sink_handle.await.unwrap();
///
///     match rx_sum.try_recv() {
///         Ok(sum) => assert_eq!("I am serious. And don't call me Shirley!", sum),
///         Err(err) => panic!("string sum not yet concatenated: {}", err),
///     }
/// }
/// ```
pub struct Fold<F, In, Acc>
where
    F: FnMut(Acc, In) -> Acc,
{
    name: SharedString,
    acc: Arc<Mutex<Acc>>,
    initial: Acc,
    operation: F,
    inlet: Inlet<In>,
    tx_api: FoldApi<Acc>,
    rx_api: mpsc::UnboundedReceiver<FoldCmd<Acc>>,
    tx_final: Option<oneshot::Sender<Acc>>,
    rx_final: Option<oneshot::Receiver<Acc>>,
}

impl<F, In, Acc> Fold<F, In, Acc>
where
    F: FnMut(Acc, In) -> Acc + Send,
    In: Debug + Send,
    Acc: Debug + Clone + Send + Sync,
{
    pub fn new<S: Into<SharedString>>(name: S, initial: Acc, operation: F) -> Self {
        let name = name.into();
        let inlet = Inlet::new(name.clone(), PORT_DATA);
        let (tx_api, rx_api) = mpsc::unbounded_channel();
        let (tx_final, rx_final) = oneshot::channel();

        Self {
            name,
            acc: Arc::new(Mutex::new(initial.clone())),
            initial,
            operation,
            inlet,
            tx_api,
            rx_api,
            tx_final: Some(tx_final),
            rx_final: Some(rx_final),
        }
    }

    #[inline]
    pub fn take_final_rx(&mut self) -> Option<oneshot::Receiver<Acc>> {
        self.rx_final.take()
    }

    #[tracing::instrument(level = "info", name = "do run fold sink", skip(self))]
    async fn do_run(&mut self) {
        let inlet = &mut self.inlet;
        let rx_api = &mut self.rx_api;
        let acc = Arc::clone(&self.acc);
        let initial = &self.initial;

        loop {
            let _timer = stage::start_stage_eval_time(self.name.as_ref());

            tracing::trace!("handling next item..");
            tokio::select! {
                input = inlet.recv() => match input {
                    Some(input) => {
                        let mut acc_2 = acc.lock().await;
                        tracing::trace!(?input, before_acc=?acc_2, "handling input");
                        *acc_2 = (self.operation)(acc_2.clone(), input);
                        tracing::trace!(after_acc=?acc_2, "folded input into accumulation");
                    },

                    None => {
                        tracing::trace!("Fold inlet dropped -- completing.");
                        break;
                    }
                },

                Some(cmd) = rx_api.recv() => Self::handle_command(cmd, Arc::clone(&acc), initial).await,

                else => {
                    tracing::trace!("fold done");
                    break;
                }
            }
        }
    }

    #[tracing::instrument(level = "info")]
    async fn handle_command(cmd: FoldCmd<Acc>, acc: Arc<Mutex<Acc>>, initial: &Acc) {
        match cmd {
            FoldCmd::GetAcc(tx) => {
                tracing::info!("handling request for current accumulation...");
                let resp = acc.lock().await;
                tracing::info!(accumulation=?resp,"sending accumulation to sender...");
                match tx.send(resp.clone()) {
                    Ok(_) => tracing::info!(accumulation=?resp, "sent accumulation"),
                    Err(resp) => tracing::warn!(accumulation=?resp, "failed to send accumulation"),
                }
            },

            FoldCmd::GetAndReset(tx) => {
                tracing::info!("handling command to reset accumulation...");
                let mut reset_accumulation = acc.lock().await;
                let resp = reset_accumulation.clone();
                *reset_accumulation = initial.clone();
                tracing::info!(accumulation=?resp, ?reset_accumulation, "sending accumulation before clearing.");
                match tx.send(resp.clone()) {
                    Ok(_) => tracing::info!(accumulation=?resp, "sent prior accumulation"),
                    Err(resp) => tracing::warn!(accumulation=?resp, "failed to send prior accumulation"),
                }
            },
        }
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn complete_fold(&mut self) -> Result<(), StageError> {
        if let Some(tx_final) = self.tx_final.take() {
            tx_final.send(self.acc.lock().await.clone()).map_err(|acc| {
                StageError::Materialization(format!(
                    "Fold sink final receiver detached. Failed to send accumulation: {:?}",
                    acc
                ))
            })?;
        }

        Ok(())
    }
}

impl<F, In, Acc> SinkShape for Fold<F, In, Acc>
where
    F: FnMut(Acc, In) -> Acc,
    Acc: Debug,
{
    type In = In;

    #[inline]
    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<F, In, Acc> Stage for Fold<F, In, Acc>
where
    F: FnMut(Acc, In) -> Acc + Send + Sync + 'static,
    In: AppData,
    Acc: AppData + Clone,
{
    #[inline]
    fn name(&self) -> SharedString {
        self.name.clone()
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.inlet.check_attachment().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run fold sink", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        self.do_run().await;
        self.complete_fold().await?;
        Ok(())
    }

    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        tracing::trace!("closing fold-sink inlet.");
        self.inlet.close().await;
        self.complete_fold().await?;
        self.rx_api.close();
        Ok(())
    }
}

impl<F, In, Acc> stage::WithApi for Fold<F, In, Acc>
where
    F: FnMut(Acc, In) -> Acc + Send + Sync + 'static,
{
    type Sender = FoldApi<Acc>;

    #[inline]
    fn tx_api(&self) -> Self::Sender {
        self.tx_api.clone()
    }
}

impl<F, In, Acc> fmt::Debug for Fold<F, In, Acc>
where
    F: FnMut(Acc, In) -> Acc,
    Acc: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Fold")
            .field("name", &self.name)
            .field("acc", &self.acc)
            .field("inlet", &self.inlet)
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
