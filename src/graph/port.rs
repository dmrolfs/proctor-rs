use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use once_cell::sync::Lazy;
use prometheus::{IntCounterVec, Opts};
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tracing::Instrument;

use crate::error::PortError;
use crate::AppData;

pub static STAGE_INGRESS_COUNTS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "proctor_stage_ingress_counts",
            "Number of items entering a stage via an Inlet",
        )
        .const_labels(crate::metrics::CONST_LABELS.clone()),
        &["stage", "port"],
    )
    .expect("failed creating proctor_stage_ingress_counts metric")
});

pub static STAGE_EGRESS_COUNTS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "proctor_stage_egress_counts",
            "Number of items exiting a stage via an Outlet",
        )
        .const_labels(crate::metrics::CONST_LABELS.clone()),
        &["stage", "port"],
    )
    .expect("failed creating proctor_stage_ingress_counts metric")
});

#[inline]
fn track_ingress(stage: &str, port_name: &str) {
    STAGE_INGRESS_COUNTS.with_label_values(&[stage, port_name]).inc()
}

#[inline]
fn track_egress(stage: &str, port_name: &str) {
    STAGE_EGRESS_COUNTS.with_label_values(&[stage, port_name]).inc()
}

pub const PORT_DATA: &str = "data";
pub const PORT_CONTEXT: &str = "context";

#[async_trait]
pub trait Port {
    fn stage(&self) -> &str;
    fn name(&self) -> &str;
    fn full_name(&self) -> String {
        format!("{}::{}", self.stage(), self.name())
    }

    /// Closes this half of a channel without dropping it.
    /// This prevents any further messages from being sent on the port while still enabling the
    /// receiver to drain messages that are buffered. Any outstanding Permit values will still be
    /// able to send messages.
    /// To guarantee that no messages are dropped, after calling close(), recv() must be called
    /// until None is returned. If there are outstanding Permit values, the recv method will not
    /// return None until those are released
    async fn close(&mut self);
}

#[async_trait]
pub trait Connect<T> {
    async fn connect(self);
}

#[async_trait]
impl<T: AppData> Connect<T> for (Outlet<T>, Inlet<T>) {
    async fn connect(mut self) {
        let outlet = self.0;
        let inlet = self.1;
        connect_out_to_in(outlet, inlet).await
    }
}

#[async_trait]
impl<T: AppData> Connect<T> for (&Outlet<T>, &Inlet<T>) {
    async fn connect(mut self) {
        let outlet = self.0.clone();
        let inlet = self.1.clone();
        connect_out_to_in(outlet, inlet).await
    }
}

#[async_trait]
impl<T: AppData> Connect<T> for (Inlet<T>, Outlet<T>) {
    async fn connect(mut self) {
        let outlet = self.1;
        let inlet = self.0;
        connect_out_to_in(outlet, inlet).await
    }
}

#[async_trait]
impl<T: AppData> Connect<T> for (&Inlet<T>, &Outlet<T>) {
    async fn connect(mut self) {
        let outlet = self.1.clone();
        let inlet = self.0.clone();
        connect_out_to_in(outlet, inlet).await
    }
}

pub async fn connect_out_to_in<T: AppData>(mut lhs: Outlet<T>, mut rhs: Inlet<T>) {
    let (tx, rx) = mpsc::channel(num_cpus::get());
    lhs.attach(&rhs.full_name(), tx).await;
    rhs.attach(&lhs.full_name(), rx).await;
}

pub type InletConnection<T> = Arc<Mutex<Option<(mpsc::Receiver<T>, String)>>>;

pub struct Inlet<T> {
    pub stage: String,
    pub name: String,
    connection: InletConnection<T>,
}

impl<T> Inlet<T> {
    pub fn new(stage: impl Into<String>, port_name: impl Into<String>) -> Self {
        Self {
            stage: stage.into(),
            name: port_name.into(),
            connection: Arc::new(Mutex::new(None)),
        }
    }

    pub fn with_receiver(
        stage: impl Into<String>, port_name: impl Into<String>, receiver_name: impl Into<String>, rx: mpsc::Receiver<T>,
    ) -> Self {
        Self {
            stage: stage.into(),
            name: port_name.into(),
            connection: Arc::new(Mutex::new(Some((rx, receiver_name.into())))),
        }
    }
}

#[async_trait]
impl<T: Send> Port for Inlet<T> {
    fn stage(&self) -> &str {
        &self.stage
    }

    fn name(&self) -> &str {
        &self.name
    }

    async fn close(&mut self) {
        let mut rx = self.connection.lock().await;
        match rx.as_mut() {
            Some(r) => {
                tracing::trace!(stage=%self.stage, inlet=%self.name, "closing Inlet");
                r.0.close()
            },
            None => {
                tracing::trace!(stage=%self.stage, inlet=%self.name, "Inlet close ignored - not attached");
            },
        }
    }
}

impl<T> Clone for Inlet<T> {
    fn clone(&self) -> Self {
        Self {
            stage: self.stage.clone(),
            name: self.name.clone(),
            connection: self.connection.clone(),
        }
    }
}

impl<T: fmt::Debug + Send> Inlet<T> {
    pub async fn attach(&mut self, sender_name: &str, rx: mpsc::Receiver<T>) {
        let mut port = self.connection.lock().await;
        *port = Some((rx, sender_name.to_string()));
    }

    pub async fn is_attached(&self) -> bool {
        self.connection.lock().await.is_some()
    }

    pub async fn check_attachment(&self) -> Result<(), PortError> {
        if self.is_attached().await {
            let sender = self.connection.lock().await.as_ref().map(|s| s.1.clone()).unwrap();
            tracing::trace!("inlet connected: {} -> {}", sender, self.full_name());
            Ok(())
        } else {
            Err(PortError::Detached(format!(
                "{}[{}]",
                self.full_name(),
                std::any::type_name::<Self>()
            )))
        }
    }

    /// Receives the next value for this port.
    ///
    /// `None` is returned when all `Sender` halves have dropped, indicating
    /// that no further values can be sent on the channel.
    ///
    /// # Panics
    ///
    /// This function panics if called before attaching to the `Inlet`.
    ///
    /// # Examples
    ///
    /// ```
    /// use proctor::graph::Inlet;
    /// use tokio::sync::mpsc;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let (tx, mut rx) = mpsc::channel(100);
    ///     let mut port = Inlet::new("port", "data");
    ///     port.attach("test_channel".into(), rx).await;
    ///
    ///     tokio::spawn(async move {
    ///         tx.send("hello").await.unwrap();
    ///     });
    ///
    ///     assert_eq!(Some("hello"), port.recv().await);
    ///     assert_eq!(None, port.recv().await);
    /// }
    /// ```
    ///
    /// Values are buffered:
    ///
    /// ```
    /// use proctor::graph::Inlet;
    /// use tokio::sync::mpsc;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let (tx, mut rx) = mpsc::channel(100);
    ///     let mut port = Inlet::new("port", "data");
    ///     port.attach("test_channel".into(), rx).await;
    ///
    ///     tx.send("hello").await.unwrap();
    ///     tx.send("world").await.unwrap();
    ///
    ///     assert_eq!(Some("hello"), port.recv().await);
    ///     assert_eq!(Some("world"), port.recv().await);
    /// }
    /// ```
    // #[tracing::instrument(level = "trace", skip(self), fields(inlet=%self.0))]
    pub async fn recv(&mut self) -> Option<T> {
        if self.is_attached().await {
            let mut rx = self.connection.lock().await;
            tracing::trace!(stage=%self.stage, inlet=%self.name, "Inlet awaiting next item...");
            let item = (*rx).as_mut()?.0.recv().await;
            tracing::trace!(stage=%self.stage, inlet=%self.name, ?item, "Inlet received {} item.", if item.is_some() {"an"} else { "no"});
            if item.is_none() && rx.is_some() {
                tracing::warn!(stage=%self.stage, inlet=%self.name, "Inlet depleted - closing receiver");
                rx.as_mut().unwrap().0.close();
                let _ = rx.take();
            }

            track_ingress(self.stage(), self.name());
            item
        } else {
            None
        }
    }
}

impl<T> fmt::Debug for Inlet<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // copy Port::full_name to avoid stricter bounds on T
        f.debug_tuple("Inlet")
            .field(&format!("{}::{}", self.stage, self.name))
            .finish()
    }
}

pub type OutletConnection<T> = Arc<Mutex<Option<(mpsc::Sender<T>, String)>>>;

pub struct Outlet<T> {
    pub stage: String,
    pub name: String,
    connection: OutletConnection<T>,
}

impl<T> Outlet<T> {
    pub fn new(stage: impl Into<String>, port_name: impl Into<String>) -> Self {
        Self {
            stage: stage.into(),
            name: port_name.into(),
            connection: Arc::new(Mutex::new(None)),
        }
    }

    pub fn with_receiver(
        stage: impl Into<String>, port_name: impl Into<String>, receiver_name: impl Into<String>, tx: mpsc::Sender<T>,
    ) -> Self {
        Self {
            stage: stage.into(),
            name: port_name.into(),
            connection: Arc::new(Mutex::new(Some((tx, receiver_name.into())))),
        }
    }
}

#[async_trait]
impl<T: Send> Port for Outlet<T> {
    fn stage(&self) -> &str {
        &self.stage
    }

    fn name(&self) -> &str {
        &self.name
    }

    async fn close(&mut self) {
        tracing::trace!(inlet=%self.name(), "closing Outlet");
        self.connection.lock().await.take();
    }
}

impl<T> Clone for Outlet<T> {
    fn clone(&self) -> Self {
        Self {
            stage: self.stage.clone(),
            name: self.name.clone(),
            connection: self.connection.clone(),
        }
    }
}

impl<T: AppData> Outlet<T> {
    pub async fn attach(&mut self, sender_name: &str, tx: mpsc::Sender<T>) {
        let mut port = self.connection.lock().await;
        *port = Some((tx, sender_name.to_string()));
    }

    pub async fn is_attached(&self) -> bool {
        self.connection.lock().await.is_some()
    }

    pub async fn check_attachment(&self) -> Result<(), PortError> {
        if self.is_attached().await {
            let receiver = self.connection.lock().await.as_ref().map(|s| s.1.clone()).unwrap();
            tracing::trace!("outlet connected: {} -> {}", self.full_name(), receiver);
            Ok(())
        } else {
            Err(PortError::Detached(format!(
                "{}[{}]",
                self.full_name(),
                std::any::type_name::<Self>()
            )))
        }
    }

    /// Sends a value, waiting until there is capacity.
    ///
    /// A successful send occurs when it is determined that the other end of the
    /// port has not hung up already. An unsuccessful send would be one where
    /// the corresponding receiver has already been closed. Note that a return
    /// value of `Err` means that the data will never be received, but a return
    /// value of `Ok` does not mean that the data will be received. It is
    /// possible for the corresponding receiver to hang up immediately after
    /// this function returns `Ok`.
    ///
    /// # Errors
    ///
    /// If the receive half of the channel is closed, either due to [`close`]
    /// being called or the [`Receiver`] handle dropping, the function returns
    /// an error. The error includes the value passed to `send`.
    ///
    /// [`close`]: Receiver::close
    /// [`Receiver`]: Receiver
    ///
    /// # Examples
    ///
    /// In the following example, each call to `send` will block until the
    /// previously sent value was received.
    ///
    /// ```rust
    /// use proctor::graph::Outlet;
    /// use tokio::sync::mpsc;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let (tx, mut rx) = mpsc::channel(1);
    ///     let mut port = Outlet::new("port", "data");
    ///     port.attach("test_channel".into(), tx).await;
    ///
    ///     tokio::spawn(async move {
    ///         for i in 0..10 {
    ///             if let Err(_) = port.send(i).await {
    ///                 println!("receiver dropped");
    ///                 return;
    ///             }
    ///         }
    ///     });
    ///
    ///     while let Some(i) = rx.recv().await {
    ///         println!("got = {}", i);
    ///     }
    /// }
    /// ```
    // #[tracing::instrument(level = "trace", skip(self), fields(?value))]
    pub async fn send(&self, value: T) -> Result<(), PortError> {
        self.check_attachment().await?;
        let tx = self.connection.lock().await;
        (*tx).as_ref().unwrap().0.send(value).await?;
        track_egress(self.stage(), self.name());
        Ok(())
    }

    pub async fn reserve_send<F, E>(&self, task: F) -> Result<(), E>
    where
        F: futures::future::Future<Output = Result<T, E>> + Send,
        E: From<PortError> + std::fmt::Debug,
    {
        self.check_attachment().await?;
        let tx = self.connection.lock().await;
        let permit = (*tx)
            .as_ref()
            .unwrap()
            .0
            .reserve()
            .await
            .map_err(|err| PortError::Channel(err.into()))?;

        let span = tracing::trace_span!("task in output port reserve_send", stage=%self.stage, port_name=%self.name);

        match task.instrument(span).await {
            Ok(data) => {
                permit.send(data);
                Ok(())
            },
            Err(err) => {
                tracing::error!(error=?err, "task future failed");
                Err(err)
            },
        }
    }
}

impl<T> fmt::Debug for Outlet<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // copy Port::full_name to avoid stricter bounds on T
        f.debug_tuple("Outlet")
            .field(&format!("{}::{}", self.stage, self.name))
            .finish()
    }
}

// //////////////////////////////////////
// // Unit Tests ////////////////////////
//
#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use tokio_test::block_on;

    use super::*;

    #[test]
    fn test_cloning_outlet() {
        let (tx, mut rx) = mpsc::channel(8);
        let mut port_1 = Outlet::new("port_1", PORT_DATA);

        block_on(async move {
            port_1.attach("test_tx".into(), tx).await;
            let port_2 = port_1.clone();

            tokio::spawn(async move {
                port_1.send("hello").await.unwrap();
            });

            tokio::spawn(async move {
                port_2.send("world").await.unwrap();
            });

            assert_eq!(Some("hello"), rx.recv().await);
            assert_eq!(Some("world"), rx.recv().await);
            assert_eq!(None, rx.recv().await);
        });
    }
}

// todo: I have not been able to work around:
// Cannot start a runtime from within a runtime. This happens because a function (like
// `block_on`) attempted to block the current thread while the thread is being used to drive
//
// Blocking receive to call outside of asynchronous contexts.
//
// # Panics
//
// This function panics if called before attaching to the `Inlet` or
// if called within an asynchronous execution context.
//
// # Examples
//
// use std::thread;
// use tokio::runtime::Runtime;
// use tokio::sync::mpsc;
// use proctor::graph::Inlet;
//
// fn main() {
//     let (tx, mut rx) = mpsc::channel::<u8>(10);
//     let mut port = Inlet::default();
//     let mut p2 = port.clone();
//     tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
//         .block_on(async move {
//             p2.attach(rx).await;
//         });
//
//     let sync_code = thread::spawn(move || {
//         assert_eq!(Some(10), port.blocking_recv());
//     });
//
//     Runtime::new()
//         .unwrap()
//         .block_on(async move {
//             let _ = tx.send(10).await;
//         });
//     sync_code.join().unwrap();
// }
// asynchronous tasks.'
// #[tracing::instrument(level = "trace", skip(self))]
// pub fn blocking_recv(&mut self) -> Option<T> {
//     let rt = tokio::runtime::Builder::new_current_thread()
//         .enable_all()
//         .build()
//         .expect("failed to create runtime for Inlet::block_recv()");
//
//     rt.block_on(async move { self.port.lock().await.as_mut()?.blocking_recv() })
// }
