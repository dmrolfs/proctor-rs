use crate::error::GraphError;
use crate::AppData;
use async_trait::async_trait;
use std::fmt;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

#[async_trait]
pub trait Port: fmt::Debug + Send {
    fn name(&self) -> &str;

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
pub trait Connect<T: AppData> {
    async fn connect(self);
}

// #[async_trait]
// impl<T: AppData> Connect<T> for (&mut Outlet<T>, &mut Inlet<T>) {
//     async fn connect(&mut self) {
//         connect_out_to_in(self.0, self.1).await
//     }
// }
//
// #[async_trait]
// impl<T: AppData> Connect<T> for (&mut Outlet<T>, &Inlet<T>) {
//     async fn connect(&mut self) {
//         let mut inlet = self.1.clone();
//         connect_out_to_in(self.0, &mut inlet).await
//     }
// }
//
// #[async_trait]
// impl<T: AppData> Connect<T> for (&Outlet<T>, &mut Inlet<T>) {
//     async fn connect(&mut self) {
//         let mut outlet = self.0.clone();
//         connect_out_to_in(&mut outlet, self.1).await
//     }
// }
//
// #[async_trait]
// impl<T: AppData> Connect<T> for (&mut Inlet<T>, &Outlet<T>) {
//     async fn connect(&mut self) {
//         let mut outlet = self.1.clone();
//         connect_out_to_in(&mut outlet, self.0).await
//     }
// }
//
// #[async_trait]
// impl<T: AppData> Connect<T> for (&Inlet<T>, &mut Outlet<T>) {
//     async fn connect(&mut self) {
//         let mut inlet = self.0.clone();
//         connect_out_to_in(self.1, &mut inlet).await
//     }
// }
//
// #[async_trait]
// impl<T: AppData> Connect<T> for (&mut Inlet<T>, &mut Outlet<T>) {
//     async fn connect(&mut self) {
//         connect_out_to_in(self.1, self.0).await
//     }
// }
//
// #[async_trait]
// impl<T: AppData> Connect<T> for (&Inlet<T>, &Outlet<T>) {
//     async fn connect(&mut self) {
//         let mut outlet = self.1.clone();
//         let mut inlet = self.0.clone();
//         connect_out_to_in(&mut outlet, &mut inlet).await
//     }
// }
//
// #[async_trait]
// impl<T: AppData> Connect<T> for (&Outlet<T>, &Inlet<T>) {
//     async fn connect(&mut self) {
//         let mut outlet = self.0.clone();
//         let mut inlet = self.1.clone();
//         connect_out_to_in(&mut outlet, &mut inlet).await
//     }
// }


#[async_trait]
impl<T: AppData> Connect<T> for (Outlet<T>, Inlet<T>) {
    async fn connect(mut self) {
        let mut outlet = self.0;
        let mut inlet = self.1;
        connect_out_to_in(outlet, inlet).await
    }
}

#[async_trait]
#[async_trait]
impl<T: AppData> Connect<T> for (&Outlet<T>, &Inlet<T>) {
    async fn connect(mut self) {
        let mut outlet = self.0.clone();
        let mut inlet = self.1.clone();
        connect_out_to_in(outlet, inlet).await
    }
}

#[async_trait]
impl<T: AppData> Connect<T> for (Inlet<T>, Outlet<T>) {
    async fn connect(mut self) {
        let mut outlet = self.1;
        let mut inlet = self.0;
        connect_out_to_in(outlet, inlet).await
    }
}


#[async_trait]
impl<T: AppData> Connect<T> for (&Inlet<T>, &Outlet<T>) {
    async fn connect(mut self) {
        let mut outlet = self.1.clone();
        let mut inlet = self.0.clone();
        connect_out_to_in(outlet, inlet).await
    }
}

pub async fn connect_out_to_in<T: AppData>(mut lhs: Outlet<T>, mut rhs: Inlet<T>) {
    let (tx, rx) = mpsc::channel(num_cpus::get());
    lhs.attach(tx).await;
    rhs.attach(rx).await;
}

pub struct Inlet<T: AppData>(String, Arc<Mutex<Option<mpsc::Receiver<T>>>>);

impl<T: AppData> Inlet<T> {
    pub fn new<S: Into<String>>(name: S) -> Self {
        Self(name.into(), Arc::new(Mutex::new(None)))
    }

    pub fn with_receiver<S: Into<String>>(name: S, rx: mpsc::Receiver<T>) -> Self {
        Self(name.into(), Arc::new(Mutex::new(Some(rx))))
    }
}

#[async_trait]
impl<T: AppData> Port for Inlet<T> {
    #[inline]
    fn name(&self) -> &str {
        self.0.as_str()
    }

    async fn close(&mut self) {
        let mut rx = self.1.lock().await;
        match rx.as_mut() {
            Some(r) => {
                tracing::trace!(inlet=%self.0, "closing Inlet");
                r.close()
            }
            None => {
                tracing::trace!(inlet=%self.0, "Inlet close ignored - not attached");
                ()
            }
        }
    }
}

impl<T: AppData> Clone for Inlet<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self(self.0.clone(), self.1.clone())
    }
}

impl<T: AppData> Inlet<T> {
    pub async fn attach(&mut self, rx: mpsc::Receiver<T>) {
        let mut port = self.1.lock().await;
        *port = Some(rx);
    }

    pub async fn is_attached(&self) -> bool {
        self.1.lock().await.is_some()
    }

    pub async fn check_attachment(&self) -> Result<(), GraphError> {
        if self.is_attached().await {
            return Ok(());
        } else {
            return Err(GraphError::Detached);
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
    /// use tokio::sync::mpsc;
    /// use proctor::graph::Inlet;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let (tx, mut rx) = mpsc::channel(100);
    ///     let mut port = Inlet::new("port");
    ///     port.attach(rx).await;
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
    /// use tokio::sync::mpsc;
    /// use proctor::graph::Inlet;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let (tx, mut rx) = mpsc::channel(100);
    ///     let mut port = Inlet::new("port");
    ///     port.attach(rx).await;
    ///
    ///     tx.send("hello").await.unwrap();
    ///     tx.send("world").await.unwrap();
    ///
    ///     assert_eq!(Some("hello"), port.recv().await);
    ///     assert_eq!(Some("world"), port.recv().await);
    /// }
    /// ```
    #[tracing::instrument(level = "trace", skip(self), fields(inlet=%self.0))]
    pub async fn recv(&mut self) -> Option<T> {
        if self.is_attached().await {
            let mut rx = self.1.lock().await;
            tracing::trace!(inlet=%self.0, "Inlet receiving...");
            let item = (*rx).as_mut()?.recv().await;
            tracing::trace!(inlet=%self.0, ?item, "Inlet ...received");
            if item.is_none() && rx.is_some() {
                tracing::info!(inlet=%self.0, "Inlet depleted - closing receiver");
                rx.as_mut().unwrap().close();
                let _ = rx.take();
            }

            item
        } else {
            tracing::trace!(inlet=%self.0, "Inlet not attached");
            None
        }
    }
}

impl<T: AppData> fmt::Debug for Inlet<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Inlet").field(&self.0).finish()
    }
}

pub struct Outlet<T: AppData>(String, Arc<Mutex<Option<mpsc::Sender<T>>>>);

impl<T: AppData> Outlet<T> {
    pub fn new<S: Into<String>>(name: S) -> Self {
        Self(name.into(), Arc::new(Mutex::new(None)))
    }

    pub fn with_sender<S: Into<String>>(name: S, tx: mpsc::Sender<T>) -> Outlet<T> {
        Self(name.into(), Arc::new(Mutex::new(Some(tx))))
    }
}

#[async_trait]
impl<T: AppData> Port for Outlet<T> {
    #[inline]
    fn name(&self) -> &str {
        self.0.as_str()
    }

    async fn close(&mut self) {
        tracing::trace!(inlet=%self.name(), "closing Outlet");
        self.1.lock().await.take();
    }
}

impl<T: AppData> Clone for Outlet<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), self.1.clone())
    }
}

impl<T: AppData> Outlet<T> {
    pub async fn attach(&mut self, tx: mpsc::Sender<T>) {
        let mut port = self.1.lock().await;
        *port = Some(tx);
    }

    pub async fn is_attached(&self) -> bool {
        self.1.lock().await.is_some()
    }

    pub async fn check_attachment(&self) -> Result<(), GraphError> {
        if self.is_attached().await {
            return Ok(());
        } else {
            return Err(GraphError::Detached);
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
    /// use tokio::sync::mpsc;
    /// use proctor::graph::Outlet;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let (tx, mut rx) = mpsc::channel(1);
    ///     let mut port = Outlet::new("port");
    ///     port.attach(tx).await;
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
    pub async fn send(&self, value: T) -> Result<(), GraphError> {
        self.check_attachment().await?;
        let tx = self.1.lock().await;
        (*tx).as_ref().unwrap().send(value).await.map_err(|err| err.into())
    }
}

impl<T: AppData> fmt::Debug for Outlet<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Outlet").field(&self.0).finish()
    }
}

// //////////////////////////////////////
// // Unit Tests ////////////////////////
//
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use tokio_test::block_on;

    #[test]
    fn test_cloning_outlet() {
        let (tx, mut rx) = mpsc::channel(8);
        let mut port_1 = Outlet::new("port_1");

        block_on(async move {
            port_1.attach(tx).await;
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

//
//todo: I have not been able to work around:
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
