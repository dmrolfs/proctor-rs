use std::fmt;

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;

use crate::graph::shape::SinkShape;
use crate::graph::{Inlet, Port, Stage};
use crate::{AppData, ProctorResult};

/// A Sink that will invoke the given procedure for each received element.
///
/// # Examples
///
/// ```
/// use proctor::graph::stage::{self, Stage};
/// use proctor::graph::Inlet;
/// use proctor::graph::SinkShape;
/// use tokio::sync::mpsc;
///
/// #[tokio::main]
/// async fn main() {
///     let my_data = vec![1, 2, 3];
///     let (tx, rx) = mpsc::channel(100);
///
///     let actual = Vec::<i32>::new();
///     let actual = std::sync::Arc::new(std::sync::Mutex::new(actual));
///
///     let fe_actual = actual.clone();
///     let mut foreach = stage::Foreach::new("collect", move |x| {
///         let data = fe_actual.lock();
///         if let Ok(mut data) = data {
///             data.push(x * 2);
///         }
///     });
///
///     foreach.inlet().attach("test_channel", rx).await;
///
///     let sink_handle = tokio::spawn(async move {
///         foreach.run().await;
///     });
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
///     let a = actual.lock();
///     let a = a.as_deref().unwrap();
///     assert_eq!(&vec![2, 4, 6], a);
/// }
/// ```
pub struct Foreach<F, In>
where
    F: Fn(In) -> (),
{
    name: String,
    operation: F,
    inlet: Inlet<In>,
}

impl<F, In> Foreach<F, In>
where
    F: Fn(In) -> (),
{
    pub fn new<S: Into<String>>(name: S, operation: F) -> Self {
        let name = name.into();
        let inlet = Inlet::new(name.clone());
        Self { name, operation, inlet }
    }
}

impl<F, In> SinkShape for Foreach<F, In>
where
    F: Fn(In) -> (),
{
    type In = In;

    #[inline]
    fn inlet(&self) -> Inlet<Self::In> { self.inlet.clone() }
}

#[dyn_upcast]
#[async_trait]
impl<F, In> Stage for Foreach<F, In>
where
    F: Fn(In) -> () + Send + Sync + 'static,
    In: AppData,
{
    #[inline]
    fn name(&self) -> &str { self.name.as_ref() }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.inlet.check_attachment().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run foreach sink", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        let op = &self.operation;
        while let Some(input) = self.inlet.recv().await {
            op(input);
        }
        Ok(())
    }

    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        tracing::trace!("closing foreach-sink inlet.");
        self.inlet.close().await;
        Ok(())
    }
}

impl<F, In> fmt::Debug for Foreach<F, In>
where
    F: Fn(In) -> (),
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Foreach").field("inlet", &self.inlet).finish()
    }
}
