use crate::graph::shape::SourceShape;
use crate::graph::{GraphResult, Outlet, Port, Stage};
use crate::AppData;
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use std::fmt;

/// Helper to create Source from Iterable. Example usage: Slice::new(vec![1,2,3]).
///
/// Starts a new Source from the given Iterable. This is like starting from an Iterator, but every
/// Subscriber directly attached to the `Outlet` of this source will see an individual flow of
/// elements (always starting from the beginning) regardless of when they subscribed.
///
/// # Examples
///
/// ```
/// use proctor::graph::Connect;
/// use proctor::graph::stage::{self, Stage};
/// use proctor::graph::{SourceShape, SinkShape};
///
/// #[tokio::main]
/// async fn main() {
///     let mut src = stage::Sequence::new(
///         "my_data",
///         vec![
///             "I am serious.".to_string(),
///             "And don't call me".to_string(),
///             "Shirley!".to_string(),
///         ]
///     );
///
///     let mut sink = stage::Fold::new(
///         "concatenate",
///         "".to_string(),
///         |acc, s: String| {
///             let result = if !acc.is_empty() { acc + " " } else { acc };
///             result + s.as_str()
///         }
///     );
///     let mut rx_quote = sink.take_final_rx().unwrap();
///
///     (src.outlet(), sink.inlet()).connect().await;
///
///     let sink_handle = tokio::spawn(async move { sink.run().await; });
///     let src_handle = tokio::spawn(async move { src.run().await; });
///
///     src_handle.await.unwrap();
///     sink_handle.await.unwrap();
///
///     match rx_quote.try_recv() {
///         Ok(quote) => assert_eq!("I am serious. And don't call me Shirley!", quote),
///         Err(err) => panic!("quote not yet assembled: {}", err),
///     }
/// }
/// ```
pub struct Sequence<T, I> {
    name: String,
    items: Option<I>,
    outlet: Outlet<T>,
}

impl<T, I> Sequence<T, I> {
    pub fn new<S, I0>(name: S, data: I0) -> Self
    where
        S: Into<String>,
        I0: IntoIterator<Item = T, IntoIter = I>,
    {
        let name = name.into();
        let outlet = Outlet::new(name.clone());
        let items = data.into_iter();
        Self {
            name,
            items: Some(items),
            outlet,
        }
    }
}

#[dyn_upcast]
#[async_trait]
impl<T, I> Stage for Sequence<T, I>
where
    T: AppData,
    I: Iterator<Item = T> + Send + Sync + 'static,
{
    #[inline]
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> GraphResult<()> {
        self.outlet.check_attachment().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run sequence source", skip(self))]
    async fn run(&mut self) -> GraphResult<()> {
        if let Some(items) = self.items.take() {
            for (count, item) in items.enumerate() {
                tracing::trace!(?item, %count, "sending item");
                self.outlet.send(item).await?
            }
        }

        Ok(())
    }

    async fn close(mut self: Box<Self>) -> GraphResult<()> {
        tracing::trace!("closing sequence-source outlet.");
        self.outlet.close().await;
        Ok(())
    }
}

impl<T, I> SourceShape for Sequence<T, I> {
    type Out = T;
    #[inline]
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

impl<T, I> fmt::Debug for Sequence<T, I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sequence")
            .field("name", &self.name)
            .field("outlet", &self.outlet)
            .finish()
    }
}
/////////////////////////////////////////////////////
// Unit Tests ///////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::sync::mpsc;
    use tokio_test::block_on;

    #[test]
    fn test_basic_usage() {
        let my_data = vec![2, 3, 5, 7, 11, 13, 17, 19];
        let actual = Arc::new(Mutex::new(Vec::<i32>::with_capacity(my_data.len())));

        let (tx, mut rx) = mpsc::channel(8);

        let mut src = Sequence::new("my_data", my_data);

        let recv_actual = actual.clone();
        block_on(async move {
            src.outlet.attach(tx).await;
            let src_handle = tokio::spawn(async move { src.run().await });

            let actual_handle = tokio::spawn(async move {
                while let Some(d) = rx.recv().await {
                    let recv_a = recv_actual.lock();
                    if let Ok(mut recv) = recv_a {
                        recv.push(d);
                    }
                }
            });

            src_handle.await.unwrap().expect("failed to join source");
            actual_handle.await.unwrap();
        });

        let a = actual.lock();
        let a = a.as_deref().unwrap();
        assert_eq!(&vec![2, 3, 5, 7, 11, 13, 17, 19], a);
    }
}
