use crate::graph::shape::{Shape, SourceShape};
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
pub struct Sequence<T>
where
    T: AppData,
{
    name: String,
    //todo can I make this `&'a [T]`? First attempt resulted in requiring `T: 'static`, which has
    // huge downstream implications requiring `T: 'static` across the graph package.
    data: Vec<T>,
    outlet: Outlet<T>,
}

impl<T> Sequence<T>
where
    T: AppData,
{
    pub fn new<S>(name: S, data: Vec<T>) -> Self
    where
        S: Into<String>,
    {
        let name = name.into();
        let outlet = Outlet::new(name.clone());
        Self { name, data, outlet }
    }
}

#[dyn_upcast]
#[async_trait]
impl<T> Stage for Sequence<T>
where
    T: AppData + 'static,
{
    #[inline]
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(
        level="info",
        name="run sequence source",
        skip(self),
        fields(name=%self.name),
    )]
    async fn run(&mut self) -> GraphResult<()> {
        for d in self.data.drain(..) {
            tracing::trace!(item=?d, "sending item");
            self.outlet.send(d).await?
        }

        Ok(())
    }

    async fn close(mut self: Box<Self>) -> GraphResult<()> {
        tracing::trace!("closing sequence-source outlet.");
        self.outlet.close().await;
        Ok(())
    }
}

impl<T> Shape for Sequence<T> where T: AppData {}

impl<T> SourceShape for Sequence<T>
where
    T: AppData,
{
    type Out = T;
    #[inline]
    fn outlet(&mut self) -> &mut Outlet<Self::Out> {
        &mut self.outlet
    }
}

impl<T> fmt::Debug for Sequence<T>
where
    T: AppData,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sequence")
            .field("name", &self.name)
            .field("nr_data_items", &self.data.len())
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
