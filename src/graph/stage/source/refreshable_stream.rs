// use std::fmt::{Debug, Display};
// use std::future::Future;
// use std::time::Duration;
// use tokio::sync::mpsc;
// use tokio::time::sleep;
// use async_stream::stream;
// use futures_core::stream::Stream;
// use tracing_futures::Instrument;
// use futures_util::stream::StreamExt;
//
//
// pub struct RefreshableSourceStream<Ctrl, A, Out, F>
//     where
//         Ctrl: Copy + Display + Debug + Into<i32>,
//         A: Fn(Option<Ctrl>) -> F,
//         Out: Debug + Send + Sized + 'static,
//         F: Future<Output = Option<Out>>,
// {
//     action: A,
//     rx_control: mpsc::Receiver<Ctrl>,
// }
//
// impl<Ctrl, A, Out, F> RefreshableSourceStream<Ctrl, A, Out, F>
//     where
//         Ctrl: Copy + Display + Debug + Into<i32>,
//         A: Fn(Option<Ctrl>) -> F,
//         Out: Debug + Send + Sized + 'static,
//         F: Future<Output = Option<Out>>,
// {
//     pub fn new( action: A, rx_control: mpsc::Receiver<Ctrl>, ) -> Self {
//         Self {
//             action,
//             rx_control,
//         }
//     }
//
//     #[tracing::instrument(level="info", skip(self))]
//     pub fn spawn(&mut self) -> impl Stream<Item = Out> {
//         let mut done = false;
//
//         let op = &self.action;
//         let operation = op(None);
//         tokio::pin!(operation);
//
//         let result = async {
//             let r2 = loop {
//                 tokio::select! {
//                 result = &mut operation, if !done => {
//                     let op_span = tracing::info_span!("operation evaluation", ?result, done);
//                     let _op_span_guard = op_span.enter();
//
//                     done = true;
//
//                     if let Some(r) = result {
//                         tracing::info!("Completed with result = {:?}", r);
//                         break r;
//                     }
//                 }
//
//                 Some(arg) = self.rx_control.recv() => {
//                     sleep(Duration::from_secs(1))
//                         .instrument(tracing::info_span!("control check", %arg))
//                         .await;
//
//                     if arg.into() % 2 == 0 {
//                         tracing::info!("setting operation with {}", arg);
//                         operation.set(op(Some(arg)));
//                         done = false;
//                     }
//                 }
//             }
//             };
//             r2.await
//             };
//
//         stream! { yield result; }
//     }
// }