use crate::graph::shape::{Shape, SourceShape};
use crate::graph::{GraphResult, Outlet, Port, Stage};
use crate::AppData;
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use std::fmt;
use std::future::Future;
use tokio::sync::mpsc;

/// A source that produces a single outcome, which may be restarted or cancelled via a control
/// channel and evaluation function.
/// work here to document and incorporate main.rs as DocTest
pub struct RefreshableSource<Ctrl, Out, A, F>
where
    Ctrl: AppData + Copy + fmt::Display + Into<i32>,
    Out: AppData + 'static,
    A: Fn(Option<Ctrl>) -> F,
    F: Future<Output = Option<Out>>,
{
    name: String,
    action: A,
    rx_control: mpsc::Receiver<Ctrl>,
    outlet: Outlet<Out>,
}

impl<Ctrl, Out, A, F> RefreshableSource<Ctrl, Out, A, F>
where
    Ctrl: AppData + Copy + fmt::Display + Into<i32>,
    Out: AppData + 'static,
    A: Fn(Option<Ctrl>) -> F,
    F: Future<Output = Option<Out>>,
{
    pub fn new<S>(name: S, action: A, rx_control: mpsc::Receiver<Ctrl>) -> Self
    where
        S: Into<String>,
    {
        let name = name.into();
        let outlet = Outlet::new(name.clone());
        Self {
            name,
            action,
            rx_control,
            outlet,
        }
    }
}

impl<Ctrl, Out, A, F> Shape for RefreshableSource<Ctrl, Out, A, F>
where
    Ctrl: AppData + Copy + fmt::Display + Into<i32>,
    Out: AppData + 'static,
    A: Fn(Option<Ctrl>) -> F,
    F: Future<Output = Option<Out>>,
{
}

impl<Ctrl, Out, A, F> SourceShape for RefreshableSource<Ctrl, Out, A, F>
where
    Ctrl: AppData + Copy + fmt::Display + Into<i32>,
    Out: AppData + 'static,
    A: Fn(Option<Ctrl>) -> F,
    F: Future<Output = Option<Out>>,
{
    type Out = Out;
    #[inline]
    fn outlet(&mut self) -> &mut Outlet<Self::Out> {
        &mut self.outlet
    }
}

#[dyn_upcast]
#[async_trait]
impl<Ctrl, Out, A, F> Stage for RefreshableSource<Ctrl, Out, A, F>
where
    Ctrl: AppData + Copy + fmt::Display + Into<i32> + 'static,
    Out: AppData + 'static,
    A: Fn(Option<Ctrl>) -> F + Send + Sync + 'static,
    F: Future<Output = Option<Out>> + Send + Sync + 'static,
{
    #[inline]
    fn name(&self) -> &str {
        self.name.as_ref()
    }

    #[tracing::instrument(
        level="info",
        name="run refreshable source",
        skip(self),
        fields(stage=%self.name),
    )]
    async fn run(&mut self) -> GraphResult<()> {
        let mut done = false;

        let op = &self.action;
        let operation = op(None);
        tokio::pin!(operation);

        let outlet = &self.outlet;
        let rx = &mut self.rx_control;

        loop {
            tokio::select! {
                result = &mut operation, if !done => {
                    let op_span = tracing::info_span!("evaluate operation", ?result);
                    let _op_span_guard = op_span.enter();
                    done = true;

                    if let Some(r) = result {
                        tracing::info!("Completed with result = {:?}", r);
                        let _ = outlet.send(r).await;
                        break;
                    }
                }

                Some(arg) = rx.recv() => {
                    let ctrl_span = tracing::info_span!("control check", %arg);
                    let _ctrl_span_guard = ctrl_span.enter();

                    if arg.into() % 2 == 0 {
                        tracing::info!("setting operation with {}", arg);
                        operation.set(op(Some(arg)));
                        done = false;
                    }
                }
            }
        }

        Ok(())
    }

    async fn close(mut self: Box<Self>) -> GraphResult<()> {
        tracing::info!("closing refreshable source outlet.");
        self.outlet.close().await;
        Ok(())
    }
}

impl<Ctrl, Out, A, F> fmt::Debug for RefreshableSource<Ctrl, Out, A, F>
where
    Ctrl: AppData + Copy + fmt::Display + Into<i32>,
    Out: AppData + 'static,
    A: Fn(Option<Ctrl>) -> F,
    F: Future<Output = Option<Out>>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RefreshableSource")
            .field("name", &self.name)
            .field("outlet", &self.outlet)
            .finish()
    }
}
