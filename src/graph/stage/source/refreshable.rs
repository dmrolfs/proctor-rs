use std::fmt::{self, Debug};
use std::future::Future;

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use tokio::sync::mpsc;

use crate::graph::shape::SourceShape;
use crate::graph::{stage, Outlet, Port, Stage, PORT_DATA};
use crate::{AppData, ProctorResult};

/// A source that produces a single outcome, which may be restarted or cancelled via a control
/// channel and evaluation function.
/// work here to document and incorporate main.rs as DocTest
pub struct RefreshableSource<Ctrl, Out, A, F>
where
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
    A: Fn(Option<Ctrl>) -> F,
    F: Future<Output = Option<Out>>,
{
    pub fn new<S: Into<String>>(name: S, action: A, rx_control: mpsc::Receiver<Ctrl>) -> Self {
        let name = name.into();
        let outlet = Outlet::new(name.clone(), PORT_DATA);
        Self { name, action, rx_control, outlet }
    }
}

impl<Ctrl, Out, A, F> SourceShape for RefreshableSource<Ctrl, Out, A, F>
where
    A: Fn(Option<Ctrl>) -> F,
    F: Future<Output = Option<Out>>,
{
    type Out = Out;

    #[inline]
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<Ctrl, Out, A, F> Stage for RefreshableSource<Ctrl, Out, A, F>
where
    Ctrl: AppData + Copy + Into<i32>,
    Out: AppData,
    A: Fn(Option<Ctrl>) -> F + Send + Sync + 'static,
    F: Future<Output = Option<Out>> + Send + 'static,
{
    #[inline]
    fn name(&self) -> &str {
        &self.name
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.outlet.check_attachment().await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", name = "run refreshable source", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        let mut done = false;

        let op = &self.action;
        let operation = op(None);
        tokio::pin!(operation);

        let outlet = &self.outlet;
        let rx = &mut self.rx_control;

        loop {
            let _timer = stage::start_stage_eval_time(&self.name);

            tokio::select! {
                result = &mut operation, if !done => {
                    done = true;

                    if let Some(r) = result {
                        tracing::debug!("Completed with result = {:?}", r);
                        let _ = outlet.send(r).await;
                        break;
                    }
                }

                Some(control_signal) = rx.recv() => {
                    let ctrl_span = tracing::trace_span!("control check", ?control_signal);
                    let _ctrl_span_guard = ctrl_span.enter();

                    //todo: this was initially a poc exercise so this evaluation could use generalization
                    if control_signal.into() % 2 == 0 {
                        tracing::trace!("setting operation with control signal..");
                        operation.set(op(Some(control_signal)));
                        done = false;
                    }
                }
            }
        }

        Ok(())
    }

    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        tracing::info!(name=%self.name(), "closing refreshable source outlet.");
        self.outlet.close().await;
        Ok(())
    }
}

impl<Ctrl, Out, A, F> Debug for RefreshableSource<Ctrl, Out, A, F>
where
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
