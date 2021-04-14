use super::{Inlet, Outlet, Port};
use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;

pub trait SourceShape {
    type Out;
    fn outlet(&self) -> Outlet<Self::Out>;
}

pub trait SinkShape {
    type In;
    fn inlet(&self) -> Inlet<Self::In>;
}

pub trait ThroughShape: SourceShape + SinkShape {}
impl<T: SourceShape + SinkShape> ThroughShape for T {}

/// A bidirectional flow of elements that consequently has two inputs and two outputs,
/// arranged like this:
///
/// {{{
///        +------+
///  In1 ~>|      |~> Out1
///        | bidi |
/// Out2 <~|      |<~ In2
///        +------+
/// }}}
pub trait BidiShape {
    type In1;
    type Out1;
    type In2;
    type Out2;

    fn inlet_1(&self) -> Inlet<Self::In1>;
    fn outlet_1(&self) -> Outlet<Self::Out1>;
    fn inlet_2(&self) -> Inlet<Self::In2>;
    fn outlet_2(&self) -> Outlet<Self::Out2>;
}

pub trait FanInShape2: SourceShape {
    type In0;
    type In1;

    fn inlet_0(&self) -> Inlet<Self::In0>;
    fn inlet_1(&self) -> Inlet<Self::In1>;
}

pub struct InletsShape<T>(pub Arc<Mutex<Vec<Inlet<T>>>>);

impl<T: Send> InletsShape<T> {
    pub fn new(inlets: Vec<Inlet<T>>) -> Self {
        Self(Arc::new(Mutex::new(inlets)))
    }

    pub async fn get(&self, index: usize) -> Option<Inlet<T>> {
        self.0.lock().await.get(index).map(|i| i.clone())
    }

    pub async fn close(&mut self) {
        for i in self.0.lock().await.iter_mut() {
            i.close().await;
        }
    }
}

impl<T> Clone for InletsShape<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> fmt::Debug for InletsShape<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Inlets").finish()
    }
}

pub trait UniformFanInShape: SourceShape {
    type In;
    //todo use once associated type defaults are stable
    // type InletShape = Arc<Mutex<Inlet<Self::In>>>;
    // type InletsShape = Arc<Mutex<Vec<Self::InletShape>>>;

    fn inlets(&self) -> InletsShape<Self::In>;
}

pub type OutletsShape<T> = Vec<Outlet<T>>;

pub trait UniformFanOutShape: SinkShape {
    type Out;
    fn outlets(&self) -> OutletsShape<Self::Out>;
}
