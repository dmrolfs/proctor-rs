use super::{Inlet, Outlet, Port};
// use crate::AppData;
use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;

// pub trait Shape {}

pub trait SourceShape {
    type Out; //: AppData;
    fn outlet(&self) -> Outlet<Self::Out>;
}

// impl<T: SourceShape> Shape for T {}

pub trait SinkShape {
    type In; //: AppData;
    fn inlet(&self) -> Inlet<Self::In>;
}

// impl<T: SinkShape> Shape for T {}

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
    type In1; //: AppData;
    type Out1; //: AppData;
    type In2; //: AppData;
    type Out2; //: AppData;

    fn inlet_1(&self) -> Inlet<Self::In1>;
    fn outlet_1(&self) -> Outlet<Self::Out1>;
    fn inlet_2(&self) -> Inlet<Self::In2>;
    fn outlet_2(&self) -> Outlet<Self::Out2>;
}

pub trait FanInShape2: SourceShape {
    type In0; //: AppData;
    type In1; //: AppData;

    fn inlet_0(&self) -> Inlet<Self::In0>;
    fn inlet_1(&self) -> Inlet<Self::In1>;
}

pub struct InletsShape<T /*: AppData*/>(pub Arc<Mutex<Vec<Inlet<T>>>>);

impl<T: Send /*: AppData*/> InletsShape<T> {
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

impl<T /*: AppData*/> Clone for InletsShape<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T /*: AppData*/> fmt::Debug for InletsShape<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Inlets").finish()
    }
}

pub trait UniformFanInShape: SourceShape {
    type In; //: AppData;
             //todo use once associated type defaults are stable
             // type InletShape = Arc<Mutex<Inlet<Self::In>>>;
             // type InletsShape = Arc<Mutex<Vec<Self::InletShape>>>;

    fn inlets(&self) -> InletsShape<Self::In>;
}

pub type OutletsShape<T> = Vec<Outlet<T>>;

pub trait UniformFanOutShape: SinkShape {
    type Out; //: AppData;
    fn outlets(&self) -> OutletsShape<Self::Out>;
}
