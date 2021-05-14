mod and_then;
mod broadcast;
mod composite_through;
mod filter;
mod filter_map;
mod identity;
mod map;
mod merge;
mod merge_n;

pub use self::and_then::AndThen;
pub use self::broadcast::Broadcast;
pub use self::composite_through::CompositeThrough;
pub use self::filter::Filter;
pub use self::filter_map::FilterMap;
pub use self::identity::Identity;
pub use self::map::Map;
pub use self::merge::Merge;
pub use self::merge_n::{MergeMsg, MergeN};

use super::Stage;
use crate::graph::ThroughShape;

pub trait ThroughStage<In, Out>: Stage + ThroughShape<In = In, Out = Out> + 'static {}
impl<In, Out, T: 'static + Stage + ThroughShape<In = In, Out = Out>> ThroughStage<In, Out> for T {}
