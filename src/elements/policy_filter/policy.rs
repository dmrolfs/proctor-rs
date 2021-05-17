use std::fmt::Debug;
use super::ProctorContext;
use crate::phases::collection::TelemetrySubscription;
use crate::graph::GraphResult;

pub trait Policy<I, C>: PolicySubscription<Context = C> + PolicyEngine<Item = I, Context = C> {}

impl<P, I, C> Policy<I, C> for P where P: PolicySubscription<Context = C> + PolicyEngine<Item = I, Context = C> {}

pub trait PolicySubscription: Debug + Send + Sync {
    type Context: ProctorContext;

    fn subscription(&self, name: &str) -> TelemetrySubscription {
        tracing::trace!(
            "context required_fields:{:?}, optional_fields:{:?}",
            Self::Context::required_context_fields(),
            Self::Context::optional_context_fields(),
        );

        let subscription = TelemetrySubscription::new(name)
            .with_required_fields(Self::Context::required_context_fields())
            .with_optional_fields(Self::Context::optional_context_fields());
        let subscription = self.do_extend_subscription(subscription);
        tracing::trace!("subscription after extension: {:?}", subscription);
        subscription
    }

    fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
        subscription
    }
}

pub trait PolicyEngine: Debug + Send + Sync {
    type Item;
    type Context: ProctorContext;

    fn load_policy_engine(&self, engine: &mut oso::Oso) -> GraphResult<()>;
    fn initialize_policy_engine(&self, engine: &mut oso::Oso) -> GraphResult<()>;
    fn query_policy(&self, engine: &oso::Oso, item_context: (Self::Item, Self::Context)) -> GraphResult<oso::Query>;
}
