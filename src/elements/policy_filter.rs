pub use protocol::*;
mod protocol;

use crate::graph::stage::{self, Stage};
use crate::graph::{GraphResult, Inlet, Outlet, Port};
use crate::graph::{SinkShape, SourceShape};
use crate::phases::collection::TelemetrySubscription;
use crate::{AppData, ProctorContext};
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use oso::Oso;
use std::collections::HashSet;
use std::fmt::{self, Debug};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::{broadcast, mpsc};
use tracing::Instrument;

pub trait PolicySettings {
    fn required_subscription_fields(&self) -> HashSet<String>;
    fn optional_subscription_fields(&self) -> HashSet<String>;
    fn specification_path(&self) -> PathBuf;
}

pub trait Policy: Debug + Send + Sync {
    type Item;
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
    fn load_knowledge_base(&self, oso: &mut oso::Oso) -> GraphResult<()>;
    fn initialize_knowledge_base(&self, oso: &mut oso::Oso) -> GraphResult<()>;
    fn query_knowledge_base(
        &self, oso: &oso::Oso, item_context: (Self::Item, Self::Context),
    ) -> GraphResult<oso::Query>;
}

pub struct PolicyFilter<T, C> {
    name: String,
    policy: Box<dyn Policy<Item = T, Context = C>>,
    context_inlet: Inlet<C>,
    inlet: Inlet<T>,
    outlet: Outlet<T>,
    tx_api: PolicyFilterApi<C>,
    rx_api: mpsc::UnboundedReceiver<PolicyFilterCmd<C>>,
    pub tx_monitor: broadcast::Sender<PolicyFilterEvent<T, C>>,
}

impl<T, C> PolicyFilter<T, C>
where
    T: Clone,
    C: Clone,
{
    pub fn new<S: Into<String>>(name: S, policy: Box<dyn Policy<Item = T, Context = C>>) -> Self {
        let name = name.into();
        let context_inlet = Inlet::new(format!("{}_policy_context_inlet", name.clone()));
        let inlet = Inlet::new(name.clone());
        let outlet = Outlet::new(name.clone());
        let (tx_api, rx_api) = mpsc::unbounded_channel();
        let (tx_monitor, _) = broadcast::channel(num_cpus::get() * 2);
        Self {
            name,
            policy,
            context_inlet,
            inlet,
            outlet,
            tx_api,
            rx_api,
            tx_monitor,
        }
    }

    #[inline]
    pub fn context_inlet(&self) -> Inlet<C> {
        self.context_inlet.clone()
    }
}

impl<T, C> SinkShape for PolicyFilter<T, C> {
    type In = T;
    #[inline]
    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

impl<T, C> SourceShape for PolicyFilter<T, C> {
    type Out = T;
    #[inline]
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<T, C> Stage for PolicyFilter<T, C>
where
    T: AppData + Clone + Sync,
    C: ProctorContext + Debug + Clone + Send + Sync,
{
    #[inline]
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> GraphResult<()> {
        self.inlet.check_attachment().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run policy_filter through", skip(self))]
    async fn run(&mut self) -> GraphResult<()> {
        let name = self.name().to_owned();
        let mut oso = self.oso()?;
        let outlet = &self.outlet;
        let item_inlet = &mut self.inlet;
        let context_inlet = &mut self.context_inlet;
        let policy = &self.policy;
        let context: Arc<Mutex<Option<C>>> = Arc::new(Mutex::new(None));
        let rx_api = &mut self.rx_api;
        let tx_monitor = &self.tx_monitor;

        loop {
            tokio::select! {
                item = item_inlet.recv() => match item {
                    Some(item) => {
                        tracing::trace!(?item, ?context, "handling next item...");

                        match context.lock().await.as_ref() {
                            Some(ctx) => Self::handle_item(item, ctx, policy, &oso, outlet, tx_monitor).await?,
                            None => Self::handle_item_before_context(item, tx_monitor)?,
                        }
                    },
                    None => {
                        tracing::info!("PolicyFilter item_inlet dropped - completing.");
                        break;
                    }
                },

                Some(ctx) = context_inlet.recv() => Self::handle_context(context.clone(), ctx, tx_monitor).await?,

                Some(command) = rx_api.recv() => {
                    let cont_loop = Self::handle_command(
                        command,
                        &mut oso,
                        name.as_str(),
                        policy,
                        context.clone(),
                    ).await?;

                    if !cont_loop {
                        tracing::trace!("policy commanded to stop - breaking...");
                        break;
                    }
                },

                else => {
                    tracing::trace!("feed into policy depleted - breaking...");
                    break;
                },
            }
        }

        Ok(())
    }

    async fn close(mut self: Box<Self>) -> GraphResult<()> {
        tracing::info!("closing policy_filter ports");
        self.inlet.close().await;
        self.outlet.close().await;
        Ok(())
    }
}

impl<T, C> PolicyFilter<T, C>
where
    T: Debug + Clone,
    C: ProctorContext + Debug + Clone,
{
    #[tracing::instrument(level = "info", name = "make policy knowledge base", skip(self))]
    fn oso(&self) -> GraphResult<Oso> {
        let mut oso = Oso::new();

        self.policy.load_knowledge_base(&mut oso).map_err(|err| {
            tracing::error!(error=?err, "failed to load policy into knowledge base.");
            err
        })?;

        self.policy.initialize_knowledge_base(&mut oso).map_err(|err| {
            tracing::error!(error=?err, "failed to initialize policy knowledge base.");
            err
        })?;

        Ok(oso)
    }

    #[tracing::instrument(level = "trace", skip(tx))]
    fn publish_event(
        event: PolicyFilterEvent<T, C>, tx: &broadcast::Sender<PolicyFilterEvent<T, C>>,
    ) -> GraphResult<()> {
        let nr_notified = tx
            .send(event.clone())
            .map_err::<crate::error::GraphError, _>(|err| err.into())?;
        tracing::trace!(%nr_notified, ?event, "notified subscribers of policy filter event.");
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "policy_filter handle item", skip(oso, outlet), fields())]
    async fn handle_item(
        item: T, context: &C, policy: &Box<dyn Policy<Item = T, Context = C>>, oso: &Oso, outlet: &Outlet<T>,
        tx: &broadcast::Sender<PolicyFilterEvent<T, C>>,
    ) -> GraphResult<()> {
        let result = {
            // query lifetime cannot span across `.await` since it cannot `Send` between threads.
            let mut query = policy.query_knowledge_base(oso, (item.clone(), context.clone()))?;
            query.next()
        };

        tracing::info!(?result, "knowledge base query results");

        match result {
            Some(Ok(result_set)) => {
                tracing::info!(
                    ?result_set,
                    ?item,
                    ?context,
                    "item and context passed policy review - sending via outlet."
                );
                outlet
                    .send(item.clone())
                    .instrument(tracing::info_span!("PolicyFilter.outlet send", ?item))
                    .await?;
                Ok(())
            }

            Some(Err(err)) => {
                tracing::warn!(error=?err, ?item, ?context, "error in policy review - skipping item.");
                Self::publish_event(PolicyFilterEvent::ItemBlocked(item), tx)?;
                Err(err.into())
            }

            None => {
                tracing::info!(
                    ?item,
                    ?context,
                    "item and context did not pass policy review - skipping item."
                );
                Self::publish_event(PolicyFilterEvent::ItemBlocked(item), tx)?;
                Ok(())
            }
        }
    }

    #[tracing::instrument(
        level = "info",
        name = "policy_filter handle item before context set",
        skip(tx),
        fields()
    )]
    fn handle_item_before_context(item: T, tx: &broadcast::Sender<PolicyFilterEvent<T, C>>) -> GraphResult<()> {
        tracing::info!(?item, "dropping item received before policy context set.");
        Self::publish_event(PolicyFilterEvent::ItemBlocked(item), tx)?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "policy_filter handle context", skip(tx), fields())]
    async fn handle_context(
        context: Arc<Mutex<Option<C>>>, recv_context: C, tx: &broadcast::Sender<PolicyFilterEvent<T, C>>,
    ) -> GraphResult<()> {
        tracing::trace!(recv_context=?recv_context, "handling policy context update...");
        let mut ctx = context.lock().await;
        *ctx = Some(recv_context);

        Self::publish_event(PolicyFilterEvent::ContextChanged(ctx.clone()), tx)?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "policy_filter handle command", skip(oso, policy,), fields())]
    async fn handle_command(
        command: PolicyFilterCmd<C>, oso: &mut Oso, name: &str, policy: &Box<dyn Policy<Item = T, Context = C>>,
        context: std::sync::Arc<tokio::sync::Mutex<Option<C>>>,
    ) -> GraphResult<bool> {
        tracing::trace!(?command, ?context, "handling policy filter command...");

        match command {
            PolicyFilterCmd::Inspect(tx) => {
                let detail = PolicyFilterDetail {
                    name: name.to_owned(),
                    context: context.lock().await.clone(),
                };
                let _ignore_failure = tx.send(detail);
                Ok(true)
            }

            PolicyFilterCmd::ReplacePolicy { new_policy, tx } => {
                oso.clear_rules();
                match new_policy {
                    PolicySource::String(policy) => oso.load_str(policy.as_str())?,
                    PolicySource::File(path) => oso.load_file(path)?,
                }

                let _ignore_failure = tx.send(());
                Ok(true)
            }

            PolicyFilterCmd::AppendPolicy {
                additional_policy: policy_source,
                tx,
            } => {
                match policy_source {
                    PolicySource::String(p) => oso.load_str(p.as_str())?,
                    PolicySource::File(path) => oso.load_file(path)?,
                }
                let _ignore_failure = tx.send(());
                Ok(true)
            }

            PolicyFilterCmd::ResetPolicy(tx) => {
                oso.clear_rules();
                policy.load_knowledge_base(oso)?;
                let _ignore_failrue = tx.send(());
                Ok(true)
            }
        }
    }
}

impl<T, C> stage::WithApi for PolicyFilter<T, C> {
    type Sender = PolicyFilterApi<C>;

    #[inline]
    fn tx_api(&self) -> Self::Sender {
        self.tx_api.clone()
    }
}

impl<T, C> stage::WithMonitor for PolicyFilter<T, C> {
    type Receiver = PolicyFilterMonitor<T, C>;
    #[inline]
    fn rx_monitor(&self) -> Self::Receiver {
        self.tx_monitor.subscribe()
    }
}

impl<T, C> Debug for PolicyFilter<T, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PolicyFilter")
            .field("name", &self.name)
            .field("policy_context", &self.policy)
            .field("context_inlet", &self.context_inlet)
            .field("inlet", &self.inlet)
            .field("outlet", &self.outlet)
            .finish()
    }
}

/////////////////////////////////////////////////////
// Unit Tests ///////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::elements::telemetry;
    use crate::error::GraphError;
    use oso::PolarClass;
    use pretty_assertions::assert_eq;
    use serde::{Deserialize, Serialize};
    use tokio::sync::mpsc;
    use tokio_test::block_on;

    // Make sure the `PolicyFilter` object is threadsafe
    // #[test]
    // static_assertions::assert_impl_all!(PolicyFilter: Send, Sync);

    #[derive(Debug, PartialEq, Clone, PolarClass)]
    struct User {
        #[polar(attribute)]
        pub username: String,
    }

    #[derive(PolarClass, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestContext {
        #[polar(attribute)]
        pub location_code: u32,
        #[polar(attribute)]
        #[serde(flatten)]
        pub qualities: telemetry::Table,
    }

    impl ProctorContext for TestContext {
        fn required_context_fields() -> HashSet<&'static str> {
            maplit::hashset! {"location_code", }
        }

        fn custom(&self) -> telemetry::Table {
            self.qualities.clone()
        }
    }

    #[derive(Debug)]
    struct TestPolicy {
        policy: String,
    }

    impl TestPolicy {
        pub fn new<S: Into<String>>(policy: S) -> Self {
            Self { policy: policy.into() }
        }
    }

    impl Policy for TestPolicy {
        type Item = User;
        type Context = TestContext;

        fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
            subscription.with_optional_fields(maplit::hashset! {"foo".to_string(), "score".to_string()})
        }

        fn load_knowledge_base(&self, oso: &mut Oso) -> GraphResult<()> {
            oso.load_str(self.policy.as_str()).map_err(|err| err.into())
        }

        fn initialize_knowledge_base(&self, oso: &mut Oso) -> GraphResult<()> {
            oso.register_class(User::get_polar_class())
                .map_err::<GraphError, _>(|err| err.into())?;
            oso.register_class(TestContext::get_polar_class())
                .map_err::<GraphError, _>(|err| err.into())?;
            Ok(())
        }

        fn query_knowledge_base(
            &self, oso: &Oso, item_context: (Self::Item, Self::Context),
        ) -> GraphResult<oso::Query> {
            oso.query_rule("allow", (item_context.0, "foo", "bar"))
                .map_err(|err| err.into())
        }
    }

    #[test]
    fn test_handle_item() -> anyhow::Result<()> {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("policy_filter::test_handle_item");
        let _main_span_guard = main_span.enter();

        let policy: Box<dyn Policy<Item = User, Context = TestContext>> = Box::new(TestPolicy::new(
            r#"allow(actor, action, resource) if actor.username.ends_with("example.com");"#,
        ));

        let policy_filter = PolicyFilter::new("test-policy-filter", policy);
        let oso = policy_filter.oso()?; //.expect("failed to build policy engine");

        let item = User {
            username: "peter.pan@example.com".to_string(),
        };

        block_on(async move {
            let (tx, mut rx) = mpsc::channel(4);
            let mut outlet = Outlet::new("test");
            outlet.attach(tx).await;

            let (tx_monitor, _rx_monitor) = broadcast::channel(4);

            let context = TestContext {
                location_code: 17,
                qualities: maplit::hashmap! {
                    "foo".to_string() => "bar".into(),
                    "score".to_string() => 13.into(),
                },
            };

            PolicyFilter::handle_item(item, &context, &policy_filter.policy, &oso, &outlet, &tx_monitor)
                .await
                .expect("handle_item failed");

            outlet.close().await;

            let actual = rx.recv().await;
            assert!(actual.is_some());
            assert_eq!(
                actual.unwrap(),
                User {
                    username: "peter.pan@example.com".to_string()
                }
            );

            assert!(rx.recv().await.is_none());
        });

        Ok(())
    }
}
