pub use policy::*;
pub use protocol::*;

mod policy;
mod protocol;

use std::collections::HashSet;
use std::convert::TryFrom;
use std::fmt::{self, Debug};
use std::sync::Arc;

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use oso::{Oso, ToPolar};
use tokio::sync::Mutex;
use tokio::sync::{broadcast, mpsc};

use crate::elements::{telemetry, TelemetryValue, ToTelemetry};
use crate::error::{GraphError, PolicyError, TelemetryError, TypeExpectation};
use crate::graph::stage::{self, Stage};
use crate::graph::{Inlet, Outlet, Port};
use crate::graph::{SinkShape, SourceShape};
use crate::{AppData, ProctorContext, ProctorResult};

pub trait PolicySettings {
    fn required_subscription_fields(&self) -> HashSet<String>;
    fn optional_subscription_fields(&self) -> HashSet<String>;
    fn source(&self) -> PolicySource;
}

#[derive(Debug, Clone, PartialEq)]
pub struct PolicyOutcome<T, C> {
    pub item: T,
    pub context: C,
    pub bindings: telemetry::Table,
}

impl<T, C> PolicyOutcome<T, C> {
    pub fn new(item: T, context: C, bindings: telemetry::Table) -> Self {
        Self { item, context, bindings }
    }
}

const T_ITEM: &'static str = "item";
const T_CONTEXT: &'static str = "context";
const T_BINDINGS: &'static str = "bindings";

impl<T, C> Into<TelemetryValue> for PolicyOutcome<T, C>
where
    T: Into<TelemetryValue>,
    C: Into<TelemetryValue>,
{
    fn into(self) -> TelemetryValue {
        TelemetryValue::Table(maplit::hashmap! {
            T_ITEM.to_string() => self.item.to_telemetry(),
            T_CONTEXT.to_string() => self.context.to_telemetry(),
            T_BINDINGS.to_string() => self.bindings.to_telemetry(),
        })
    }
}

impl<T, C> TryFrom<TelemetryValue> for PolicyOutcome<T, C>
where
    T: TryFrom<TelemetryValue>,
    <T as TryFrom<TelemetryValue>>::Error: Into<TelemetryError>,
    C: TryFrom<TelemetryValue>,
    <C as TryFrom<TelemetryValue>>::Error: Into<TelemetryError>,
{
    type Error = PolicyError;

    fn try_from(value: TelemetryValue) -> Result<Self, Self::Error> {
        if let TelemetryValue::Table(ref table) = value {
            let item = if let Some(i) = table.get(T_ITEM) {
                T::try_from(i.clone()).map_err(|err| PolicyError::TelemetryError(err.into()))
            } else {
                Err(PolicyError::DataNotFound(T_ITEM.to_string()))
            }?;

            let context = if let Some(c) = table.get(T_CONTEXT) {
                C::try_from(c.clone()).map_err(|err| PolicyError::TelemetryError(err.into()))
            } else {
                Err(PolicyError::DataNotFound(T_CONTEXT.to_string()))
            }?;

            let bindings = if let Some(b) = table.get(T_BINDINGS) {
                telemetry::Table::try_from(b.clone()).map_err(|err| err.into())
            } else {
                Err(PolicyError::DataNotFound(T_BINDINGS.to_string()))
            }?;

            Ok(PolicyOutcome { item, context, bindings })
        } else {
            Err(PolicyError::TelemetryError(TelemetryError::TypeError {
                expected: format!("telemetry {}", TypeExpectation::Table),
                actual: Some(format!("{:?}", value)),
            }))
        }
    }
}

pub struct PolicyFilter<T, C, A, P>
where
    P: QueryPolicy<Item = T, Context = C, Args = A>,
{
    name: String,
    policy: P,
    context_inlet: Inlet<C>,
    inlet: Inlet<T>,
    outlet: Outlet<PolicyOutcome<T, C>>,
    tx_api: PolicyFilterApi<C>,
    rx_api: mpsc::UnboundedReceiver<PolicyFilterCmd<C>>,
    pub tx_monitor: broadcast::Sender<PolicyFilterEvent<T, C>>,
}

impl<T, C, A, P> PolicyFilter<T, C, A, P>
where
    T: Clone,
    C: Clone,
    P: QueryPolicy<Item = T, Context = C, Args = A>,
{
    pub fn new<S: Into<String>>(name: S, policy: P) -> Self {
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

impl<T, C, A, P> SinkShape for PolicyFilter<T, C, A, P>
where
    P: QueryPolicy<Item = T, Context = C, Args = A>,
{
    type In = T;

    #[inline]
    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

impl<T, C, A, P> SourceShape for PolicyFilter<T, C, A, P>
where
    P: QueryPolicy<Item = T, Context = C, Args = A>,
{
    type Out = PolicyOutcome<T, C>;

    #[inline]
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<T, C, A, P> Stage for PolicyFilter<T, C, A, P>
where
    T: AppData + ToPolar + Clone + Sync,
    C: ProctorContext + Debug + Clone + Send + Sync,
    A: 'static,
    P: QueryPolicy<Item = T, Context = C, Args = A> + 'static,
{
    #[inline]
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run policy_filter through", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        self.do_run().await?;
        Ok(())
    }

    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        self.do_close().await?;
        Ok(())
    }
}

impl<T, C, A, P> PolicyFilter<T, C, A, P>
where
    T: AppData + ToPolar + Clone + Sync,
    C: ProctorContext + Debug + Clone + Send + Sync,
    A: 'static,
    P: QueryPolicy<Item = T, Context = C, Args = A> + 'static,
{
    #[inline]
    async fn do_check(&self) -> Result<(), GraphError> {
        self.inlet.check_attachment().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    #[inline]
    async fn do_run(&mut self) -> Result<(), GraphError> {
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
                        Arc::clone(&context),
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

    #[inline]
    async fn do_close(mut self: Box<Self>) -> Result<(), GraphError> {
        tracing::info!("closing policy_filter ports");
        self.inlet.close().await;
        self.outlet.close().await;
        Ok(())
    }
}

impl<T, C, A, P> PolicyFilter<T, C, A, P>
where
    T: AppData + ToPolar + Clone,
    C: ProctorContext + Debug + Clone,
    P: QueryPolicy<Item = T, Context = C, Args = A>,
{
    #[tracing::instrument(level = "info", name = "make policy knowledge base", skip(self))]
    fn oso(&mut self) -> Result<Oso, PolicyError> {
        let mut oso = Oso::new();

        self.policy.load_policy_engine(&mut oso).map_err(|err| {
            tracing::error!(error=?err, "failed to load policy into knowledge base.");
            err
        })?;

        self.policy.initialize_policy_engine(&mut oso).map_err(|err| {
            tracing::error!(error=?err, "failed to initialize policy knowledge base.");
            err
        })?;

        Ok(oso)
    }

    #[tracing::instrument(level = "trace", skip(tx))]
    fn publish_event(
        event: PolicyFilterEvent<T, C>, tx: &broadcast::Sender<PolicyFilterEvent<T, C>>,
    ) -> Result<(), PolicyError> {
        let nr_notified = tx
            .send(event.clone())
            .map_err(|err| PolicyError::PublishError(err.into()))?;
        tracing::trace!(%nr_notified, ?event, "notified subscribers of policy filter event.");
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "policy_filter handle item", skip(oso, outlet), fields())]
    async fn handle_item(
        item: T, context: &C, policy: &P, oso: &Oso, outlet: &Outlet<PolicyOutcome<T, C>>,
        tx: &broadcast::Sender<PolicyFilterEvent<T, C>>,
    ) -> Result<(), PolicyError> {
        // query lifetime cannot span across `.await` since it cannot `Send` between threads.
        let query_result = policy.query_policy(oso, policy.make_query_args(&item, context));

        tracing::info!(?query_result, "knowledge base query results");

        match query_result {
            // a successful query has Some bindings; otherwise the policy did not pass or errored.
            Ok(QueryResult { bindings: Some(bindings) }) => {
                tracing::info!(
                    ?item,
                    ?bindings,
                    "item and context passed policy review - sending via outlet."
                );
                outlet.send(PolicyOutcome::new(item, context.clone(), bindings)).await?;
                Ok(())
            },

            Ok(_) => {
                tracing::info!(
                    "item and context did not pass policy review (no passing result from knowledge base) - skipping \
                     item."
                );
                Self::publish_event(PolicyFilterEvent::ItemBlocked(item), tx)?;
                Ok(())
            },

            Err(err) => {
                tracing::warn!(error=?err, ?item, ?context, "error in policy review - skipping item.");
                Self::publish_event(PolicyFilterEvent::ItemBlocked(item), tx)?;
                Err(err.into())
            },
        }
    }

    #[tracing::instrument(
        level = "info",
        name = "policy_filter handle item before context set",
        skip(tx),
        fields()
    )]
    fn handle_item_before_context(item: T, tx: &broadcast::Sender<PolicyFilterEvent<T, C>>) -> Result<(), PolicyError> {
        tracing::info!(?item, "dropping item received before policy context set.");
        Self::publish_event(PolicyFilterEvent::ItemBlocked(item), tx)?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "policy_filter handle context", skip(tx), fields())]
    async fn handle_context(
        context: Arc<Mutex<Option<C>>>, recv_context: C, tx: &broadcast::Sender<PolicyFilterEvent<T, C>>,
    ) -> Result<(), PolicyError> {
        tracing::trace!(recv_context=?recv_context, "handling policy context update...");
        let mut ctx = context.lock().await;
        *ctx = Some(recv_context);

        Self::publish_event(PolicyFilterEvent::ContextChanged(ctx.clone()), tx)?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "policy_filter handle command", skip(oso, policy,), fields())]
    async fn handle_command(
        command: PolicyFilterCmd<C>, oso: &mut Oso, name: &str, policy: &P, context: Arc<Mutex<Option<C>>>,
    ) -> Result<bool, PolicyError> {
        tracing::trace!(?command, ?context, "handling policy filter command...");

        match command {
            PolicyFilterCmd::Inspect(tx) => {
                let detail = PolicyFilterDetail {
                    name: name.to_owned(),
                    context: context.lock().await.clone(),
                };
                let _ignore_failure = tx.send(detail);
                Ok(true)
            },

            PolicyFilterCmd::ReplacePolicy { new_policy, tx } => {
                oso.clear_rules();
                match new_policy {
                    PolicySource::String(policy) => oso.load_str(policy.as_str())?,
                    PolicySource::File(path) => oso.load_file(path)?,
                    PolicySource::NoPolicy => (),
                };

                let _ignore_failure = tx.send(());
                Ok(true)
            },

            PolicyFilterCmd::AppendPolicy { additional_policy: policy_source, tx } => {
                match policy_source {
                    PolicySource::String(p) => oso.load_str(p.as_str())?,
                    PolicySource::File(path) => oso.load_file(path)?,
                    PolicySource::NoPolicy => (),
                };

                let _ignore_failure = tx.send(());
                Ok(true)
            },

            PolicyFilterCmd::ResetPolicy(tx) => {
                oso.clear_rules();
                policy.load_policy_engine(oso)?;
                let _ignore_failrue = tx.send(());
                Ok(true)
            },
        }
    }
}

impl<T, C, A, P> stage::WithApi for PolicyFilter<T, C, A, P>
where
    P: QueryPolicy<Item = T, Context = C, Args = A>,
{
    type Sender = PolicyFilterApi<C>;

    #[inline]
    fn tx_api(&self) -> Self::Sender {
        self.tx_api.clone()
    }
}

impl<T, C, A, P> stage::WithMonitor for PolicyFilter<T, C, A, P>
where
    P: QueryPolicy<Item = T, Context = C, Args = A>,
{
    type Receiver = PolicyFilterMonitor<T, C>;

    #[inline]
    fn rx_monitor(&self) -> Self::Receiver {
        self.tx_monitor.subscribe()
    }
}

impl<T, C, A, P> Debug for PolicyFilter<T, C, A, P>
where
    P: QueryPolicy<Item = T, Context = C, Args = A>,
{
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
    use std::collections::HashMap;

    use claim::assert_ok;
    use oso::PolarClass;
    use pretty_assertions::assert_eq;
    use serde::{Deserialize, Serialize};
    use tokio::sync::mpsc;
    use tokio_test::block_on;

    use super::*;
    use crate::elements::telemetry;
    use crate::phases::collection::TelemetrySubscription;

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

    impl PolicySubscription for TestPolicy {
        type Context = TestContext;

        fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
            subscription.with_optional_fields(maplit::hashset! {"foo".to_string(), "score".to_string()})
        }
    }

    impl QueryPolicy for TestPolicy {
        type Args = (Self::Item, &'static str, &'static str);
        type Context = TestContext;
        type Item = User;

        #[tracing::instrument(level = "info", skip(oso))]
        fn load_policy_engine(&self, oso: &mut Oso) -> Result<(), PolicyError> {
            oso.load_str(self.policy.as_str()).map_err(|err| err.into())
        }

        #[tracing::instrument(level = "info", skip(oso))]
        fn initialize_policy_engine(&mut self, oso: &mut Oso) -> Result<(), PolicyError> {
            oso.register_class(User::get_polar_class())?;
            oso.register_class(TestContext::get_polar_class())?;
            Ok(())
        }

        #[tracing::instrument(level = "info")]
        fn make_query_args(&self, item: &Self::Item, _context: &Self::Context) -> Self::Args {
            (item.clone(), "foo", "bar")
        }

        #[tracing::instrument(level = "info", skip(engine))]
        fn query_policy(&self, engine: &Oso, args: Self::Args) -> Result<QueryResult, PolicyError> {
            QueryResult::from_query(engine.query_rule("allow", args)?)
        }
    }

    #[test]
    fn test_handle_item() -> anyhow::Result<()> {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("policy_filter::test_handle_item");
        let _main_span_guard = main_span.enter();

        let policy = TestPolicy::new(r#"allow(actor, _action, _resource) if actor.username.ends_with("example.com");"#);

        let mut policy_filter = PolicyFilter::new("test-policy-filter", policy);
        let oso = assert_ok!(policy_filter.oso()); //.expect("failed to build policy engine");

        let item = User { username: "peter.pan@example.com".to_string() };

        tracing::info!(?item, "entering test section...");

        block_on(async move {
            let (tx, mut rx) = mpsc::channel(4);
            let mut outlet = Outlet::new("test");
            outlet.attach("test_tx", tx).await;

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
                PolicyOutcome::new(
                    User { username: "peter.pan@example.com".to_string() },
                    context,
                    HashMap::default()
                )
            );

            assert!(rx.recv().await.is_none());
        });

        Ok(())
    }
}
