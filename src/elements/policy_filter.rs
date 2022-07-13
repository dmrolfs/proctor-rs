pub use outcome::*;
pub use policy::*;
pub use protocol::*;
pub use result::*;
pub use settings::*;
pub use source::*;

mod outcome;
mod policy;
mod protocol;
mod result;
mod settings;
mod source;

use std::fmt::{self, Debug};
use std::sync::Arc;

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use once_cell::sync::Lazy;
use oso::{Oso, ToPolar};
use prometheus::{HistogramOpts, HistogramTimer, HistogramVec};
use serde::Serialize;
use tokio::sync::Mutex;
use tokio::sync::{broadcast, mpsc};

use crate::error::{PolicyError, PortError};
use crate::graph::stage::{self, Stage};
use crate::graph::{Inlet, Outlet, Port, PORT_CONTEXT, PORT_DATA};
use crate::graph::{SinkShape, SourceShape};
use crate::{track_errors, AppData, Correlation, ProctorContext, ProctorResult};

pub(crate) static POLICY_FILTER_EVAL_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "policy_filter_eval_time",
            "Time spent in PolicyFilter policy evaluation in seconds",
        )
        .const_labels(crate::metrics::CONST_LABELS.clone()),
        &["stage"],
    )
    .expect("failed creating policy_filter_eval_time metric")
});

#[inline]
fn start_policy_timer(stage: &str) -> HistogramTimer {
    POLICY_FILTER_EVAL_TIME.with_label_values(&[stage]).start_timer()
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum PolicyResult {
    Passed,
    Blocked,
    Failed,
}

impl fmt::Display for PolicyResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match *self {
            Self::Passed => "passed",
            Self::Blocked => "blocked",
            Self::Failed => "failed",
        };
        write!(f, "{}", label)
    }
}

pub struct PolicyFilter<T, C, A, P, D>
where
    P: QueryPolicy<Item = T, Context = C, Args = A, TemplateData = D>,
{
    name: String,
    policy: P,
    context_inlet: Inlet<C>,
    inlet: Inlet<T>,
    outlet: Outlet<PolicyOutcome<T, C>>,
    tx_api: PolicyFilterApi<C, D>,
    rx_api: mpsc::UnboundedReceiver<PolicyFilterCmd<C, D>>,
    pub tx_monitor: broadcast::Sender<Arc<PolicyFilterEvent<T, C>>>,
}

impl<T, C, A, P, D> PolicyFilter<T, C, A, P, D>
where
    T: Clone,
    C: Clone,
    P: QueryPolicy<Item = T, Context = C, Args = A, TemplateData = D>,
{
    pub fn new<S: Into<String>>(name: S, mut policy: P) -> Result<Self, PolicyError> {
        Self::check_policy(&mut policy)?;
        let name = name.into();

        let context_inlet = Inlet::new(name.clone(), PORT_CONTEXT);
        let inlet = Inlet::new(name.clone(), PORT_DATA);
        let outlet = Outlet::new(name.clone(), PORT_DATA);
        let (tx_api, rx_api) = mpsc::unbounded_channel();
        let (tx_monitor, _) = broadcast::channel(num_cpus::get() * 2);

        Ok(Self {
            name: name + "_policy_filter_stage",
            policy,
            context_inlet,
            inlet,
            outlet,
            tx_api,
            rx_api,
            tx_monitor,
        })
    }

    fn check_policy(policy: &mut P) -> Result<(), PolicyError> {
        let mut oso = Oso::new();
        policy.load_policy_engine(&mut oso)?;
        Ok(())
    }

    #[inline]
    pub fn context_inlet(&self) -> Inlet<C> {
        self.context_inlet.clone()
    }
}

impl<T, C, A, P, D> SinkShape for PolicyFilter<T, C, A, P, D>
where
    P: QueryPolicy<Item = T, Context = C, Args = A, TemplateData = D>,
{
    type In = T;

    #[inline]
    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

impl<T, C, A, P, D> SourceShape for PolicyFilter<T, C, A, P, D>
where
    P: QueryPolicy<Item = T, Context = C, Args = A, TemplateData = D>,
{
    type Out = PolicyOutcome<T, C>;

    #[inline]
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<T, C, A, P, D> Stage for PolicyFilter<T, C, A, P, D>
where
    T: AppData + Correlation + ToPolar,
    C: ProctorContext,
    A: 'static,
    P: QueryPolicy<Item = T, Context = C, Args = A, TemplateData = D> + 'static,
    D: AppData + Serialize,
{
    #[inline]
    fn name(&self) -> &str {
        &self.name
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", name = "run policy_filter through", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        self.do_run().await?;
        Ok(())
    }

    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        self.do_close().await?;
        Ok(())
    }
}

impl<T, C, A, P, D> PolicyFilter<T, C, A, P, D>
where
    T: AppData + Correlation + ToPolar,
    C: ProctorContext,
    A: 'static,
    P: QueryPolicy<Item = T, Context = C, Args = A, TemplateData = D> + 'static,
    D: AppData + Serialize,
{
    #[inline]
    async fn do_check(&self) -> Result<(), PortError> {
        self.inlet.check_attachment().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    #[inline]
    async fn do_run(&mut self) -> Result<(), PolicyError> {
        let name = self.name().to_string();
        let mut oso = self.oso()?;
        let outlet = &self.outlet;
        let item_inlet = &mut self.inlet;
        let context_inlet = &mut self.context_inlet;
        let policy = &mut self.policy;
        let context: Arc<Mutex<Option<C>>> = Arc::new(Mutex::new(policy.zero_context()));
        let rx_api = &mut self.rx_api;
        let tx_monitor = &self.tx_monitor;

        loop {
            let _timer = stage::start_stage_eval_time(&name);

            tokio::select! {
                item = item_inlet.recv() => match item {
                    Some(item) => {
                        tracing::debug!(?item, ?context, "{} handling next item...", name);

                        match context.lock().await.as_ref() {
                            Some(ctx) => Self::handle_item(&name, item, ctx, policy, &oso, outlet, tx_monitor).await?,
                            None => Self::handle_item_before_context(item, tx_monitor)?,
                        }
                    },
                    None => {
                        tracing::info!("{} PolicyFilter inlet,{:?}, dropped - completing.", name, item_inlet);
                        break;
                    }
                },

                Some(ctx) = context_inlet.recv() => Self::handle_context(context.clone(), ctx, tx_monitor).await?,

                Some(command) = rx_api.recv() => {
                    let cont_loop = Self::handle_command(
                        command,
                        &mut oso,
                        &name,
                        policy,
                        Arc::clone(&context),
                    ).await;

                    if !cont_loop {
                        tracing::trace!("{} policy commanded to stop - breaking...", name);
                        break;
                    }
                },

                else => {
                    tracing::trace!("{} feed into policy depleted - breaking...", name);
                    break;
                },
            }
        }

        Ok(())
    }

    #[inline]
    async fn do_close(mut self: Box<Self>) -> Result<(), PortError> {
        tracing::trace!("closing policy_filter ports");
        self.inlet.close().await;
        self.outlet.close().await;
        Ok(())
    }
}

impl<T, C, A, P, D> PolicyFilter<T, C, A, P, D>
where
    T: AppData + Correlation + ToPolar,
    C: ProctorContext,
    P: QueryPolicy<Item = T, Context = C, Args = A, TemplateData = D>,
    D: AppData + Serialize,
{
    #[tracing::instrument(level = "trace", name = "make policy knowledge base", skip(self))]
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
        event: PolicyFilterEvent<T, C>, tx: &broadcast::Sender<Arc<PolicyFilterEvent<T, C>>>,
    ) -> Result<(), PolicyError> {
        let nr_notified = tx.send(Arc::new(event)).map_err(|err| PolicyError::Publish(err.into()))?;
        tracing::trace!(%nr_notified, "notifying subscribers of policy filter event.");
        Ok(())
    }

    #[tracing::instrument(
        level = "trace",
        name = "policy_filter handle item",
        skip(oso, outlet, tx, policy),
        fields(stage=%name)
    )]
    async fn handle_item(
        name: &str, item: T, context: &C, policy: &P, oso: &Oso, outlet: &Outlet<PolicyOutcome<T, C>>,
        tx: &broadcast::Sender<Arc<PolicyFilterEvent<T, C>>>,
    ) -> Result<(), PolicyError> {
        let _timer = start_policy_timer(name);

        // query lifetime cannot span across `.await` since it cannot `Send` between threads.
        let query_result = policy.query_policy(oso, policy.make_query_args(&item, context));

        match query_result {
            Ok(result) if result.passed => {
                tracing::info!(
                    correlation=?item.correlation(), ?result,
                    "item passed {name} policy review - sending via outlet."
                );
                outlet
                    .send(PolicyOutcome::new(item.clone(), context.clone(), result.clone()))
                    .await?;
                Self::publish_event(PolicyFilterEvent::ItemPassed(item, result), tx)?;
                Ok(())
            },

            Ok(result) => {
                tracing::info!(
                    correlation=?item.correlation(), ?policy, ?result,
                    "item failed {name} policy review - skipping."
                );
                Self::publish_event(PolicyFilterEvent::ItemBlocked(item, Some(result)), tx)?;
                Ok(())
            },

            Err(err) => {
                tracing::error!(
                    correlation=?item.correlation(), error=?err, ?policy,
                    "error in {name} policy review (possible error in policy definition) - skipping item."
                );
                Self::publish_event(PolicyFilterEvent::ItemBlocked(item, None), tx)?;
                Err(err)
            },
        }
    }

    #[tracing::instrument(
        level = "trace",
        name = "policy_filter handle item before context set",
        skip(tx),
        fields()
    )]
    fn handle_item_before_context(
        item: T, tx: &broadcast::Sender<Arc<PolicyFilterEvent<T, C>>>,
    ) -> Result<(), PolicyError> {
        tracing::info!(correlation=?item.correlation(), ?item, "dropping item received before policy context set.");
        Self::publish_event(PolicyFilterEvent::ItemBlocked(item, None), tx)?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", name = "policy_filter handle context", skip(context, tx), fields())]
    async fn handle_context(
        context: Arc<Mutex<Option<C>>>, recv_context: C, tx: &broadcast::Sender<Arc<PolicyFilterEvent<T, C>>>,
    ) -> Result<(), PolicyError> {
        tracing::trace!(recv_context=?recv_context, "handling policy context update...");
        let mut ctx = context.lock().await;
        *ctx = Some(recv_context);

        Self::publish_event(PolicyFilterEvent::ContextChanged(ctx.clone()), tx)?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", name = "policy_filter handle command", skip(oso, policy,), fields())]
    async fn handle_command(
        command: PolicyFilterCmd<C, D>, oso: &mut Oso, name: &str, policy: &mut P, context: Arc<Mutex<Option<C>>>,
    ) -> bool {
        tracing::trace!(?command, ?context, "handling policy filter command...");

        match command {
            PolicyFilterCmd::Inspect(tx) => {
                let detail = PolicyFilterDetail {
                    name: name.to_string(),
                    context: context.lock().await.clone(),
                    policy_template_data: policy.policy_template_data().cloned(),
                };
                let _ignore_failure = tx.send(detail);
                true
            },

            PolicyFilterCmd::ReplacePolicies { new_policies, new_template_data, tx } => {
                Self::do_reset_policy_engine(name, policy, new_policies, new_template_data, oso);
                let _ignore_failure = tx.send(());
                true
            },

            PolicyFilterCmd::AppendPolicy { additional_policy, new_template_data, tx } => {
                let mut sources: Vec<PolicySource> = policy.sources().to_vec();
                sources.push(additional_policy);
                Self::do_reset_policy_engine(name, policy, sources, new_template_data, oso);
                let _ignore_failure = tx.send(());
                true
            },
        }
    }

    fn do_reset_policy_engine(
        name: &str, policy: &mut P, new_policies: Vec<PolicySource>, new_template_data: Option<D>, engine: &mut Oso,
    ) {
        let sources = policy.sources_mut();
        *sources = new_policies;

        let new_data = new_template_data.clone();
        if let Some((template_data, data)) = policy.policy_template_data_mut().zip(new_data) {
            *template_data = data;
        }

        if let Err(err) = policy.load_policy_engine(engine) {
            let policy_type = type_name_of_val(policy);
            tracing::error!(
                policy_stage=%name, error=?err, ?new_template_data,
                "failed to replace policies in {}", policy_type
            );
            track_errors(name, &err.into());
        }
    }
}

fn type_name_of_val<T>(_val: &T) -> &'static str {
    std::any::type_name::<T>()
}

impl<T, C, A, P, D> stage::WithApi for PolicyFilter<T, C, A, P, D>
where
    P: QueryPolicy<Item = T, Context = C, Args = A, TemplateData = D>,
{
    type Sender = PolicyFilterApi<C, D>;

    #[inline]
    fn tx_api(&self) -> Self::Sender {
        self.tx_api.clone()
    }
}

impl<T, C, A, P, D> stage::WithMonitor for PolicyFilter<T, C, A, P, D>
where
    P: QueryPolicy<Item = T, Context = C, Args = A, TemplateData = D>,
{
    type Receiver = PolicyFilterMonitor<T, C>;

    #[inline]
    fn rx_monitor(&self) -> Self::Receiver {
        self.tx_monitor.subscribe()
    }
}

impl<T, C, A, P, D> Debug for PolicyFilter<T, C, A, P, D>
where
    P: QueryPolicy<Item = T, Context = C, Args = A, TemplateData = D>,
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
    use std::collections::HashSet;

    use claim::*;
    use oso::PolarClass;
    use pretty_assertions::assert_eq;
    use pretty_snowflake::{Id, Label};
    use prometheus::Registry;
    use serde::{Deserialize, Serialize};
    use tokio::sync::mpsc;
    use tokio_test::block_on;

    use super::*;
    use crate::elements::telemetry;
    use crate::phases::sense::{SubscriptionRequirements, TelemetrySubscription};

    #[derive(Label, Debug, PartialEq, Clone, PolarClass)]
    struct User {
        pub correlation_id: Id<Self>,
        #[polar(attribute)]
        pub username: String,
    }

    impl Correlation for User {
        type Correlated = Self;

        fn correlation(&self) -> &Id<Self::Correlated> {
            &self.correlation_id
        }
    }

    #[derive(PolarClass, Label, Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestContext {
        pub correlation_id: Id<Self>,

        #[polar(attribute)]
        pub location_code: u32,
        #[polar(attribute)]
        #[serde(flatten)]
        pub qualities: telemetry::TableValue,
    }

    impl Correlation for TestContext {
        type Correlated = Self;

        fn correlation(&self) -> &Id<Self::Correlated> {
            &self.correlation_id
        }
    }

    #[async_trait]
    impl ProctorContext for TestContext {
        type Error = PolicyError;

        fn custom(&self) -> telemetry::TableType {
            (&*self.qualities).clone()
        }
    }

    impl SubscriptionRequirements for TestContext {
        fn required_fields() -> HashSet<String> {
            maplit::hashset! { "location_code".into(), }
        }
    }

    #[derive(Debug)]
    struct TestPolicy(Vec<PolicySource>);

    impl TestPolicy {
        pub fn new<S: AsRef<str>>(policy: S) -> Self {
            let policy = PolicySource::from_complete_string(Self::base_template_name(), policy)
                .expect("failed to make test policy");

            Self(vec![policy])
        }
    }

    impl PolicySubscription for TestPolicy {
        type Requirements = TestContext;

        fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
            subscription.with_optional_fields(maplit::hashset! {"foo".to_string(), "score".to_string()})
        }
    }

    impl QueryPolicy for TestPolicy {
        type Args = (Self::Item, &'static str, &'static str);
        type Context = TestContext;
        type Item = User;
        type TemplateData = ();

        fn base_template_name() -> &'static str {
            "test_policy"
        }

        fn policy_template_data(&self) -> Option<&Self::TemplateData> {
            None
        }

        fn policy_template_data_mut(&mut self) -> Option<&mut Self::TemplateData> {
            None
        }

        fn sources(&self) -> &[PolicySource] {
            &self.0
        }

        fn sources_mut(&mut self) -> &mut Vec<PolicySource> {
            &mut self.0
        }

        #[tracing::instrument(level = "trace", skip(oso))]
        fn initialize_policy_engine(&self, oso: &mut Oso) -> Result<(), PolicyError> {
            oso.register_class(User::get_polar_class())?;
            oso.register_class(TestContext::get_polar_class())?;
            Ok(())
        }

        #[tracing::instrument(level = "trace")]
        fn make_query_args(&self, item: &Self::Item, _context: &Self::Context) -> Self::Args {
            (item.clone(), "foo", "bar")
        }

        #[tracing::instrument(level = "trace", skip(engine))]
        fn query_policy(&self, engine: &Oso, args: Self::Args) -> Result<QueryResult, PolicyError> {
            QueryResult::from_query(engine.query_rule("allow", args)?)
        }
    }

    #[test]
    fn test_handle_item() -> anyhow::Result<()> {
        Lazy::force(&crate::tracing::TEST_TRACING);
        let registry = assert_ok!(Registry::new_custom(Some("test_handle_item".to_string()), None));
        assert_ok!(registry.register(Box::new(POLICY_FILTER_EVAL_TIME.clone())));

        let main_span = tracing::info_span!("policy_filter::test_handle_item");
        let _main_span_guard = main_span.enter();

        let policy = TestPolicy::new(r#"allow(actor, _action, _resource) if actor.username.ends_with("example.com");"#);

        let mut policy_filter = PolicyFilter::new("test-policy-filter", policy)?;
        let oso = assert_ok!(policy_filter.oso()); //.expect("failed to build policy engine");

        let correlation_id = Id::direct("User", 93, "DIUH");

        let item = User {
            correlation_id: correlation_id.clone(),
            username: "peter.pan@example.com".to_string(),
        };

        tracing::info!(?item, "entering test section...");

        block_on(async move {
            let (tx, mut rx) = mpsc::channel(4);
            let mut outlet = Outlet::new("test", PORT_DATA);
            outlet.attach("test_tx".into(), tx).await;

            let (tx_monitor, _rx_monitor) = broadcast::channel(4);

            let context = TestContext {
                correlation_id: Id::direct("TestContext", 123, "ABC"),
                location_code: 17,
                qualities: maplit::hashmap! {
                    "foo".to_string() => "bar".into(),
                    "score".to_string() => 13.into(),
                }
                .into(),
            };

            PolicyFilter::handle_item(
                "test_stage",
                item,
                &context,
                &policy_filter.policy,
                &oso,
                &outlet,
                &tx_monitor,
            )
            .await
            .expect("handle_item failed");

            outlet.close().await;

            let actual = rx.recv().await;
            assert!(actual.is_some());
            assert_eq!(
                actual.unwrap(),
                PolicyOutcome::new(
                    User {
                        correlation_id,
                        username: "peter.pan@example.com".to_string()
                    },
                    context,
                    QueryResult { passed: true, bindings: Bindings::default() }
                )
            );

            assert!(rx.recv().await.is_none());
        });

        // todo: this is simple experiment with metrics to eval how it may be used in testing.
        assert_eq!(registry.gather().len(), 1);

        Ok(())
    }
}
