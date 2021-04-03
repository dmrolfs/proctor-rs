use crate::graph::stage::{self, Stage, WithApi, WithMonitor};
use crate::graph::{GraphResult, Inlet, Outlet, Port};
use crate::graph::{Shape, SinkShape, SourceShape, ThroughShape};
use crate::Ack;
use crate::AppData;
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use oso::{Oso, PolarClass};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::Instrument;
use std::sync::Arc;
use tokio::sync::Mutex;

pub trait PolicySettings: fmt::Debug {
    fn specification_path(&self) -> PathBuf;
}

pub trait PolicyContext: fmt::Debug + Send + Sync {
    type Item;
    type Environment;
    fn description(&self) -> &str;
    fn load_policy(&self, oso: &mut oso::Oso) -> GraphResult<()>;
    fn initialize(&self, oso: &mut oso::Oso) -> GraphResult<()>;
    fn do_query_rule(&self, oso: &oso::Oso, item_env: (Self::Item, Self::Environment)) -> GraphResult<oso::Query>;
}

pub type PolicyFilterApi<E> = mpsc::UnboundedSender<PolicyFilterCmd<E>>;
pub type PolicyFilterMonitor<T, E> = broadcast::Receiver<PolicyFilterEvent<T, E>>;

#[derive(Debug)]
pub enum PolicyFilterCmd<E> {
    ReplacePolicy { new_policy: PolicySource, tx: oneshot::Sender<Ack> },
    AppendPolicy { policy: PolicySource, tx: oneshot::Sender<Ack> },
    ResetPolicy(oneshot::Sender<Ack>),
    Inspect(oneshot::Sender<PolicyFilterDetail<E>>)
}

impl<E> PolicyFilterCmd<E> {
    pub fn replace_policy(new_policy: PolicySource) -> (PolicyFilterCmd<E>, oneshot::Receiver<Ack>) {
        let (tx, rx) = oneshot::channel();
        (Self::ReplacePolicy { new_policy, tx }, rx)
    }

    pub fn append_policy(policy: PolicySource) -> (PolicyFilterCmd<E>, oneshot::Receiver<Ack>) {
        let (tx, rx) = oneshot::channel();
        (Self::AppendPolicy { policy, tx }, rx)
    }

    pub fn reset_policy() -> (PolicyFilterCmd<E>, oneshot::Receiver<Ack>) {
        let (tx, rx) = oneshot::channel();
        (Self::ResetPolicy(tx), rx)
    }

    pub fn inspect() -> (PolicyFilterCmd<E>, oneshot::Receiver<PolicyFilterDetail<E>>) {
        let (tx, rx) = oneshot::channel();
        (Self::Inspect(tx), rx)
    }
}

#[derive(Debug)]
pub struct PolicyFilterDetail<E> {
    pub description: String,
    pub environment: Option<E>,
}

#[derive(Debug, Clone)]
pub enum PolicyFilterEvent<T: AppData, E: AppData> {
    EnvironmentChanged(Option<E>),
    ItemBlocked(T),
}

#[derive(Debug)]
pub enum PolicySource {
    String(String),
    File(std::path::PathBuf),
}

pub struct PolicyFilter<T, E>
where
    T: AppData + Clone,
    E: AppData + Clone,
{
    name: String,
    description: String,
    policy_context: Box<dyn PolicyContext<Item = T, Environment = E> + 'static>,
    environment_inlet: Inlet<E>,
    inlet: Inlet<T>,
    outlet: Outlet<T>,
    tx_api: PolicyFilterApi<E>,
    rx_api: mpsc::UnboundedReceiver<PolicyFilterCmd<E>>,
    tx_monitor: broadcast::Sender<PolicyFilterEvent<T, E>>,
}

impl<T, E> PolicyFilter<T, E>
where
    T: AppData + Clone,
    E: AppData + Clone,
{
    pub fn new<S>(name: S, policy: Box<dyn PolicyContext<Item = T, Environment = E>>) -> Self
    where
        S: Into<String>,
        // P: PolicyContext<Item = T, Environment = E> + 'static,
    {
        let name = name.into();
        let environment_inlet = Inlet::new(format!("{}_environment", name.clone()));
        let inlet = Inlet::new(name.clone());
        let outlet = Outlet::new(name.clone());
        let (tx_api, rx_api) = mpsc::unbounded_channel();
        let (tx_monitor, _) = broadcast::channel(num_cpus::get() * 2);
        Self {
            name,
            description: policy.description().to_owned(),
            policy_context: policy,
            environment_inlet,
            inlet,
            outlet,
            tx_api,
            rx_api,
            tx_monitor,
        }
    }

    #[inline]
    pub fn environment_inlet(&mut self) -> &mut Inlet<E> {
        &mut self.environment_inlet
    }
}

impl<T, E> Shape for PolicyFilter<T, E>
where
    T: AppData + Clone,
    E: AppData + Clone,
{
}

impl<T, E> ThroughShape for PolicyFilter<T, E>
where
    T: AppData + Clone,
    E: AppData + Clone,
{
}

impl<T, E> SinkShape for PolicyFilter<T, E>
where
    T: AppData + Clone,
    E: AppData + Clone,
{
    type In = T;
    #[inline]
    fn inlet(&mut self) -> &mut Inlet<Self::In> {
        &mut self.inlet
    }
}

impl<T, E> SourceShape for PolicyFilter<T, E>
where
    T: AppData + Clone,
    E: AppData + Clone,
{
    type Out = T;
    #[inline]
    fn outlet(&mut self) -> &mut Outlet<Self::Out> {
        &mut self.outlet
    }
}

#[dyn_upcast]
#[async_trait]
impl<T, E> Stage for PolicyFilter<T, E>
where
    T: AppData + Clone,
    E: AppData + Clone,
{
    #[inline]
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(
        level="info",
        name="run policy_filter through",
        skip(self),
        fields(stage=%self.name, description=%self.description,)
    )]
    async fn run(&mut self) -> GraphResult<()> {
        let mut oso = self.oso()?;
        let outlet = &self.outlet;
        let item_inlet = &mut self.inlet;
        let env_inlet = &mut self.environment_inlet;
        let context = &self.policy_context;
        let environment: Arc<Mutex<Option<E>>> = Arc::new(Mutex::new(None));
        let description = self.description.as_str();
        let policy_context = &self.policy_context;
        let rx_api = &mut self.rx_api;
        let tx_monitor = &self.tx_monitor;

        loop {
            tracing::info!("receiving next message...");

            tokio::select! {
                item = item_inlet.recv() => match item {
                    Some(item) => {
                        tracing::trace!(?item, ?environment, "handling next item...");

                        match environment.lock().await.as_ref() {
                            Some(env) => PolicyFilter::handle_item(item, env, &context, &oso, outlet, tx_monitor).await?,
                            None => PolicyFilter::<T, E>::handle_item_before_env(item, tx_monitor)?,
                        }
                    },
                    None => {
                        tracing::info!("PolicyFilter item_inlet dropped - completing.");
                        break;
                    }
                },

                Some(env) = env_inlet.recv() => PolicyFilter::handle_environment(environment.clone(), env, tx_monitor).await?,

                Some(command) = rx_api.recv() => {
                    let cont_loop = PolicyFilter::handle_command(
                        command,
                        &mut oso,
                        description,
                        policy_context,
                        environment.clone(),
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
        tracing::trace!("closing policy_filter ports");
        self.inlet.close().await;
        self.outlet.close().await;
        Ok(())
    }
}

impl<T, E> PolicyFilter<T, E>
where
    T: AppData + Clone,
    E: AppData + Clone,
{
    fn oso(&self) -> GraphResult<Oso> {
        let mut oso = Oso::new();
        self.policy_context.initialize(&mut oso)?;
        self.policy_context.load_policy(&mut oso)?;
        Ok(oso)
    }

    #[tracing::instrument(level="trace", skip(tx))]
    fn publish_event(event: PolicyFilterEvent<T, E>, tx: &broadcast::Sender<PolicyFilterEvent<T, E>>) -> GraphResult<()> {
        let nr_notified = tx.send(event.clone()).map_err::<crate::error::GraphError, _>(|err| err.into())?;
        tracing::trace!(%nr_notified, ?event, "notified subscribers of policy filter event.");
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(oso, outlet), fields())]
    async fn handle_item(
        item: T,
        environment: &E,
        context: &Box<dyn PolicyContext<Item = T, Environment = E>>,
        oso: &Oso, outlet: &Outlet<T>,
        tx: &broadcast::Sender<PolicyFilterEvent<T, E>>,
    ) -> GraphResult<()> {
        let result = {
            // query lifetime cannot span across `.await` since it cannot `Send` between threads.
            let mut query = context.do_query_rule(oso, (item.clone(), environment.clone()))?;
            query.next()
        };

        tracing::info!(?result, "knowledge base query results");

        match result {
            Some(Ok(result_set)) => {
                tracing::info!(?result_set, ?item, ?environment, "item passed policy review. sending via outlet...");
                outlet
                    .send(item.clone())
                    .instrument(tracing::info_span!("PolicyFilter.outlet send", ?item))
                    .await?;
                Ok(())
            }

            Some(Err(err)) => {
                tracing::warn!(?item, ?environment, "error in policy review - skipping item.");
                PolicyFilter::publish_event(PolicyFilterEvent::ItemBlocked(item), tx)?;
                Err(err.into())
            }

            None => {
                tracing::info!(?item, ?environment, "empty result from policy review - skipping item.");
                PolicyFilter::publish_event(PolicyFilterEvent::ItemBlocked(item), tx)?;
                Ok(())
            }
        }
    }

    #[tracing::instrument(level="info", skip(tx), fields())]
    fn handle_item_before_env(item: T, tx: &broadcast::Sender<PolicyFilterEvent<T, E>>) -> GraphResult<()> {
        tracing::info!(?item, "dropping item received before policy environment set.");
        PolicyFilter::publish_event(PolicyFilterEvent::ItemBlocked(item), tx)?;
        Ok(())
    }

    #[tracing::instrument(level="info", skip(tx), fields(),)]
    async fn handle_environment(
        environment: Arc<Mutex<Option<E>>>,
        recv_env: E,
        tx: &broadcast::Sender<PolicyFilterEvent<T, E>>,
    ) -> GraphResult<()> {
        tracing::trace!(recv_environment=?recv_env, "handling policy environment update...");
        let mut env = environment.lock().await;
        *env = Some(recv_env);

        PolicyFilter::publish_event(PolicyFilterEvent::EnvironmentChanged(env.clone()), tx)?;
        Ok(())
    }

    #[tracing::instrument(
    level="info",
        skip(oso, description, policy_context,),
        fields()
    )]
    async fn handle_command(
        command: PolicyFilterCmd<E>,
        oso: &mut Oso,
        description: &str,
        policy_context: &Box<dyn PolicyContext<Item = T, Environment = E>>,
        environment: std::sync::Arc<tokio::sync::Mutex<Option<E>>>,
    ) -> GraphResult<bool> {
        tracing::trace!(?command, ?environment, "handling policy filter command...");

        match command {
            PolicyFilterCmd::Inspect(tx) => {
                let detail = PolicyFilterDetail {
                    description: description.to_owned(),
                    environment: environment.lock().await.clone(),
                };
                let _ignore_failure = tx.send(detail);
                Ok(true)
            },

            PolicyFilterCmd::ReplacePolicy { new_policy, tx } => {
                oso.clear_rules();
                match new_policy {
                    PolicySource::String(policy) => oso.load_str(policy.as_str())?,
                    PolicySource::File(path) => oso.load_file(path)?,
                }

                let _ignore_failure = tx.send(());
                Ok(true)
            },

            PolicyFilterCmd::AppendPolicy { policy, tx} => {
                match policy {
                    PolicySource::String(p) => oso.load_str(p.as_str())?,
                    PolicySource::File(path) => oso.load_file(path)?,
                }
                let _ignore_failure = tx.send(());
                Ok(true)
            },

            PolicyFilterCmd::ResetPolicy(tx) => {
                oso.clear_rules();
                policy_context.load_policy(oso)?;
                let _ignore_failrue = tx.send(());
                Ok(true)
            }
        }
    }
}

impl<T, E> stage::WithApi for PolicyFilter<T, E>
    where
        T: AppData + Clone,
        E: AppData + Clone,
{
    type Sender = PolicyFilterApi<E>;

    #[inline]
    fn tx_api(&self) -> Self::Sender { self.tx_api.clone() }
}

impl<T, E> stage::WithMonitor for PolicyFilter<T, E>
    where
        T: AppData + Clone,
        E: AppData + Clone,
{
    type Receiver = PolicyFilterMonitor<T, E>;
    #[inline]
    fn rx_monitor(&self) -> Self::Receiver { self.tx_monitor.subscribe() }
}

impl<T, E> fmt::Debug for PolicyFilter<T, E>
where
    T: AppData + Clone,
    E: AppData + Clone,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PolicyFilter")
            .field("name", &self.name)
            .field("description", &self.description)
            .field("policy_context", &self.policy_context)
            .field("environment_inlet", &self.environment_inlet)
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
    use crate::elements::TelemetryData;
    use crate::error::GraphError;
    use std::collections::HashMap;
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
    struct TestEnvironment {
        #[polar(attribute)]
        pub location_code: u32,
        #[polar(attribute)]
        pub qualities: HashMap<String, String>,
    }

    #[derive(Debug)]
    struct TestPolicy {
        description: String,
        policy: String,
    }

    impl TestPolicy {
        pub fn new<S0: Into<String>, S1: Into<String>>(description: S0, policy: S1) -> Self {
            Self {
                description: description.into(),
                policy: policy.into(),
            }
        }
    }

    impl PolicyContext for TestPolicy {
        type Item = User;
        type Environment = TestEnvironment;

        fn description(&self) -> &str {
            self.description.as_str()
        }

        fn load_policy(&self, oso: &mut Oso) -> GraphResult<()> {
            oso.load_str(self.policy.as_str()).map_err(|err| err.into())
        }

        fn initialize(&self, oso: &mut Oso) -> GraphResult<()> {
            let foo = oso.register_class(User::get_polar_class()).map_err::<GraphError, _>(|err| err.into())?;
            oso.register_class(TestEnvironment::get_polar_class())
                .map_err::<GraphError, _>(|err| err.into())?;
            Ok(())
        }

        fn do_query_rule(&self, oso: &Oso, item_env: (Self::Item, Self::Environment)) -> GraphResult<oso::Query> {
            oso.query_rule("allow", (item_env.0, "foo", "bar")).map_err(|err| err.into())
        }
    }

    #[test]
    fn test_handle_item() -> anyhow::Result<()> {
        lazy_static::initialize(&crate::telemetry::TEST_TRACING);
        let main_span = tracing::info_span!("policy_filter::test_handle_item");
        let _main_span_guard = main_span.enter();

        let policy: Box<dyn PolicyContext<Item = User, Environment = TestEnvironment>> = Box::new(TestPolicy::new(
            "test policy 1",
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

            let (tx_monitor, rx_monitor) = broadcast::channel(4);

            let env = TestEnvironment {
                location_code: 17,
                qualities: maplit::hashmap! {
                    "foo".to_string() => "bar".to_string(),
                    "score".to_string() => 13.to_string(),
                },
            };

            PolicyFilter::handle_item(item, &env, &policy_filter.policy_context, &oso, &outlet, &tx_monitor)
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
