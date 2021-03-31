use crate::graph::stage::Stage;
use crate::graph::{GraphResult, Inlet, Outlet, Port};
use crate::graph::{Shape, SinkShape, SourceShape, ThroughShape};
use crate::AppData;
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use oso::{Oso, PolarClass};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::{Path, PathBuf};
use std::pin::Pin;

pub trait PolicySettings: fmt::Debug {
    fn specification_path(&self) -> PathBuf;
}

pub trait PolicyContext: fmt::Debug + Send + Sync {
    type Item;
    type Environment;
    fn description(&self) -> &str;
    fn load_policy(&self, engine: &mut oso::Oso) -> GraphResult<()>;
    fn initialize(&self, engine: &mut oso::Oso) -> GraphResult<()>;
    fn do_query_rule(&self, engine: &oso::Oso, item_env: (Self::Item, Self::Environment)) -> GraphResult<oso::Query>;
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
        Self {
            name,
            description: policy.description().to_owned(),
            policy_context: policy,
            environment_inlet,
            inlet,
            outlet,
        }
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
        let oso = self.oso()?;
        let outlet = &self.outlet;
        let item_rx = &mut self.inlet;
        let env_rx = &mut self.environment_inlet;
        let context = &self.policy_context;
        let mut environment: Option<E> = None;

        // let foo = item_rx.recv().unwrap();
        loop {
            tokio::select! {
                Some(item) = item_rx.recv() => match environment.as_ref() {
                    Some(env) => PolicyFilter::handle_item(item, env, &context, &oso, outlet).await?,
                    None => PolicyFilter::<T, E>::handle_item_before_env(item)?,
                },

                Some(env) = env_rx.recv() => {
                    environment = Some(env);
                },

                else => {
                    tracing::warn!("feed into policy depleted - breaking...");
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

    #[tracing::instrument(
        level="info",
        skip(oso, outlet),
        fields()
    )]
    async fn handle_item(
        item: T, environment: &E, context: &Box<dyn PolicyContext<Item = T, Environment = E>>, oso: &Oso, outlet: &Outlet<T>,
    ) -> GraphResult<()> {
        let result = {
            // query lifetime cannot span across `.await` since it cannot `Send` between threads.
            let mut query = context.do_query_rule(oso, (item.clone(), environment.clone()))?;
            query.next()
        };

        match result {
            Some(Ok(result_set)) => {
                tracing::info!(?result_set, ?item, ?environment, "item passed policy review. sending via outlet...");
                outlet.send(item).await
            }

            Some(Err(err)) => {
                tracing::warn!(?item, ?environment, "error in policy review.");
                Err(err.into())
            }

            None => {
                tracing::info!(?item, ?environment, "empty result from policy review - skipping item.");
                Ok(())
            }
        }
    }

    #[tracing::instrument(level="info", skip(item), fields(?item))]
    fn handle_item_before_env(item: T) -> GraphResult<()> {
        tracing::info!("dropping item received before policy environment set.");
        Ok(())
    }
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
    use crate::error::GraphError;
    use crate::phases::collection::TelemetryData;
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
            oso.load_str(r#"allow(actor, action, resource) if actor.username.ends_with("example.com");"#)
                .map_err(|err| err.into())
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
        let subscriber = crate::telemetry::get_subscriber("proctor", "trace");
        crate::telemetry::init_subscriber(subscriber);

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

            let env = TestEnvironment {
                location_code: 17,
                qualities: maplit::hashmap! {
                    "foo".to_string() => "bar".to_string(),
                    "score".to_string() => 13.to_string(),
                },
            };

            PolicyFilter::handle_item(item, &env, &policy_filter.policy_context, &oso, &outlet)
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
