pub mod magnet;
pub mod protocol;
pub mod subscription;

use std::collections::HashSet;
use std::fmt::{self, Debug};

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use futures::future::FutureExt;
pub use magnet::*;
use once_cell::sync::Lazy;
use pretty_snowflake::Id;
use prometheus::{IntCounterVec, IntGauge, Opts};
pub use protocol::*;
pub use subscription::*;
use tokio::sync::mpsc;

use crate::elements::{Telemetry, Timestamp};
use crate::error::{CollectionError, ProctorError};
use crate::graph::stage::Stage;
use crate::graph::{stage, Inlet, OutletsShape, Port, SinkShape, UniformFanOutShape};
use crate::{ProctorIdGenerator, ProctorResult, SharedString};

pub(crate) static SUBSCRIPTIONS_GAUGE: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "clearinghouse_subscriptions",
        "Number of active data subscriptions to the telemetry clearinghouse.",
    )
    .expect("failed creating clearinghouse_subscriptions metric")
});

pub(crate) static PUBLICATIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("clearinghouse_publications", "Count of subscription publications"),
        &["subscription"],
    )
    .expect("failed creating clearninghouse_publications metric")
});

#[inline]
fn track_subscriptions(count: usize) {
    SUBSCRIPTIONS_GAUGE.set(count as i64);
}

#[inline]
fn track_publications(subscription: &str) {
    PUBLICATIONS.with_label_values(&[subscription]).inc();
}

pub const SUBSCRIPTION_TIMESTAMP: &str = "timestamp";
pub const SUBSCRIPTION_CORRELATION: &str = "correlation_id";

pub type CorrelationGenerator = ProctorIdGenerator<Telemetry>;

/// Clearinghouse is a sink for collected telemetry data and a subscription-based source for
/// groups of telemetry fields.
pub struct Clearinghouse {
    name: SharedString,
    subscriptions: Vec<TelemetrySubscription>,
    database: Telemetry,
    correlation_generator: CorrelationGenerator,
    inlet: Inlet<Telemetry>,
    tx_api: ClearinghouseApi,
    rx_api: mpsc::UnboundedReceiver<ClearinghouseCmd>,
}

const PORT_TELEMETRY: &str = "telemetry";

impl Clearinghouse {
    pub fn new(name: impl Into<SharedString>, correlation_generator: CorrelationGenerator) -> Self {
        let name = name.into();
        let (tx_api, rx_api) = mpsc::unbounded_channel();
        let inlet = Inlet::new(name.clone(), PORT_TELEMETRY);

        Self {
            name,
            subscriptions: Vec::default(),
            database: Telemetry::default(),
            correlation_generator,
            inlet,
            tx_api,
            rx_api,
        }
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn subscribe(&mut self, subscription: TelemetrySubscription, receiver: &Inlet<Telemetry>) {
        tracing::info!(stage=%self.name, ?subscription, "adding clearinghouse subscription.");
        subscription.connect_to_receiver(receiver).await;
        self.subscriptions.push(subscription);
        track_subscriptions(self.subscriptions.len());
    }

    #[tracing::instrument(level = "trace", skip(subscriptions, database,))]
    async fn handle_telemetry_data(
        stage_name: &SharedString, data: Option<Telemetry>, subscriptions: &[TelemetrySubscription],
        database: &mut Telemetry, correlation_generator: &mut CorrelationGenerator,
    ) -> Result<bool, CollectionError> {
        match data {
            Some(d) => {
                let updated_fields = d.keys().cloned().collect::<HashSet<_>>();
                let interested =
                    Self::find_interested_subscriptions(subscriptions, database.keys().collect(), updated_fields);

                database.extend(d);
                Self::push_to_subscribers(stage_name, database, interested, correlation_generator).await?;
                Ok(true)
            },

            None => {
                tracing::info!(
                    "telemetry sources dried up - stopping since subscribers have data they're going to get."
                );
                Ok(false)
            },
        }
    }

    #[tracing::instrument(level = "trace", skip(subscriptions,))]
    fn find_interested_subscriptions<'s>(
        subscriptions: &'s [TelemetrySubscription], available: HashSet<&String>, changed: HashSet<String>,
    ) -> Vec<&'s TelemetrySubscription> {
        let interested = subscriptions
            .iter()
            .filter(|s| {
                let is_interested = s.any_interest(&available, &changed);
                tracing::trace!(
                    "is subscription, {}, interested in changed fields:{:?} => {}",
                    s.name(),
                    changed,
                    is_interested
                );
                is_interested
            })
            .collect::<Vec<_>>();

        tracing::info!(
            nr_subscriptions=%subscriptions.len(),
            nr_interested=%interested.len(),
            ?interested,
            "interested subscriptions {}.",
            if interested.is_empty() { "not found"} else { "found" }
        );

        interested
    }

    #[tracing::instrument(
        level = "info",
        skip(database, subscribers, correlation_generator),
        fields(subscribers = ?subscribers.iter().map(|s| s.name()).collect::<Vec<_>>(), ),
    )]
    async fn push_to_subscribers(
        stage_name: &SharedString, database: &Telemetry, subscribers: Vec<&TelemetrySubscription>,
        correlation_generator: &mut CorrelationGenerator,
    ) -> Result<(), CollectionError> {
        if subscribers.is_empty() {
            tracing::info!("not publishing - no subscribers corresponding to field changes.");
            return Ok(());
        }

        let nr_subscribers = subscribers.len();
        let now = Timestamp::now();

        let fulfilled = subscribers
            .into_iter()
            .map(|s| Self::fulfill_subscription(s, database).map(|fulfillment| (s, fulfillment)))
            .flatten()
            .map(|(s, mut fulfillment)| {
                // auto-filled properties
                fulfillment.insert(SUBSCRIPTION_TIMESTAMP.to_string(), now.into());
                let correlation_id: Id<Telemetry> = correlation_generator.next_id();
                fulfillment.insert(SUBSCRIPTION_CORRELATION.to_string(), correlation_id.clone().into());

                tracing::info!(subscription=%s.name(), ?correlation_id, "sending subscription data update.");
                Self::update_metrics(s, &fulfillment);
                s.send(fulfillment).map(move |send_status| {
                    track_publications(s.name().as_ref());
                    (s, send_status)
                })
            })
            .collect::<Vec<_>>();

        let nr_fulfilled = fulfilled.len();

        let statuses = futures::future::join_all(fulfilled).await;
        let result = if let Some((s, Err(err))) = statuses.into_iter().find(|(_, status)| status.is_err()) {
            tracing::error!(subscription=%s.name(), "failed to send fulfilled subscription.");
            // todo: change to track *all* errors -- only first found is currently tracked
            // todo: resolve design to avoid this hack to satisfy track_errors api while not exposing
            // collection stage with proctor error.
            let proctor_err = ProctorError::CollectionError(err);
            crate::graph::track_errors(stage_name.as_ref(), &proctor_err);
            let err = match proctor_err {
                ProctorError::CollectionError(err) => Some(err),
                err => {
                    tracing::error!(error=?err, "Unexpected error in clearinghouse");
                    None
                },
            }
            .unwrap();

            Err(err)
        } else {
            Ok(())
        };

        tracing::info!(
            nr_fulfilled=%nr_fulfilled,
            nr_not_fulfilled=%(nr_subscribers - nr_fulfilled),
            sent_ok=%result.is_ok(),
            "published to subscriptions"
        );

        result
    }

    #[tracing::instrument(level = "info")]
    fn fulfill_subscription(subscription: &TelemetrySubscription, database: &Telemetry) -> Option<Telemetry> {
        subscription.fulfill(database)
    }

    #[tracing::instrument(level = "info", skip(subscription, telemetry))]
    fn update_metrics(subscription: &TelemetrySubscription, telemetry: &Telemetry) {
        subscription.update_metrics(telemetry)
    }

    #[tracing::instrument(level = "trace", skip(subscriptions, database))]
    async fn handle_command(
        command: ClearinghouseCmd, subscriptions: &mut Vec<TelemetrySubscription>, database: &Telemetry,
    ) -> Result<bool, CollectionError> {
        match command {
            ClearinghouseCmd::GetSnapshot { name, tx } => {
                let snapshot = match name {
                    None => {
                        tracing::info!("no subscription specified - responding with clearinghouse snapshot.");
                        ClearinghouseSnapshot {
                            database: database.clone(),
                            missing: HashSet::default(),
                            subscriptions: subscriptions.clone(),
                        }
                    },

                    Some(name) => match subscriptions.iter().find(|s| s.name() == name.as_str()) {
                        Some(sub) => {
                            let (db, missing) = sub.trim_to_subscription(database)?;

                            tracing::info!(
                                requested_subscription=%name,
                                data=?db,
                                missing=?missing,
                                "subscription found - focusing snapshot."
                            );

                            ClearinghouseSnapshot {
                                database: db,
                                missing,
                                subscriptions: vec![sub.clone()],
                            }
                        },

                        None => {
                            tracing::info!(requested_subscription=%name, "subscription not found - returning clearinghouse snapshot.");
                            ClearinghouseSnapshot {
                                database: database.clone(),
                                missing: HashSet::default(),
                                subscriptions: subscriptions.clone(),
                            }
                        },
                    },
                };

                let _ = tx.send(snapshot);
                Ok(true)
            },

            ClearinghouseCmd::Subscribe { subscription, receiver, tx } => {
                tracing::info!(?subscription, "adding telemetry subscriber.");
                subscription.connect_to_receiver(&receiver).await;
                subscriptions.push(*subscription);
                track_subscriptions(subscriptions.len());
                let _ = tx.send(());
                Ok(true)
            },

            ClearinghouseCmd::Unsubscribe { name, tx } => {
                // let mut subs = subscriptions.lock().await;
                let dropped = subscriptions.iter().position(|s| s.name() == name.as_str()).map(|pos| {
                    let _dropped = subscriptions.remove(pos);
                    track_subscriptions(subscriptions.len());
                });

                tracing::info!(?dropped, "subscription dropped");
                let _ = tx.send(());
                Ok(true)
            },
        }
    }
}

impl Debug for Clearinghouse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clearinghouse")
            .field("name", &self.name)
            .field("subscriptions", &self.subscriptions)
            .field("data", &self.database)
            .finish()
    }
}

impl SinkShape for Clearinghouse {
    type In = Telemetry;

    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

impl UniformFanOutShape for Clearinghouse {
    type Out = Telemetry;

    fn outlets(&self) -> OutletsShape<Self::Out> {
        self.subscriptions.iter().map(|s| s.outlet_to_subscription()).collect()
    }
}

#[dyn_upcast]
#[async_trait]
impl Stage for Clearinghouse {
    fn name(&self) -> SharedString {
        self.name.clone()
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run clearinghouse", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        self.do_run().await?;
        Ok(())
    }

    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        self.do_close().await?;
        Ok(())
    }
}

impl Clearinghouse {
    async fn do_check(&self) -> Result<(), CollectionError> {
        self.inlet.check_attachment().await?;

        for s in self.subscriptions.iter() {
            s.outlet_to_subscription().check_attachment().await?;
        }

        Ok(())
    }

    async fn do_run(&mut self) -> Result<(), CollectionError> {
        let stage_name = &self.name;
        let mut inlet = self.inlet.clone();
        let rx_api = &mut self.rx_api;
        let database = &mut self.database;
        let correlation_generator = &mut self.correlation_generator;
        let subscriptions = &mut self.subscriptions;

        loop {
            tracing::trace!(
                nr_subscriptions=%subscriptions.len(),
                subscriptions=?subscriptions,
                database=?database,
                "handling next item.."
            );

            tokio::select! {
                data = inlet.recv() => {
                    let cont_loop = Self::handle_telemetry_data(
                        stage_name,
                        data,
                        subscriptions,
                        database,
                        correlation_generator
                    ).await?;

                    if !cont_loop {
                        break;
                    }
                },

                Some(command) = rx_api.recv() => {
                    let cont_loop = Self::handle_command(
                        command,
                        subscriptions,
                        database,
                    )
                    .await?;

                    if !cont_loop {
                        break;
                    }
                },

                else => {
                    tracing::trace!("clearinghouse done");
                    break;
                }
            }
        }

        Ok(())
    }

    async fn do_close(mut self: Box<Self>) -> Result<(), CollectionError> {
        self.inlet.close().await;
        for s in self.subscriptions {
            s.close().await;
        }
        self.rx_api.close();
        Ok(())
    }
}

impl stage::WithApi for Clearinghouse {
    type Sender = ClearinghouseApi;

    fn tx_api(&self) -> Self::Sender {
        self.tx_api.clone()
    }
}

// /////////////////////////////////////////////////////
// // Unit Tests ///////////////////////////////////////
//
#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::convert::TryFrom;
    use std::convert::TryInto;
    use std::sync::Arc;
    use std::time::Duration;

    use claim::*;
    use pretty_assertions::assert_eq;
    use pretty_snowflake::{Id, IdPrettifier, PrettyIdGenerator};
    use prometheus::{IntCounterVec, IntGaugeVec, Opts, Registry};
    use tokio::sync::oneshot;
    use tokio_test::block_on;
    use tracing::Instrument;

    use super::*;
    use crate::elements::telemetry::ToTelemetry;
    use crate::graph::stage::{self, Stage, WithApi};
    use crate::graph::{Connect, Outlet, SinkShape, SourceShape, PORT_DATA};

    static ID_GENERATOR: Lazy<CorrelationGenerator> =
        Lazy::new(|| PrettyIdGenerator::single_node(IdPrettifier::default()));
    static CAT_COUNTS: Lazy<IntCounterVec> = Lazy::new(|| {
        IntCounterVec::new(Opts::new("cat_counts", "How many cats were found."), &["subscription"])
            .expect("failed to create cat_counts metric")
    });
    static CURRENT_POS: Lazy<IntGaugeVec> = Lazy::new(|| {
        IntGaugeVec::new(Opts::new("current_pos", "current position"), &["subscription"])
            .expect("failed to create current_pos metric")
    });

    fn setup_metrics(name: impl Into<String>) -> Registry {
        let registry = assert_ok!(Registry::new_custom(Some(name.into()), None));
        assert_ok!(registry.register(Box::new(CAT_COUNTS.clone())));
        assert_ok!(registry.register(Box::new(CURRENT_POS.clone())));
        registry
    }

    static SUBSCRIPTIONS: Lazy<Vec<TelemetrySubscription>> = Lazy::new(|| {
        vec![
            TelemetrySubscription::Explicit {
                name: "none".into(),
                required_fields: HashSet::default(),
                optional_fields: HashSet::default(),
                outlet_to_subscription: Outlet::new("none_outlet", PORT_DATA),
                update_metrics: None,
            },
            TelemetrySubscription::Explicit {
                name: "cat_pos".into(),
                required_fields: maplit::hashset! {"pos".into(), "cat".into()},
                optional_fields: maplit::hashset! {"extra".into()},
                outlet_to_subscription: Outlet::new("cat_pos_outlet", PORT_DATA),
                update_metrics: Some(Arc::new(Box::new(|subscription, telemetry| {
                    let update_span = tracing::info_span!(
                        "update_cat_pos_subscription_metrics", %subscription, ?telemetry
                    );
                    let _guard = update_span.enter();

                    let cat_found = telemetry.iter().find(|(k, _)| k.as_str() == "cat").is_some();
                    if cat_found {
                        tracing::info!(%cat_found, "recording CAT_COUNTS metric..");
                        CAT_COUNTS.with_label_values(&[subscription.as_ref()]).inc();
                    }

                    let pos: Option<i64> = telemetry.iter().find_map(|(k, v)| {
                        if k.as_str() == "pos" {
                            let v: Option<i64> = v.clone().try_into().ok();
                            v
                        } else {
                            None
                        }
                    });
                    if let Some(p) = pos {
                        tracing::info!(?pos, "recording CURRENT_POS metric...");
                        CURRENT_POS.with_label_values(&[subscription.as_ref()]).set(p);
                    }
                }))),
            },
            TelemetrySubscription::Explicit {
                name: "all".into(),
                required_fields: maplit::hashset! {"pos".into(), "cat".into(), "value".into()},
                optional_fields: HashSet::default(),
                outlet_to_subscription: Outlet::new("all_outlet", PORT_DATA),
                update_metrics: None,
            },
        ]
    });

    static DB_ROWS: Lazy<Vec<Telemetry>> = Lazy::new(|| {
        vec![
            maplit::btreemap! {"pos".to_string() => 1.into(), "cat".to_string() => "Stella".into(), "extra".to_string() => "Ripley".into(),}.into_iter().collect(),
            maplit::btreemap! {"value".to_string() => 3.14159.into(), "cat".to_string() => "Otis".into(), "extra".to_string() => "Ripley".into(),}.into_iter().collect(),
            maplit::btreemap! {"pos".to_string() => 3.into(), "cat".to_string() => "Neo".into(), "extra".to_string() => "Ripley".into(),}.into_iter().collect(),
            maplit::btreemap! {"pos".to_string() => 4.into(), "value".to_string() => 2.71828.into(), "cat".to_string() => "Apollo".into(), "extra".to_string() => "Ripley".into(),}.into_iter().collect(),
        ]
    });

    static EXPECTED: Lazy<HashMap<String, Vec<Option<Telemetry>>>> = Lazy::new(|| {
        maplit::hashmap! {
            SUBSCRIPTIONS[0].name().to_string() => vec![
                None,
                None,
                None,
                None,
            ],
            SUBSCRIPTIONS[1].name().to_string() => vec![
                Some(maplit::btreemap! {"pos".to_string() => 1.into(), "cat".to_string() => "Stella".into(), "extra".to_string() => "Ripley".into(),}.into_iter().collect()),
                None,
                Some(maplit::btreemap! {"pos".to_string() => 3.into(), "cat".to_string() => "Neo".into(), "extra".to_string() => "Ripley".into(),}.into_iter().collect()),
                Some(maplit::btreemap! {"pos".to_string() => 4.into(), "cat".to_string() => "Apollo".into(), "extra".to_string() => "Ripley".into(),}.into_iter().collect()),
            ],
            SUBSCRIPTIONS[2].name().to_string() => vec![
                None,
                None,
                None,
                Some(maplit::btreemap! {"pos".to_string() => 4.into(), "value".to_string() => 2.71828.into(), "cat".to_string() => "Apollo".into(),}.into_iter().collect()),
            ],
        }
    });

    #[test]
    fn test_create_with_subscriptions() -> anyhow::Result<()> {
        Lazy::force(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_create_with_subscriptions");
        let _main_span_guard = main_span.enter();

        let mut clearinghouse = Clearinghouse::new("test", ID_GENERATOR.clone());
        assert!(clearinghouse.database.is_empty());
        assert!(clearinghouse.subscriptions.is_empty());
        assert_eq!(clearinghouse.name, "test");

        let sub1_inlet = Inlet::new("sub1", PORT_DATA);
        block_on(async {
            assert!(!clearinghouse.inlet.is_attached().await);

            clearinghouse
                .subscribe(
                    TelemetrySubscription::new("sub1")
                        .with_required_fields(maplit::hashset! {"aaa", "bbb"})
                        .with_optional_fields(HashSet::<&str>::default()),
                    &sub1_inlet,
                )
                .await;
        });
        assert_eq!(clearinghouse.subscriptions.len(), 1);
        block_on(async {
            assert!(sub1_inlet.is_attached().await);
        });
        Ok(())
    }

    #[test]
    fn test_api_add_subscriptions() -> anyhow::Result<()> {
        Lazy::force(&crate::tracing::TEST_TRACING);
        const SPAN_NAME: &'static str = "test_api_add_subscriptions";
        let main_span = tracing::info_span!(SPAN_NAME);
        let _main_span_guard = main_span.enter();

        block_on(async move {
            let data: Telemetry = maplit::hashmap! { "aaa".to_string() => 17.to_telemetry() }
                .into_iter()
                .collect();
            let mut tick = stage::Tick::new("tick", Duration::from_nanos(0), Duration::from_millis(5), data);
            let tx_tick_api = tick.tx_api();
            let mut clearinghouse = Clearinghouse::new("test-clearinghouse", ID_GENERATOR.clone());
            let tx_api = clearinghouse.tx_api();
            (tick.outlet(), clearinghouse.inlet()).connect().await;

            let tick_handle = tokio::spawn(async move { tick.run().await });
            let clear_handle = tokio::spawn(
                async move { clearinghouse.run().await }.instrument(tracing::info_span!("spawn clearinghouse")),
            );

            let nr_0_span = tracing::info_span!("nr_subscriptions is 0");
            let _ = nr_0_span.enter();

            let (get_0, rx_get_0) = ClearinghouseCmd::get_clearinghouse_snapshot();
            tx_api.send(get_0)?;
            let ClearinghouseSnapshot {
                database: _db_0,
                missing: _missing_0,
                subscriptions: subs_0,
            } = rx_get_0.await?;
            let nr_subscriptions = subs_0.len();
            tracing::info!(%nr_subscriptions, "assert nr_subscriptions is 0...");
            assert_eq!(nr_subscriptions, 0);

            tracing::info!("sending add subscriber command to clearinghouse...");
            let sub1_inlet = Inlet::new("sub1", PORT_DATA);
            let (add_cmd, rx_add) = ClearinghouseCmd::subscribe(
                TelemetrySubscription::new("sub1")
                    .with_required_fields(maplit::hashset! {"aaa", "bbb"})
                    .with_optional_fields(HashSet::<&str>::default()),
                sub1_inlet.clone(),
            );
            tx_api.send(add_cmd)?;
            tracing::info!("waiting for api confirmation...");
            rx_add.await?;

            let nr_1_span = tracing::info_span!("nr_subscriptions is 1");
            let _ = nr_1_span.enter();

            let (get_1, rx_get_1) = ClearinghouseCmd::get_clearinghouse_snapshot();
            tx_api.send(get_1)?;
            let ClearinghouseSnapshot {
                database: _db_1,
                missing: _missing_1,
                subscriptions: subs_1,
            } = rx_get_1.await?;
            let nr_subscriptions = subs_1.len();
            tracing::info!(%nr_subscriptions, "assert nr_subscriptions is now 1...");
            assert_eq!(nr_subscriptions, 1);

            tracing::info!("stopping tick source...");
            let (tx_tick_stop, rx_tick_stop) = oneshot::channel();
            let stop_tick = stage::tick::TickMsg::Stop { tx: tx_tick_stop };
            tx_tick_api.send(stop_tick)?;
            rx_tick_stop.await??;

            tracing::info!("waiting for clearinghouse to stop...");
            tick_handle.await??;
            clear_handle.await??;
            tracing::info!("test finished");
            anyhow::Result::<()>::Ok(())
        })?;

        Ok(())
    }

    #[test]
    fn test_api_remove_subscriptions() -> anyhow::Result<()> {
        Lazy::force(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_api_remove_subscriptions");
        let _main_span_guard = main_span.enter();

        block_on(async move {
            let data: Telemetry = maplit::hashmap! { "dr".to_string() => 17.to_telemetry() }
                .into_iter()
                .collect();
            let mut tick = stage::Tick::new("tick", Duration::from_nanos(0), Duration::from_millis(5), data);
            let tx_tick_api = tick.tx_api();
            let mut clearinghouse = Clearinghouse::new("test-clearinghouse", ID_GENERATOR.clone());
            let tx_api = clearinghouse.tx_api();
            (tick.outlet(), clearinghouse.inlet()).connect().await;

            let tick_handle = tokio::spawn(async move { tick.run().await });
            let clear_handle = tokio::spawn(async move { clearinghouse.run().await });

            let inlet_1 = Inlet::new("inlet_1", PORT_DATA);
            let (add, rx_add) = ClearinghouseCmd::subscribe(
                TelemetrySubscription::new("sub1")
                    .with_required_fields(maplit::hashset! { "dr" })
                    .with_optional_fields(HashSet::<String>::default()),
                inlet_1.clone(),
            );
            tx_api.send(add)?;

            let (get_1, rx_get_1) = ClearinghouseCmd::get_clearinghouse_snapshot();
            tx_api.send(get_1)?;

            rx_add.await?;
            let ClearinghouseSnapshot { database: _, missing: _, subscriptions: subs1 } = rx_get_1.await?;
            assert_eq!(subs1.len(), 1);

            let name_1 = subs1[0].name();
            let (remove, rx_remove) = ClearinghouseCmd::unsubscribe(name_1);
            tx_api.send(remove)?;
            rx_remove.await?;

            let (get_2, rx_get_2) = ClearinghouseCmd::get_clearinghouse_snapshot();
            tx_api.send(get_2)?;
            let ClearinghouseSnapshot { database: _, missing: _, subscriptions: subs2 } = rx_get_2.await?;
            assert_eq!(subs2.len(), 0);

            tracing::info!("stopping tick source...");
            let (stop_tick, _) = stage::tick::TickMsg::stop();
            tx_tick_api.send(stop_tick)?;

            tick_handle.await??;
            clear_handle.await??;
            Ok(())
        })
    }

    #[test]
    fn test_find_interested_subscriptions() {
        Lazy::force(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_find_interested_subscriptions");
        let _main_span_guard = main_span.enter();

        let actual =
            Clearinghouse::find_interested_subscriptions(&SUBSCRIPTIONS, HashSet::default(), HashSet::default());
        assert_eq!(actual, Vec::<&TelemetrySubscription>::default());

        let available = "extra".to_string();
        let actual = Clearinghouse::find_interested_subscriptions(
            &SUBSCRIPTIONS,
            maplit::hashset! {&available},
            maplit::hashset! {"extra".to_string()},
        );
        assert_eq!(actual, vec![&SUBSCRIPTIONS[1]]);

        let available = "nonsense".to_string();
        let actual = Clearinghouse::find_interested_subscriptions(
            &SUBSCRIPTIONS,
            maplit::hashset! {&available},
            maplit::hashset! {"nonsense".to_string()},
        );
        assert_eq!(actual, Vec::<&TelemetrySubscription>::default());

        let available = "pos".to_string();
        let actual = Clearinghouse::find_interested_subscriptions(
            &SUBSCRIPTIONS,
            maplit::hashset! {&available},
            maplit::hashset! {"pos".to_string()},
        );
        assert_eq!(actual, vec![&SUBSCRIPTIONS[1], &SUBSCRIPTIONS[2]]);

        let available = "value".to_string();
        let actual = Clearinghouse::find_interested_subscriptions(
            &SUBSCRIPTIONS,
            maplit::hashset! {&available},
            maplit::hashset! {"value".to_string()},
        );
        assert_eq!(actual, vec![&SUBSCRIPTIONS[2]]);

        let base = maplit::hashset! {"pos".to_string(), "cat".to_string()};
        let available = base.iter().collect::<HashSet<_>>();
        let actual = Clearinghouse::find_interested_subscriptions(
            &SUBSCRIPTIONS,
            available,
            maplit::hashset! {"pos".to_string(), "cat".to_string()},
        );
        assert_eq!(actual, vec![&SUBSCRIPTIONS[1], &SUBSCRIPTIONS[2]]);

        let base = maplit::hashset! {"pos".to_string(), "value".to_string(), "cat".to_string()};
        let available = base.iter().collect::<HashSet<_>>();
        let actual = Clearinghouse::find_interested_subscriptions(
            &SUBSCRIPTIONS,
            available,
            maplit::hashset! {"pos".to_string(), "value".to_string(), "cat".to_string()},
        );
        assert_eq!(actual, vec![&SUBSCRIPTIONS[1], &SUBSCRIPTIONS[2]]);

        let base = maplit::hashset! {"pos".to_string(), "value".to_string(), "cat".to_string(), "extra".to_string()};
        let available = base.iter().collect::<HashSet<_>>();
        let actual = Clearinghouse::find_interested_subscriptions(
            &SUBSCRIPTIONS,
            available,
            maplit::hashset! {"pos".to_string(), "value".to_string(), "cat".to_string(), "extra".to_string()},
        );
        assert_eq!(actual, vec![&SUBSCRIPTIONS[1], &SUBSCRIPTIONS[2]]);
    }

    #[test]
    fn test_fulfill_subscription() {
        Lazy::force(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_fulfill_subscription");
        let _main_span_guard = main_span.enter();

        for subscriber in 0..=2 {
            // let sub = &SUBSCRIPTIONS[subscriber];
            if let TelemetrySubscription::Explicit {
                name: sub_name,
                required_fields: sub_required_fields,
                optional_fields: sub_optional_fields,
                ..
            } = &SUBSCRIPTIONS[subscriber]
            {
                let expected = &EXPECTED[sub_name.as_ref()];
                tracing::info!(
                    nr=%subscriber,
                    subscriber_name=%sub_name,
                    subscriber_required_fields=?sub_required_fields,
                    subscriber_optional_fields=?sub_optional_fields,
                    ?expected,
                    "next test scenario..."
                );
                for ((row, data_row), expected_row) in DB_ROWS.iter().enumerate().zip(expected) {
                    let actual = Clearinghouse::fulfill_subscription(
                        &TelemetrySubscription::new(sub_name)
                            .with_required_fields(sub_required_fields.clone())
                            .with_optional_fields(sub_optional_fields.clone()),
                        data_row,
                    );

                    assert_eq!((sub_name, row, &actual), (sub_name, row, expected_row));
                }
            }
        }
    }

    #[test]
    fn test_push_to_subscribers() {
        Lazy::force(&crate::tracing::TEST_TRACING);
        const SPAN_NAME: &'static str = "test_push_to_subscribers";
        let main_span = tracing::info_span!(SPAN_NAME);
        let _main_span_guard = main_span.enter();

        let registry = setup_metrics(SPAN_NAME);

        let all_expected: HashMap<String, Vec<Option<Telemetry>>> = maplit::hashmap! {
            SUBSCRIPTIONS[0].name().to_string() => vec![
                None, //Some(HashMap::default()),
                None, //Some(HashMap::default()),
                None, //Some(HashMap::default()),
                None, //Some(HashMap::default()),
            ],
            SUBSCRIPTIONS[1].name().to_string() => vec![
                Some(maplit::hashmap! {"pos".to_string() => 1.into(), "cat".to_string() => "Stella".into(), "extra".to_string() => "Ripley".into(),}.into_iter().collect()),
                Some(maplit::hashmap! {"pos".to_string() => 3.into(), "cat".to_string() => "Neo".into(), "extra".to_string() => "Ripley".into(),}.into_iter().collect()),
                Some(maplit::hashmap! {"pos".to_string() => 4.into(), "cat".to_string() => "Apollo".into(), "extra".to_string() => "Ripley".into(),}.into_iter().collect()),
                None,
            ],
            SUBSCRIPTIONS[2].name().to_string() => vec![
                Some(maplit::hashmap! {"pos".to_string() => 4.into(), "value".to_string() => 2.71828.into(), "cat".to_string() => "Apollo".into(),}.into_iter().collect()),
                None,
                None,
                None,
            ],
        };

        let mut id_generator = ID_GENERATOR.clone();

        block_on(async move {
            let nr_skip = 0; // todo expand test to remove this line
            let nr_take = SUBSCRIPTIONS.len(); // todo expand test to remove this line
            let subscriptions = SUBSCRIPTIONS
                .iter()
                .skip(nr_skip)//todo expand test to remove this line
                .take(nr_take)//todo expand test to remove this line
                .collect::<Vec<_>>();
            let mut sub_receivers = Vec::with_capacity(subscriptions.len());
            for s in SUBSCRIPTIONS.iter().skip(nr_skip).take(nr_take) {
                // todo expand test to remove this line
                let receiver: Inlet<Telemetry> = Inlet::new(s.name(), PORT_DATA);
                (&s.outlet_to_subscription(), &receiver).connect().await;
                sub_receivers.push((s, receiver));
            }

            for row in 0..DB_ROWS.len() {
                let db = &DB_ROWS[row];
                tracing::info!(?db, "pushing database to subscribers");
                Clearinghouse::push_to_subscribers(
                    &"test_clearinghouse".into(),
                    db,
                    subscriptions.clone(),
                    &mut id_generator,
                )
                .await
                .expect("failed to publish");
            }

            for s in subscriptions.iter() {
                let mut outlet = s.outlet_to_subscription();
                outlet.close().await;
            }

            for row in 0..DB_ROWS.len() {
                for (sub, receiver) in sub_receivers.iter_mut() {
                    let sub_name = sub.name();
                    tracing::info!(%sub_name, "test iteration");
                    let expected = &all_expected[sub_name.as_ref()][row];
                    let actual: Option<Telemetry> = receiver.recv().await;
                    tracing::info!(%sub_name, ?actual, ?expected, "asserting scenario");
                    let actuals = actual.and_then(|mut a| {
                        let ts = assert_ok!(a
                            .remove(SUBSCRIPTION_TIMESTAMP)
                            .map(|ts| Timestamp::try_from(ts))
                            .transpose());
                        let corr = assert_ok!(a
                            .remove(SUBSCRIPTION_CORRELATION)
                            .map(|c| {
                                tracing::warn!("DMR: trying to parse Id from c={:?}", c);
                                Id::try_from(c)
                            })
                            .transpose());
                        let auto = ts.zip(corr);
                        Some(a).zip(auto)
                    });
                    assert_eq!(actuals.is_some(), expected.is_some());
                    actuals.map(|(a, _): (Telemetry, (Timestamp, Id<Telemetry>))| {
                        let a = Some(a);
                        assert_eq!((row, &sub_name, &a), (row, &sub_name, expected));
                    });
                }
            }
        });

        let metric_family = registry.gather();
        assert_eq!(metric_family.len(), 2);

        let cat_metric_name = format!("{}_{}", SPAN_NAME, "cat_counts");
        let cat_metric = assert_some!(metric_family.iter().find(|m| m.get_name() == &cat_metric_name));
        assert_eq!(
            format!("{:?}", cat_metric),
            r##"name: "test_push_to_subscribers_cat_counts" help: "How many cats were found." type: COUNTER metric {label {name: "subscription" value: "cat_pos"} counter {value: 3}}"##
        );

        let pos_metric_name = format!("{}_{}", SPAN_NAME, "current_pos");
        let pos_metric = assert_some!(metric_family.iter().find(|m| m.get_name() == &pos_metric_name));
        assert_eq!(
            format!("{:?}", pos_metric),
            r##"name: "test_push_to_subscribers_current_pos" help: "current position" type: GAUGE metric {label {name: "subscription" value: "cat_pos"} gauge {value: 4}}"##
        );
    }
}
