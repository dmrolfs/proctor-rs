pub mod agent;
mod cache;
pub mod protocol;
pub mod subscription;
#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::fmt::{self, Debug};

pub use agent::*;
use async_trait::async_trait;
use cache::TelemetryCache;
pub use cache::{TelemetryCacheSettings, CacheTtl};
use cast_trait_object::dyn_upcast;
use futures::future::FutureExt;
use once_cell::sync::Lazy;
use pretty_snowflake::Id;
use prometheus::{IntCounterVec, IntGauge, Opts};
pub use protocol::*;
use stretto::CacheError;
pub use subscription::*;
use tokio::sync::mpsc;

use crate::elements::{Telemetry, Timestamp};
use crate::error::{ProctorError, SenseError};
use crate::graph::stage::Stage;
use crate::graph::{stage, Inlet, OutletsShape, Port, SinkShape, UniformFanOutShape};
use crate::{ProctorIdGenerator, ProctorResult};

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

pub const SUBSCRIPTION_TIMESTAMP: &str = "recv_timestamp";
pub const SUBSCRIPTION_CORRELATION: &str = "correlation_id";

pub type CorrelationGenerator = ProctorIdGenerator<Telemetry>;

/// Clearinghouse is a sink for collected telemetry data and a subscription-based source for
/// groups of telemetry fields.
pub struct Clearinghouse {
    name: String,
    subscriptions: Vec<TelemetrySubscription>,
    cache: TelemetryCache,
    cache_settings: TelemetryCacheSettings,
    correlation_generator: tokio::sync::Mutex<CorrelationGenerator>,
    inlet: Inlet<Telemetry>,
    tx_api: ClearinghouseApi,
    rx_api: mpsc::UnboundedReceiver<ClearinghouseCmd>,
}

const PORT_TELEMETRY: &str = "telemetry";

impl Clearinghouse {
    pub fn new(
        name: impl Into<String>, cache_settings: &TelemetryCacheSettings, correlation_generator: CorrelationGenerator,
    ) -> Self {
        let name = name.into();
        let (tx_api, rx_api) = mpsc::unbounded_channel();
        let inlet = Inlet::new(name.clone(), PORT_TELEMETRY);
        let cache = TelemetryCache::new(cache_settings);

        Self {
            name,
            subscriptions: Vec::default(),
            cache,
            cache_settings: cache_settings.clone(),
            correlation_generator: tokio::sync::Mutex::new(correlation_generator),
            inlet,
            tx_api,
            rx_api,
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn subscribe(&mut self, subscription: TelemetrySubscription, receiver: &Inlet<Telemetry>) {
        tracing::info!(stage=%self.name, ?subscription, "adding clearinghouse subscription.");
        subscription.connect_to_receiver(receiver).await;
        self.subscriptions.push(subscription);
        track_subscriptions(self.subscriptions.len());
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn handle_telemetry_data(&mut self, data: Option<Telemetry>) -> Result<bool, SenseError> {
        match data {
            Some(d) => {
                let mut pushed_fields = HashSet::with_capacity(d.len());
                for (k, v) in d {
                    let cache_insert = self
                        .cache
                        .try_insert(k.clone(), v, self.cache_settings.incremental_item_cost)
                        .await
                        .map(|added| {
                            if added {
                                pushed_fields.insert(k.clone());
                            } else {
                                tracing::warn!(stage=%self.name, field=%k, "failed to add to clearinghouse cache.");
                            }
                        });

                    if let Err(err) = cache_insert {
                        tracing::warn!(stage=%self.name, field=%k, error=?err, "cache error during clearinghouse cache insert.");
                    }
                }
                if let Err(err) = self.cache.wait().await {
                    tracing::error!(stage=%self.name, error=?err, "failed to wait on clearinghouse cache. - ignoring");
                }

                let interested = Self::find_interested_subscriptions(&self.subscriptions, &pushed_fields);

                self.push_to_subscribers(interested).await?;
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

    #[tracing::instrument(level = "trace", skip(subscriptions, pushed,))]
    fn find_interested_subscriptions<'s, 'f>(
        subscriptions: &'s [TelemetrySubscription], pushed: &'f HashSet<String>,
    ) -> Vec<&'s TelemetrySubscription> {
        let interested = subscriptions
            .iter()
            .filter(|s| {
                let is_interested = s.any_interest(pushed.iter());
                tracing::debug!(
                    "is subscription, {}, interested in pushed fields:{:?} => {}",
                    s.name(),
                    pushed,
                    is_interested
                );
                is_interested
            })
            .collect::<Vec<_>>();

        tracing::debug!(
            nr_subscriptions=%subscriptions.len(),
            nr_interested=%interested.len(),
            ?interested,
            "interested subscriptions {}.",
            if interested.is_empty() { "not found"} else { "found" }
        );

        interested
    }

    #[tracing::instrument(
        level = "trace",
        skip(self, subscribers),
        fields(subscribers = ?subscribers.iter().map(|s| s.name()).collect::<Vec<_>>(), ),
    )]
    async fn push_to_subscribers(&self, subscribers: Vec<&TelemetrySubscription>) -> Result<(), SenseError> {
        if subscribers.is_empty() {
            tracing::debug!("not publishing - no subscribers corresponding to field changes.");
            return Ok(());
        }

        let nr_subscribers = subscribers.len();
        let mut correlation_generator = self.correlation_generator.lock().await;

        let fulfilled = subscribers
            .into_iter()
            .filter_map(|s| Self::fulfill_subscription(s, &self.cache).map(|fulfillment| (s, fulfillment)))
            .collect::<Vec<_>>()
            .into_iter()
            .map(|(s, mut fulfillment)| {
                let (_corr, _recv_ts) = Self::add_automatic_telemetry(&mut fulfillment, &mut correlation_generator);
                tracing::debug!(subscription=%s.name(), ?fulfillment, "sending subscription data update.");
                s.update_metrics(&fulfillment);
                s.send(fulfillment).map(move |send_status| {
                    track_publications(s.name());
                    (s, send_status)
                })
            })
            .collect::<Vec<_>>();

        let nr_fulfilled = fulfilled.len();

        let statuses = futures::future::join_all(fulfilled).await;
        let result = if let Some((s, Err(err))) = statuses.into_iter().find(|(_, status)| status.is_err()) {
            tracing::error!(error=?err, subscription=%s.name(), "failed to send fulfilled subscription.");
            // todo: change to track *all* errors -- only first found is currently tracked
            // todo: resolve design to avoid this hack to satisfy track_errors api while not exposing
            // sense stage with proctor error.
            let proctor_err = ProctorError::SensePhase(err);
            crate::graph::track_errors(self.name(), &proctor_err);
            let err = match proctor_err {
                ProctorError::SensePhase(err) => Some(err),
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

        tracing::debug!(
            nr_fulfilled=%nr_fulfilled,
            nr_not_fulfilled=%(nr_subscribers - nr_fulfilled),
            sent_ok=%result.is_ok(),
            "published to subscriptions"
        );

        result
    }

    fn add_automatic_telemetry(
        telemetry: &mut Telemetry, correlation_generator: &mut CorrelationGenerator,
    ) -> (Id<Telemetry>, Timestamp) {
        let now = Timestamp::now();
        telemetry.insert(SUBSCRIPTION_TIMESTAMP.to_string(), now.into());
        let correlation_id: Id<Telemetry> = correlation_generator.next_id();
        telemetry.insert(SUBSCRIPTION_CORRELATION.to_string(), correlation_id.clone().into());
        (correlation_id, now)
    }

    #[tracing::instrument(level = "trace")]
    fn fulfill_subscription(subscription: &TelemetrySubscription, cache: &TelemetryCache) -> Option<Telemetry> {
        subscription.fulfill(cache)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn handle_command(
        &mut self,
        command: ClearinghouseCmd, // subscriptions: &mut Vec<TelemetrySubscription>, database: &Telemetry,
    ) -> Result<bool, SenseError> {
        match command {
            ClearinghouseCmd::GetSnapshot { name, tx } => {
                let telemetry = self.cache.get_telemetry();

                let snapshot = match name {
                    None => {
                        tracing::trace!("no subscription specified - responding with clearinghouse snapshot.");
                        ClearinghouseSnapshot {
                            telemetry,
                            missing: HashSet::default(),
                            subscriptions: self.subscriptions.clone(),
                        }
                    },

                    Some(name) => match self.subscriptions.iter().find(|s| s.name() == name.as_str()) {
                        Some(sub) => {
                            let (db, missing) = sub.trim_to_subscription(&telemetry)?;

                            tracing::debug!(
                                requested_subscription=%name, data=?db, missing=?missing,
                                "subscription found - focusing snapshot."
                            );

                            ClearinghouseSnapshot {
                                telemetry: db,
                                missing,
                                subscriptions: vec![sub.clone()],
                            }
                        },

                        None => {
                            tracing::debug!(requested_subscription=%name, "subscription not found - returning clearinghouse snapshot.");
                            ClearinghouseSnapshot {
                                telemetry,
                                missing: HashSet::default(),
                                subscriptions: self.subscriptions.clone(),
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
                self.subscriptions.push(*subscription);
                track_subscriptions(self.subscriptions.len());
                let _ = tx.send(());
                Ok(true)
            },

            ClearinghouseCmd::Unsubscribe { name, tx } => {
                let dropped = self
                    .subscriptions
                    .iter()
                    .position(|s| s.name() == name.as_str())
                    .map(|pos| {
                        let _dropped = self.subscriptions.remove(pos);
                        track_subscriptions(self.subscriptions.len());
                    });

                tracing::info!(?dropped, "subscription dropped");
                let _ = tx.send(());
                Ok(true)
            },

            ClearinghouseCmd::Clear { tx } => {
                if let Err(err) = self.clear_cache().await {
                    tracing::error!(error=?err, "failed to clear clearinghouse telemetry cache - ignoring.");
                }

                let _ = tx.send(());
                Ok(true)
            },
        }
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn clear_cache(&mut self) -> Result<(), CacheError> {
        // clear doesn't work now, but hopefully soon!
        let clear_ack = match self.cache.clear() {
            Ok(()) => self.cache.wait().await,
            err => err,
        };
        match clear_ack {
            Ok(()) => tracing::info!(stage=%self.name(), "clearinghouse telemetry cache cleared."),
            Err(err) => {
                tracing::error!(error=?err, "failed to clear clearinghouse telemetry cache -- ignoring.")
            },
        }

        self.cache = TelemetryCache::new(&self.cache_settings);
        Ok(())
    }
}

impl Debug for Clearinghouse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clearinghouse")
            .field("name", &self.name)
            .field("subscriptions", &self.subscriptions)
            .field("telemetry_ttl", &self.cache_settings.ttl)
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
    fn name(&self) -> &str {
        &self.name
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", name = "run clearinghouse", skip(self))]
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
    async fn do_check(&self) -> Result<(), SenseError> {
        self.inlet.check_attachment().await?;

        for s in self.subscriptions.iter() {
            s.outlet_to_subscription().check_attachment().await?;
        }

        Ok(())
    }

    async fn do_run(&mut self) -> Result<(), SenseError> {
        loop {
            let _timer = stage::start_stage_eval_time(self.name());

            tracing::trace!(
                nr_subscriptions=%self.subscriptions.len(),
                subscriptions=?self.subscriptions,
                "handling next item.."
            );

            tokio::select! {
                data = self.inlet.recv() => {
                    let cont_loop = self.handle_telemetry_data(data).await?;

                    if !cont_loop {
                        break;
                    }
                },

                Some(command) = self.rx_api.recv() => {
                    let continue_loop = self.handle_command(command).await?;

                    if !continue_loop {
                        break;
                    }
                },

                else => break,
            }
        }

        Ok(())
    }

    async fn do_close(mut self: Box<Self>) -> Result<(), SenseError> {
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
mod clearinghouse_tests {
    use std::collections::HashMap;
    use std::convert::TryFrom;
    use std::convert::TryInto;
    use std::sync::Arc;
    use std::time::Duration;

    use claim::*;
    use pretty_assertions::assert_eq;
    use pretty_snowflake::{AlphabetCodec, Id, IdPrettifier, PrettyIdGenerator};
    use prometheus::{IntCounterVec, IntGaugeVec, Opts, Registry};
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::sync::oneshot;
    use tokio_test::block_on;
    use tracing::Instrument;

    use super::*;
    use crate::elements::telemetry::{TableType, ToTelemetry};
    use crate::graph::stage::{self, ActorSourceCmd, Stage, WithApi};
    use crate::graph::{Connect, Outlet, SinkShape, SourceShape, PORT_DATA};
    use crate::phases::sense::clearinghouse::cache::CacheTtl;
    use crate::phases::sense::SubscriptionChannel;

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

        let sub1_inlet = Inlet::new("sub1", PORT_DATA);
        block_on(async {
            let mut clearinghouse =
                Clearinghouse::new("test", &TelemetryCacheSettings::default(), ID_GENERATOR.clone());
            // assert!(clearinghouse.cache.is_empty());
            assert!(clearinghouse.subscriptions.is_empty());
            assert_eq!(clearinghouse.name, "test");
            assert!(!clearinghouse.inlet.is_attached().await);


            clearinghouse
                .subscribe(
                    TelemetrySubscription::new("sub1")
                        .with_required_fields(maplit::hashset! {"aaa", "bbb"})
                        .with_optional_fields(HashSet::<&str>::default()),
                    &sub1_inlet,
                )
                .await;
            assert_eq!(clearinghouse.subscriptions.len(), 1);
        });

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
            let mut clearinghouse = Clearinghouse::new(
                "test-clearinghouse",
                &TelemetryCacheSettings::default(),
                ID_GENERATOR.clone(),
            );
            let tx_api = clearinghouse.tx_api();
            (tick.outlet(), clearinghouse.inlet()).connect().await;

            let tick_handle = tokio::spawn(async move { tick.run().await });
            let clear_handle = tokio::spawn(
                async move { clearinghouse.run().await }.instrument(tracing::info_span!("spawn clearinghouse")),
            );

            let nr_0_span = tracing::info_span!("nr_subscriptions is 0");
            let _ = nr_0_span.enter();

            let ClearinghouseSnapshot {
                telemetry: _db_0,
                missing: _missing_0,
                subscriptions: subs_0,
            } = assert_ok!(ClearinghouseCmd::get_clearinghouse_snapshot(&tx_api).await);
            let nr_subscriptions = subs_0.len();
            tracing::info!(%nr_subscriptions, "assert nr_subscriptions is 0...");
            assert_eq!(nr_subscriptions, 0);

            tracing::info!("sending add subscriber command to clearinghouse...");
            let sub1_inlet = Inlet::new("sub1", PORT_DATA);
            assert_ok!(
                ClearinghouseCmd::subscribe(
                    &tx_api,
                    TelemetrySubscription::new("sub1")
                        .with_required_fields(maplit::hashset! {"aaa", "bbb"})
                        .with_optional_fields(HashSet::<&str>::default()),
                    sub1_inlet.clone(),
                )
                .await
            );

            let nr_1_span = tracing::info_span!("nr_subscriptions is 1");
            let _ = nr_1_span.enter();

            let ClearinghouseSnapshot {
                telemetry: _db_1,
                missing: _missing_1,
                subscriptions: subs_1,
            } = assert_ok!(ClearinghouseCmd::get_clearinghouse_snapshot(&tx_api).await);
            let nr_subscriptions = subs_1.len();
            tracing::info!(%nr_subscriptions, "assert nr_subscriptions is now 1...");
            assert_eq!(nr_subscriptions, 1);

            tracing::info!("stopping tick source...");
            let (tx_tick_stop, rx_tick_stop) = oneshot::channel();
            let stop_tick = stage::tick::TickCmd::Stop { tx: tx_tick_stop };
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
            let data: Telemetry = maplit::hashmap! { "dr".to_string() => 17_i64.to_telemetry() }
                .into_iter()
                .collect();
            let mut tick = stage::Tick::new("tick", Duration::from_nanos(0), Duration::from_millis(5), data);
            let tx_tick_api = tick.tx_api();
            let mut clearinghouse = Clearinghouse::new(
                "test-clearinghouse",
                &TelemetryCacheSettings::default(),
                ID_GENERATOR.clone(),
            );
            let tx_api = clearinghouse.tx_api();
            (tick.outlet(), clearinghouse.inlet()).connect().await;

            let tick_handle = tokio::spawn(async move { tick.run().await });
            let clear_handle = tokio::spawn(async move { clearinghouse.run().await });

            let inlet_1 = Inlet::new("inlet_1", PORT_DATA);
            assert_ok!(
                ClearinghouseCmd::subscribe(
                    &tx_api,
                    TelemetrySubscription::new("sub1")
                        .with_required_fields(maplit::hashset! { "dr" })
                        .with_optional_fields(HashSet::<String>::default()),
                    inlet_1.clone(),
                )
                .await
            );

            let ClearinghouseSnapshot { telemetry: _, missing: _, subscriptions: subs1 } =
                assert_ok!(ClearinghouseCmd::get_clearinghouse_snapshot(&tx_api).await);
            assert_eq!(subs1.len(), 1);

            let name_1 = subs1[0].name();
            assert_ok!(ClearinghouseCmd::unsubscribe(&tx_api, name_1.as_ref()).await);

            let ClearinghouseSnapshot { telemetry: _, missing: _, subscriptions: subs2 } =
                assert_ok!(ClearinghouseCmd::get_clearinghouse_snapshot(&tx_api).await);
            assert_eq!(subs2.len(), 0);

            tracing::info!("stopping tick source...");
            assert_ok!(stage::tick::TickCmd::stop(&tx_tick_api).await);
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

        // let available: HashMap<String, TelemetryValue> = HashMap::default();
        // let available_keys: HashSet<String> = available.keys().cloned().collect();
        let actual = Clearinghouse::find_interested_subscriptions(&SUBSCRIPTIONS, &HashSet::default()); //&available_keys);
        assert_eq!(actual, Vec::<&TelemetrySubscription>::default());

        // let available = maplit::hashmap! {"extra".to_string() => TelemetryValue::Unit };
        let actual = Clearinghouse::find_interested_subscriptions(
            &SUBSCRIPTIONS,
            // &available_keys,
            &maplit::hashset! {"extra".to_string()},
        );
        assert_eq!(actual, vec![&SUBSCRIPTIONS[1]]);

        // let available = maplit::hashmap! {"nonsense".to_string() => TelemetryValue::Unit };
        let actual = Clearinghouse::find_interested_subscriptions(
            &SUBSCRIPTIONS,
            // &available.keys(),
            &maplit::hashset! {"nonsense".to_string()},
        );
        assert_eq!(actual, Vec::<&TelemetrySubscription>::default());

        // let available = maplit::hashmap! {"pos".to_string() => TelemetryValue::Unit };
        let actual = Clearinghouse::find_interested_subscriptions(
            &SUBSCRIPTIONS,
            // &available.keys(),
            &maplit::hashset! {"pos".to_string()},
        );
        assert_eq!(actual, vec![&SUBSCRIPTIONS[1], &SUBSCRIPTIONS[2]]);

        // let available = maplit::hashmap! {"value".to_string() => TelemetryValue::Unit };
        let actual = Clearinghouse::find_interested_subscriptions(
            &SUBSCRIPTIONS,
            // &available.keys(),
            &maplit::hashset! {"value".to_string()},
        );
        assert_eq!(actual, vec![&SUBSCRIPTIONS[2]]);

        // let base = maplit::hashset! {"pos".to_string(), "cat".to_string()};
        // let available: HashMap<String, TelemetryValue> =
        //     base.iter().map(|k| (k.clone(), TelemetryValue::Unit)).collect();
        let actual = Clearinghouse::find_interested_subscriptions(
            &SUBSCRIPTIONS,
            // &available.keys(),
            &maplit::hashset! {"pos".to_string(), "cat".to_string()},
        );
        assert_eq!(actual, vec![&SUBSCRIPTIONS[1], &SUBSCRIPTIONS[2]]);

        // let base = maplit::hashset! {"pos".to_string(), "value".to_string(), "cat".to_string()};
        // let available: HashMap<String, TelemetryValue> =
        //     base.iter().map(|k| (k.clone(), TelemetryValue::Unit)).collect();
        let actual = Clearinghouse::find_interested_subscriptions(
            &SUBSCRIPTIONS,
            // &available.keys(),
            &maplit::hashset! {"pos".to_string(), "value".to_string(), "cat".to_string()},
        );
        assert_eq!(actual, vec![&SUBSCRIPTIONS[1], &SUBSCRIPTIONS[2]]);

        // let base = maplit::hashset! {"pos".to_string(), "value".to_string(), "cat".to_string(),
        // "extra".to_string()}; let available: HashMap<String, TelemetryValue> =
        //     base.iter().map(|k| (k.clone(), TelemetryValue::Unit)).collect();
        let actual = Clearinghouse::find_interested_subscriptions(
            &SUBSCRIPTIONS,
            // &available.keys(),
            &maplit::hashset! {"pos".to_string(), "value".to_string(), "cat".to_string(), "extra".to_string()},
        );
        assert_eq!(actual, vec![&SUBSCRIPTIONS[1], &SUBSCRIPTIONS[2]]);
    }

    #[test]
    fn test_fulfill_subscription() {
        Lazy::force(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_fulfill_subscription");
        let _main_span_guard = main_span.enter();

        let settings = TelemetryCacheSettings {
            ttl: CacheTtl {
                default_ttl: Duration::from_secs(60),
                ..CacheTtl::default()
            },
            nr_counters: 1_000,
            max_cost: 1e6 as i64,
            incremental_item_cost: 1,
            cleanup_interval: Duration::from_secs(5),
        };

        for subscriber in 0..=2 {
            if let TelemetrySubscription::Explicit {
                name: sub_name,
                required_fields: sub_required_fields,
                optional_fields: sub_optional_fields,
                ..
            } = &SUBSCRIPTIONS[subscriber]
            {
                let expected = &EXPECTED[sub_name];
                tracing::info!(
                    nr=%subscriber,
                    subscriber_name=%sub_name,
                    subscriber_required_fields=?sub_required_fields,
                    subscriber_optional_fields=?sub_optional_fields,
                    ?expected,
                    "next test scenario..."
                );
                for ((row, data_row), expected_row) in DB_ROWS.iter().enumerate().zip(expected) {
                    block_on(async {
                        let mut cache = TelemetryCache::new(&settings);

                        for (k, v) in data_row.iter() {
                            assert!(
                                cache
                                    .insert_with_ttl(k.clone(), v.clone(), 1, Duration::from_secs(60))
                                    .await
                            );
                        }

                        assert_ok!(cache.wait().await);
                        assert_eq!(data_row.keys().cloned().collect::<HashSet<_>>(), cache.seen());
                        assert_eq!(data_row, &cache.get_telemetry());
                        let actual = Clearinghouse::fulfill_subscription(
                            &TelemetrySubscription::new(sub_name)
                                .with_required_fields(sub_required_fields.clone())
                                .with_optional_fields(sub_optional_fields.clone()),
                            &cache,
                        );

                        assert_eq!((sub_name, row, &actual), (sub_name, row, expected_row));
                    })
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

        let id_generator = ID_GENERATOR.clone();

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
                let mut c = Clearinghouse::new(
                    format!("test-{row}"),
                    &TelemetryCacheSettings::default(),
                    id_generator.clone(),
                );
                for (k, v) in DB_ROWS[row].iter() {
                    assert!(
                        c.cache
                            .insert(k.clone(), v.clone(), c.cache_settings.incremental_item_cost)
                            .await
                    );
                }
                assert_ok!(c.cache.wait().await);

                let db = c.cache.get_telemetry();
                assert_eq!(db.keys().cloned().collect::<HashSet<_>>(), c.cache.seen());
                assert_eq!(&DB_ROWS[row], &db);
                tracing::info!(?db, "pushing database to subscribers");
                assert_ok!(c.push_to_subscribers(subscriptions.clone()).await);
            }

            for s in subscriptions.iter() {
                let mut outlet = s.outlet_to_subscription();
                outlet.close().await;
            }

            for row in 0..DB_ROWS.len() {
                for (sub, receiver) in sub_receivers.iter_mut() {
                    let sub_name = sub.name();
                    tracing::info!(%sub_name, "test iteration");
                    let expected = &all_expected[sub_name][row];
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
                                tracing::info!("trying to parse Id from c={:?}", c);
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

    fn without_auto_fields(telemetry: &Telemetry) -> Telemetry {
        let mut telemetry = telemetry.clone();
        telemetry.remove(SUBSCRIPTION_TIMESTAMP);
        telemetry.remove(SUBSCRIPTION_CORRELATION);
        telemetry
    }

    #[test]
    fn test_clearinghouse_use_after_clear() {
        Lazy::force(&crate::tracing::TEST_TRACING);
        const SPAN_NAME: &'static str = "test_clearinghouse_use_after_clear";
        let main_span = tracing::info_span!(SPAN_NAME);
        let _main_span_guard = main_span.enter();

        let cache_settings = TelemetryCacheSettings {
            ttl: CacheTtl {
                default_ttl: Duration::from_secs(1),
                ..CacheTtl::default()
            },
            nr_counters: 1000,
            max_cost: 100,
            incremental_item_cost: 1,
            cleanup_interval: Duration::from_millis(500),
        };

        block_on(async {
            let source = stage::ActorSource::new("source");
            let tx_source = source.tx_api();

            let id_gen = PrettyIdGenerator::single_node(IdPrettifier::<AlphabetCodec>::default());
            let mut clearinghouse = Clearinghouse::new("clearinghouse", &cache_settings, id_gen);
            let tx_clearinghouse = clearinghouse.tx_api();

            let subscription = TelemetrySubscription::Explicit {
                name: "sub".into(),
                required_fields: maplit::hashset! {"pos".into(), "cat".into()},
                optional_fields: maplit::hashset! {"extra".into()},
                outlet_to_subscription: Outlet::new("sub_outlet", PORT_DATA),
                update_metrics: None,
            };

            let channel: SubscriptionChannel<TableType> = assert_ok!(SubscriptionChannel::new("channel").await);
            let (tx_sink, mut rx_sink) = mpsc::channel(8);

            (source.outlet(), clearinghouse.inlet()).connect().await;
            clearinghouse.subscribe(subscription, &channel.subscription_receiver).await;
            channel.outlet().attach("sink", tx_sink).await;

            let mut graph = crate::graph::Graph::default();
            graph.push_back(Box::new(source)).await;
            graph.push_back(Box::new(clearinghouse)).await;
            graph.push_back(Box::new(channel)).await;
            let graph_handle = tokio::spawn(async move { graph.run().await });

            assert_ok!(
                ActorSourceCmd::push(
                    &tx_source,
                    Telemetry::from(maplit::hashmap! {
                        "pos".into() => 13_i64.into(),
                        "cat".into() => "stella".into(),
                    })
                )
                .await
            );

            let actual = assert_ok!(rx_sink.try_recv());
            assert_eq!(
                without_auto_fields(&Telemetry::from(actual)),
                without_auto_fields(&Telemetry::from(maplit::hashmap! {
                    "pos".into() => 13_i64.into(),
                    "cat".into() => "stella".into(),
                }))
            );

            assert_ok!(ClearinghouseCmd::clear(&tx_clearinghouse).await);

            assert_matches!(rx_sink.try_recv(), Err(TryRecvError::Empty));

            assert_ok!(
                ActorSourceCmd::push(
                    &tx_source,
                    Telemetry::from(maplit::hashmap! {
                        "pos".into() => 13_i64.into(),
                        "extra".into() => 3.14_f64.into(),
                    })
                )
                .await
            );

            assert_matches!(rx_sink.try_recv(), Err(TryRecvError::Empty));

            assert_ok!(
                ActorSourceCmd::push(
                    &tx_source,
                    Telemetry::from(maplit::hashmap! {
                        "cat".into() => "neo".into(),
                    })
                )
                .await
            );

            let actual = assert_some!(rx_sink.recv().await);
            assert_eq!(
                without_auto_fields(&Telemetry::from(actual)),
                without_auto_fields(&Telemetry::from(maplit::hashmap! {
                    "pos".into() => 13_i64.into(),
                    "cat".into() => "neo".into(),
                    "extra".into() => 3.14_f64.into(),
                }))
            );

            assert_ok!(ActorSourceCmd::stop(&tx_source).await);

            assert_ok!(assert_ok!(graph_handle.await));
            assert_none!(rx_sink.recv().await);
        })
    }
}
