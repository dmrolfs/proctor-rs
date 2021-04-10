use crate::elements::TelemetryData;
use crate::graph::stage::Stage;
use crate::graph::{stage, Connect, GraphResult, Inlet, Outlet, OutletsShape, Port, SinkShape, UniformFanOutShape};
use crate::Ack;
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use futures::future::FutureExt;
use std::collections::HashSet;
use std::fmt::{self, Debug};
use std::iter::FromIterator;
use tokio::sync::{mpsc, oneshot};

pub type ClearinghouseApi = mpsc::UnboundedSender<ClearinghouseCmd>;

#[derive(Debug)]
pub enum ClearinghouseCmd {
    Subscribe {
        subscription: TelemetrySubscription,
        receiver: Inlet<TelemetryData>,
        tx: oneshot::Sender<Ack>,
    },
    Unsubscribe {
        name: String,
        tx: oneshot::Sender<Ack>,
    },
    GetSnapshot {
        name: Option<String>,
        tx: oneshot::Sender<ClearinghouseResp>,
    },
}

impl ClearinghouseCmd {
    #[inline]
    pub fn subscribe(
        subscription: TelemetrySubscription, receiver: Inlet<TelemetryData>,
    ) -> (Self, oneshot::Receiver<Ack>) {
        let (tx, rx) = oneshot::channel();
        (
            Self::Subscribe {
                subscription,
                receiver,
                tx,
            },
            rx,
        )
    }

    #[inline]
    pub fn unsubscribe<S: Into<String>>(name: S) -> (Self, oneshot::Receiver<Ack>) {
        let (tx, rx) = oneshot::channel();
        (Self::Unsubscribe { name: name.into(), tx }, rx)
    }

    #[inline]
    pub fn get_clearinghouse_snapshot() -> (Self, oneshot::Receiver<ClearinghouseResp>) {
        let (tx, rx) = oneshot::channel();
        (Self::GetSnapshot { name: None, tx }, rx)
    }

    #[inline]
    pub fn get_subscription_snapshot<S: Into<String>>(name: S) -> (Self, oneshot::Receiver<ClearinghouseResp>) {
        let (tx, rx) = oneshot::channel();
        (
            Self::GetSnapshot {
                name: Some(name.into()),
                tx,
            },
            rx,
        )
    }
}

#[derive(Debug)]
pub enum ClearinghouseResp {
    Snapshot {
        database: TelemetryData,
        missing: HashSet<String>,
        subscriptions: Vec<TelemetrySubscription>,
    },
}

#[derive(Debug, Clone)]
pub struct TelemetrySubscription {
    pub name: String,
    pub required_fields: HashSet<String>,
    pub optional_fields: HashSet<String>,
    pub outlet_to_subscription: Outlet<TelemetryData>,
}

impl TelemetrySubscription {
    pub fn new<S: Into<String>>(name: S) -> Self {
        let name = name.into();
        let outlet_to_subscription = Outlet::new(format!("outlet_for_subscription_{}", name));
        Self {
            name,
            required_fields: HashSet::default(),
            optional_fields: HashSet::default(),
            outlet_to_subscription,
        }
    }

    pub fn with_required_fields<S: Into<String>>(self, required_fields: HashSet<S>) -> Self {
        let required_fields = required_fields.into_iter().map(|s| s.into()).collect();
        Self {
            required_fields,
            ..self
        }
    }

    pub fn with_optional_fields<S: Into<String>>(self, optional_fields: HashSet<S>) -> Self {
        let optional_fields = optional_fields.into_iter().map(|s| s.into()).collect();
        Self {
            optional_fields,
            ..self
        }
    }
}

impl TelemetrySubscription {
    pub async fn close(mut self) {
        self.outlet_to_subscription.close().await;
    }
}

impl PartialEq for TelemetrySubscription {
    fn eq(&self, other: &Self) -> bool {
        (self.name == other.name)
            && (self.required_fields == other.required_fields)
            && (self.optional_fields == other.optional_fields)
    }
}

/// Clearinghouse is a sink for collected telemetry data and a subscription-based source for
/// groups of telemetry fields.
///
///
pub struct Clearinghouse {
    name: String,
    subscriptions: Vec<TelemetrySubscription>,
    database: TelemetryData,
    inlet: Inlet<TelemetryData>,
    tx_api: ClearinghouseApi,
    rx_api: mpsc::UnboundedReceiver<ClearinghouseCmd>,
}

impl Clearinghouse {
    pub fn new<S: Into<String>>(name: S) -> Self {
        let (tx_api, rx_api) = mpsc::unbounded_channel();
        let name = name.into();
        let inlet = Inlet::new(name.clone());

        Self {
            name,
            subscriptions: Vec::default(),
            database: TelemetryData::default(),
            inlet,
            tx_api,
            rx_api,
        }
    }

    pub async fn add_subscription(&mut self, subscription: TelemetrySubscription, receiver: &Inlet<TelemetryData>) {
        tracing::info!(stage=%self.name, ?subscription, "adding clearinghouse subscription.");
        (&subscription.outlet_to_subscription, receiver).connect().await;
        self.subscriptions.push(subscription);
    }

    #[tracing::instrument(level = "trace", skip(subscriptions, database,))]
    async fn handle_telemetry_data(
        data: Option<TelemetryData>, subscriptions: &Vec<TelemetrySubscription>, database: &mut TelemetryData,
    ) -> GraphResult<bool> {
        match data {
            Some(d) => {
                let updated_fields = d.keys().cloned().collect::<HashSet<_>>();
                let interested = Self::find_interested_subscriptions(subscriptions, updated_fields);

                database.extend(d);
                Self::push_to_subscribers(database, interested).await?;

                Ok(true)
            }

            None => {
                tracing::info!(
                    "telemetry sources dried up - stopping since subscribers have data they're going to get."
                );
                Ok(false)
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(subscriptions,))]
    fn find_interested_subscriptions(
        subscriptions: &Vec<TelemetrySubscription>, changed: HashSet<String>,
    ) -> Vec<&TelemetrySubscription> {
        let interested = subscriptions
            .iter()
            //todo: add consideration for optional fields
            .filter(|s| !s.required_fields.intersection(&changed).collect::<HashSet<_>>().is_empty())
            .collect::<Vec<_>>();

        tracing::info!(
            changed_fields=?changed,
            nr_subscriptions=%subscriptions.len(),
            nr_interested=%interested.len(),
            "interested subscriptions"
        );

        interested
    }

    #[tracing::instrument(level = "trace", skip(database, subscribers))]
    async fn push_to_subscribers(
        database: &TelemetryData, subscribers: Vec<&TelemetrySubscription>,
    ) -> GraphResult<()> {
        if subscribers.is_empty() {
            tracing::info!("not publishing - no clearinghouse subscribers");
            return Ok(());
        }

        let nr_subscribers = subscribers.len();
        let fulfilled = subscribers
            .into_iter()
            //todo: extend with optional fields
            .map(|s| Self::fulfill_subscription(&s.name, &s.required_fields, database).map(|fulfillment| (s, fulfillment)))
            .flatten()
            .map(|(s, fulfillment)| {
                let o = &s.outlet_to_subscription;
                tracing::info!(subscription=%s.name, "sending subscription data update.");
                o.send(fulfillment).map(move |send_status| (s, send_status))
            })
            .collect::<Vec<_>>();

        let nr_fulfilled = fulfilled.len();

        let statuses = futures::future::join_all(fulfilled).await;
        let result: GraphResult<()> = if let Some((s, err)) = statuses.into_iter().find(|(_, status)| status.is_err()) {
            tracing::error!(subscriber=%s.name, "failed to send fulfilled subscription.");
            err
        } else {
            Ok(())
        };

        tracing::info!(
            nr_fulfilled=%nr_fulfilled,
            nr_not_fulfilled=%(nr_subscribers - nr_fulfilled),
            sent_ok=%result.is_ok(),
            "publishing impacted subscriptions"
        );

        result
    }

    //todo fufill optional fields too; may simplfy interface to subscription?
    fn fulfill_subscription(
        subscription_name: &str, pick_list: &HashSet<String>, database: &TelemetryData,
    ) -> Option<TelemetryData> {
        let mut ready = Vec::new();
        let mut unfilled = Vec::new();

        for key in pick_list {
            match database.get(key) {
                Some(value) => ready.push((key.clone(), value.clone())),
                None => unfilled.push(key),
            }
        }

        if ready.is_empty() || !unfilled.is_empty() {
            tracing::info!(
                subscription=%subscription_name,
                unfilled_fields=?unfilled,
                "waiting for full subscription - not publishing."
            );

            None
        } else {
            Some(TelemetryData::from_iter(ready))
        }
    }

    #[tracing::instrument(level = "trace", skip(subscriptions, database))]
    async fn handle_command(
        command: ClearinghouseCmd, subscriptions: &mut Vec<TelemetrySubscription>, database: &TelemetryData,
    ) -> GraphResult<bool> {
        match command {
            ClearinghouseCmd::GetSnapshot { name, tx } => {
                let snapshot = match name {
                    None => {
                        tracing::info!("no subscription specified - responding with clearinghouse snapshot.");
                        ClearinghouseResp::Snapshot {
                            database: database.clone(),
                            missing: HashSet::default(),
                            subscriptions: subscriptions.clone(),
                        }
                    }

                    Some(name) => match subscriptions.iter().find(|s| s.name == name) {
                        Some(sub) => {
                            let mut db = database.clone();
                            db.retain(|k, _| sub.required_fields.contains(k.as_str()));

                            let mut missing: HashSet<String> = HashSet::default();
                            for field in db.keys() {
                                if sub.required_fields.contains(field) {
                                    missing.insert(field.clone());
                                }
                            }

                            tracing::info!(
                                requested_subscription=%name,
                                data=?db,
                                missing=?missing,
                                "subscription found - focusing snapshot."
                            );

                            ClearinghouseResp::Snapshot {
                                database: db,
                                missing,
                                subscriptions: vec![sub.clone()],
                            }
                        }

                        None => {
                            tracing::info!(requested_subscription=%name, "subscription not found - returning clearinghouse snapshot.");
                            ClearinghouseResp::Snapshot {
                                database: database.clone(),
                                missing: HashSet::default(),
                                subscriptions: subscriptions.clone(),
                            }
                        }
                    },
                };

                let _ = tx.send(snapshot);
                Ok(true)
            }

            ClearinghouseCmd::Subscribe {
                subscription,
                receiver,
                tx,
            } => {
                tracing::info!(?subscription, "adding telemetry subscriber.");
                (&subscription.outlet_to_subscription, &receiver).connect().await;
                subscriptions.push(subscription);
                let _ = tx.send(());
                Ok(true)
            }

            ClearinghouseCmd::Unsubscribe { name, tx } => {
                // let mut subs = subscriptions.lock().await;
                let dropped = subscriptions
                    .iter()
                    .position(|s| s.name == name)
                    .map(|pos| subscriptions.remove(pos));

                tracing::info!(?dropped, "subscription dropped");
                let _ = tx.send(());
                Ok(true)
            }
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
    type In = TelemetryData;
    #[inline]
    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

impl UniformFanOutShape for Clearinghouse {
    type Out = TelemetryData;

    #[inline]
    fn outlets(&self) -> OutletsShape<Self::Out> {
        self.subscriptions
            .iter()
            .map(|s| s.outlet_to_subscription.clone())
            .collect()
    }
}

#[dyn_upcast]
#[async_trait]
impl Stage for Clearinghouse {
    #[inline]
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(level = "info", name = "run clearinghouse", skip(self))]
    async fn run(&mut self) -> GraphResult<()> {
        let mut inlet = self.inlet.clone();
        let rx_api = &mut self.rx_api;
        let database = &mut self.database;
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
                    let cont_loop = Self::handle_telemetry_data(data, subscriptions, database).await?;

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

    async fn close(mut self: Box<Self>) -> GraphResult<()> {
        tracing::trace!("closing clearinghouse.");
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

    #[inline]
    fn tx_api(&self) -> Self::Sender {
        self.tx_api.clone()
    }
}

// /////////////////////////////////////////////////////
// // Unit Tests ///////////////////////////////////////
//
#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::stage::{self, Stage, WithApi};
    use crate::graph::{Connect, SinkShape, SourceShape};
    use lazy_static::lazy_static;
    use std::collections::HashMap;
    use std::time::Duration;
    use tokio::sync::oneshot;
    use tokio_test::block_on;
    use tracing::Instrument;

    lazy_static! {
        //todo test optional fields
        static ref SUBSCRIPTIONS: Vec<TelemetrySubscription> = vec![
            TelemetrySubscription {
                name: "none".to_string(),
                required_fields: HashSet::default(),
                optional_fields: HashSet::default(),
                outlet_to_subscription: Outlet::new("none_outlet"),
            },
            TelemetrySubscription {
                name: "cat_pos".to_string(),
                required_fields: maplit::hashset!{"pos".to_string(), "cat".to_string()},
                optional_fields: HashSet::default(),
                outlet_to_subscription: Outlet::new("cat_pos_outlet"),
            },
            TelemetrySubscription {
                name: "all".to_string(),
                required_fields: maplit::hashset!{"pos".to_string(), "cat".to_string(), "value".to_string()},
                optional_fields: HashSet::default(),
                outlet_to_subscription: Outlet::new("all_outlet"),
            },
        ];

        static ref DB_ROWS: Vec<TelemetryData> = vec![
            TelemetryData::from_data(maplit::hashmap! {"pos".to_string() => "1".to_string(), "cat".to_string() => "Stella".to_string()}),
            TelemetryData::from_data(maplit::hashmap! {"value".to_string() => "3.14159".to_string(), "cat".to_string() => "Otis".to_string()}),
            TelemetryData::from_data(maplit::hashmap! {"pos".to_string() => "3".to_string(), "cat".to_string() => "Neo".to_string()}),
            TelemetryData::from_data(maplit::hashmap! {"pos".to_string() => "4".to_string(), "value".to_string() => "2.71828".to_string(), "cat".to_string() => "Apollo".to_string()}),
        ];

        static ref EXPECTED: HashMap<String, Vec<Option<TelemetryData>>> = maplit::hashmap! {
            SUBSCRIPTIONS[0].name.clone() => vec![
                None, //Some(HashMap::default()),
                None, //Some(HashMap::default()),
                None, //Some(HashMap::default()),
                None, //Some(HashMap::default()),
            ],
            SUBSCRIPTIONS[1].name.clone() => vec![
                Some(TelemetryData::from_data(maplit::hashmap! {"pos".to_string() => "1".to_string(), "cat".to_string() => "Stella".to_string()})),
                None,
                Some(TelemetryData::from_data(maplit::hashmap! {"pos".to_string() => "3".to_string(), "cat".to_string() => "Neo".to_string()})),
                Some(TelemetryData::from_data(maplit::hashmap! {"pos".to_string() => "4".to_string(), "cat".to_string() => "Apollo".to_string()})),
            ],
            SUBSCRIPTIONS[2].name.clone() => vec![
                None,
                None,
                None,
                Some(TelemetryData::from_data(maplit::hashmap! {"pos".to_string() => "4".to_string(), "value".to_string() => "2.71828".to_string(), "cat".to_string() => "Apollo".to_string()})),
            ],
        };
    }

    #[test]
    fn test_create_with_subscriptions() -> anyhow::Result<()> {
        lazy_static::initialize(&crate::telemetry::TEST_TRACING);
        let main_span = tracing::info_span!("test_create_with_subscriptions");
        let _main_span_guard = main_span.enter();

        let mut clearinghouse = Clearinghouse::new("test");
        assert!(clearinghouse.database.is_empty());
        assert!(clearinghouse.subscriptions.is_empty());
        assert_eq!(clearinghouse.name, "test");

        let sub1_inlet = Inlet::new("sub1");
        block_on(async {
            assert!(!clearinghouse.inlet.is_attached().await);

            //todo test optional fields
            clearinghouse
                .add_subscription(
                    TelemetrySubscription::new("sub1")
                        .with_required_fields(maplit::hashset! {"aaa".to_string(), "bbb".to_string()})
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
        lazy_static::initialize(&crate::telemetry::TEST_TRACING);
        let main_span = tracing::info_span!("test_api_add_subscriptions");
        let _main_span_guard = main_span.enter();

        block_on(async move {
            let data = TelemetryData::from_data(maplit::hashmap! { "aaa".to_string() => "17".to_string() });
            let mut tick = stage::Tick::new("tick", Duration::from_nanos(0), Duration::from_millis(5), data);
            let tx_tick_api = tick.tx_api();
            let mut clearinghouse = Clearinghouse::new("test-clearinghouse");
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
            let ClearinghouseResp::Snapshot {
                database: _db_0,
                missing: _missing_0,
                subscriptions: subs_0,
            } = rx_get_0.await?;
            let nr_subscriptions = subs_0.len();
            tracing::info!(%nr_subscriptions, "assert nr_subscriptions is 0...");
            assert_eq!(nr_subscriptions, 0);

            tracing::info!("sending add subscriber command to clearinghouse...");
            let sub1_inlet = Inlet::new("sub1");
            let (add_cmd, rx_add) = ClearinghouseCmd::subscribe(
                TelemetrySubscription::new("sub1")
                        .with_required_fields(maplit::hashset! {"aaa", "bbb"})
                        //todo: test optional fields
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
            let ClearinghouseResp::Snapshot {
                database: _db_1,
                missing: _missing_1,
                subscriptions: subs_1,
            } = rx_get_1.await?;
            let nr_subscriptions = subs_1.len();
            tracing::info!(%nr_subscriptions, "assert nr_subscriptions is now 1...");
            assert_eq!(nr_subscriptions, 1);

            tracing::warn!("stopping tick source...");
            let (tx_tick_stop, rx_tick_stop) = oneshot::channel();
            let stop_tick = stage::tick::TickMsg::Stop { tx: tx_tick_stop };
            tx_tick_api.send(stop_tick)?;
            rx_tick_stop.await??;

            tracing::info!("waiting for clearinghouse to stop...");
            tick_handle.await??;
            clear_handle.await??;
            tracing::info!("test finished");
            Ok(())
        })
    }

    #[test]
    fn test_api_remove_subscriptions() -> anyhow::Result<()> {
        lazy_static::initialize(&crate::telemetry::TEST_TRACING);
        let main_span = tracing::info_span!("test_api_remove_subscriptions");
        let _main_span_guard = main_span.enter();

        block_on(async move {
            let data = TelemetryData::from_data(maplit::hashmap! { "dr".to_string() => "17".to_string() });
            let mut tick = stage::Tick::new("tick", Duration::from_nanos(0), Duration::from_millis(5), data);
            let tx_tick_api = tick.tx_api();
            let mut clearinghouse = Clearinghouse::new("test-clearinghouse");
            let tx_api = clearinghouse.tx_api();
            (tick.outlet(), clearinghouse.inlet()).connect().await;

            let tick_handle = tokio::spawn(async move { tick.run().await });
            let clear_handle = tokio::spawn(async move { clearinghouse.run().await });

            let inlet_1 = Inlet::new("inlet_1");
            let (add, rx_add) = ClearinghouseCmd::subscribe(
                TelemetrySubscription::new("sub1")
                    .with_required_fields(maplit::hashset! { "dr".to_string() })
                //todo: test optional fields
                    .with_optional_fields(HashSet::<&str>::default()),
                inlet_1.clone(),
            );
            tx_api.send(add)?;

            let (get_1, rx_get_1) = ClearinghouseCmd::get_clearinghouse_snapshot();
            tx_api.send(get_1)?;

            rx_add.await?;
            let ClearinghouseResp::Snapshot {
                database: _,
                missing: _,
                subscriptions: subs1,
            } = rx_get_1.await?;
            assert_eq!(subs1.len(), 1);

            let name_1 = &subs1[0].name;
            let (remove, rx_remove) = ClearinghouseCmd::unsubscribe(name_1);
            tx_api.send(remove)?;
            rx_remove.await?;

            let (get_2, rx_get_2) = ClearinghouseCmd::get_clearinghouse_snapshot();
            tx_api.send(get_2)?;
            let ClearinghouseResp::Snapshot {
                database: _,
                missing: _,
                subscriptions: subs2,
            } = rx_get_2.await?;
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
        lazy_static::initialize(&crate::telemetry::TEST_TRACING);
        let main_span = tracing::info_span!("test_find_interested_subscriptions");
        let _main_span_guard = main_span.enter();

        let actual = Clearinghouse::find_interested_subscriptions(&SUBSCRIPTIONS, HashSet::default());
        assert_eq!(actual, Vec::<&TelemetrySubscription>::default());

        let actual =
            Clearinghouse::find_interested_subscriptions(&SUBSCRIPTIONS, maplit::hashset! {"extra".to_string()});
        assert_eq!(actual, Vec::<&TelemetrySubscription>::default());

        let actual = Clearinghouse::find_interested_subscriptions(&SUBSCRIPTIONS, maplit::hashset! {"pos".to_string()});
        assert_eq!(actual, vec![&SUBSCRIPTIONS[1], &SUBSCRIPTIONS[2]]);

        let actual =
            Clearinghouse::find_interested_subscriptions(&SUBSCRIPTIONS, maplit::hashset! {"value".to_string()});
        assert_eq!(actual, vec![&SUBSCRIPTIONS[2]]);

        let actual = Clearinghouse::find_interested_subscriptions(
            &SUBSCRIPTIONS,
            maplit::hashset! {"pos".to_string(), "cat".to_string()},
        );
        assert_eq!(actual, vec![&SUBSCRIPTIONS[1], &SUBSCRIPTIONS[2]]);

        let actual = Clearinghouse::find_interested_subscriptions(
            &SUBSCRIPTIONS,
            maplit::hashset! {"pos".to_string(), "value".to_string(), "cat".to_string()},
        );
        assert_eq!(actual, vec![&SUBSCRIPTIONS[1], &SUBSCRIPTIONS[2]]);

        let actual = Clearinghouse::find_interested_subscriptions(
            &SUBSCRIPTIONS,
            maplit::hashset! {"pos".to_string(), "value".to_string(), "cat".to_string(), "extra".to_string()},
        );
        assert_eq!(actual, vec![&SUBSCRIPTIONS[1], &SUBSCRIPTIONS[2]]);
    }

    #[test]
    fn test_fulfill_subscription() {
        lazy_static::initialize(&crate::telemetry::TEST_TRACING);
        let main_span = tracing::info_span!("test_fulfill_subscription");
        let _main_span_guard = main_span.enter();

        for subscriber in 0..=2 {
            let sub = &SUBSCRIPTIONS[subscriber];
            let expected = &EXPECTED[&sub.name];
            for ((row, data_row), expected_row) in DB_ROWS.iter().enumerate().zip(expected) {
                let actual = Clearinghouse::fulfill_subscription(sub.name.as_str(), &sub.required_fields, data_row);

                assert_eq!((subscriber, row, &actual), (subscriber, row, expected_row));
            }
        }
    }

    #[test]
    fn test_push_to_subscribers() {
        lazy_static::initialize(&crate::telemetry::TEST_TRACING);
        let main_span = tracing::info_span!("test_push_to_subscribers");
        let _main_span_guard = main_span.enter();

        let all_expected: HashMap<String, Vec<Option<TelemetryData>>> = maplit::hashmap! {
            SUBSCRIPTIONS[0].name.clone() => vec![
                None, //Some(HashMap::default()),
                None, //Some(HashMap::default()),
                None, //Some(HashMap::default()),
                None, //Some(HashMap::default()),
            ],
            SUBSCRIPTIONS[1].name.clone() => vec![
                Some(TelemetryData::from_data(maplit::hashmap! {"pos".to_string() => "1".to_string(), "cat".to_string() => "Stella".to_string()})),
                Some(TelemetryData::from_data(maplit::hashmap! {"pos".to_string() => "3".to_string(), "cat".to_string() => "Neo".to_string()})),
                Some(TelemetryData::from_data(maplit::hashmap! {"pos".to_string() => "4".to_string(), "cat".to_string() => "Apollo".to_string()})),
                None,
            ],
            SUBSCRIPTIONS[2].name.clone() => vec![
                Some(TelemetryData::from_data(maplit::hashmap! {"pos".to_string() => "4".to_string(), "value".to_string() => "2.71828".to_string(), "cat".to_string() => "Apollo".to_string()})),
                None,
                None,
                None,
            ],
        };
        block_on(async move {
            let nr_skip = 0; //todo expand test to remove this line
            let nr_take = SUBSCRIPTIONS.len(); //todo expand test to remove this line
            let subscriptions = SUBSCRIPTIONS
                .iter()
                .skip(nr_skip)//todo expand test to remove this line
                .take(nr_take)//todo expand test to remove this line
                .collect::<Vec<_>>();
            let mut sub_receivers = Vec::with_capacity(subscriptions.len());
            for s in SUBSCRIPTIONS.iter().skip(nr_skip).take(nr_take) {
                //todo expand test to remove this line
                let receiver: Inlet<TelemetryData> = Inlet::new(format!("recv_from_{}", s.name));
                (&s.outlet_to_subscription, &receiver).connect().await;
                sub_receivers.push((s, receiver));
            }

            for row in 0..DB_ROWS.len() {
                let db = &DB_ROWS[row];
                tracing::warn!(?db, "pushing database to subscribers");
                Clearinghouse::push_to_subscribers(db, subscriptions.clone())
                    .await
                    .expect("failed to publish");
            }

            for s in subscriptions.iter() {
                let mut outlet = s.outlet_to_subscription.clone();
                outlet.close().await;
            }

            for row in 0..DB_ROWS.len() {
                for (sub, receiver) in sub_receivers.iter_mut() {
                    let sub_name = &sub.name;
                    tracing::warn!(%sub_name, "test iteration");
                    let expected = &all_expected[&sub.name][row];
                    let actual: Option<TelemetryData> = receiver.recv().await;
                    tracing::warn!(%sub_name, ?actual, ?expected, "asserting scenario");
                    assert_eq!((row, sub_name, &actual), (row, sub_name, expected));
                }
            }
        })
    }
}
