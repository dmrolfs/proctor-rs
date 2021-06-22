use std::collections::HashSet;
use std::fmt::{self, Debug};

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use futures::future::FutureExt;
use tokio::sync::{mpsc, oneshot};

use crate::elements::Telemetry;
use crate::error::CollectionError;
use crate::graph::stage::Stage;
use crate::graph::{stage, Connect, Inlet, Outlet, OutletsShape, Port, SinkShape, UniformFanOutShape};
use crate::{Ack, ProctorResult};

pub type ClearinghouseApi = mpsc::UnboundedSender<ClearinghouseCmd>;

#[derive(Debug)]
pub enum ClearinghouseCmd {
    Subscribe {
        subscription: TelemetrySubscription,
        receiver: Inlet<Telemetry>,
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
        subscription: TelemetrySubscription, receiver: Inlet<Telemetry>,
    ) -> (Self, oneshot::Receiver<Ack>) {
        let (tx, rx) = oneshot::channel();
        (Self::Subscribe { subscription, receiver, tx }, rx)
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
        (Self::GetSnapshot { name: Some(name.into()), tx }, rx)
    }
}

#[derive(Debug)]
pub enum ClearinghouseResp {
    Snapshot {
        database: Telemetry,
        missing: HashSet<String>,
        subscriptions: Vec<TelemetrySubscription>,
    },
}

#[derive(Debug, Clone)]
pub enum TelemetrySubscription {
    All {
        name: String,
        outlet_to_subscription: Outlet<Telemetry>,
    },
    Explicit {
        name: String,
        required_fields: HashSet<String>,
        optional_fields: HashSet<String>,
        outlet_to_subscription: Outlet<Telemetry>,
    },
    /* Remainder {
     *     name: String,
     *     outlet_to_subscription: Outlet<TelemetryData>,
     * } */
}

impl TelemetrySubscription {
    pub fn new<S: Into<String>>(name: S) -> Self {
        let name = name.into();
        let outlet_to_subscription = Outlet::new(format!("outlet_for_subscription_{}", name));
        Self::All { name, outlet_to_subscription }
    }

    pub fn with_required_fields<S: Into<String>>(self, required_fields: HashSet<S>) -> Self {
        let required_fields = required_fields.into_iter().map(|s| s.into()).collect();

        match self {
            Self::All { name, outlet_to_subscription } => Self::Explicit {
                name,
                required_fields,
                optional_fields: HashSet::default(),
                outlet_to_subscription,
            },
            Self::Explicit {
                name,
                required_fields: mut my_required_fields,
                optional_fields,
                outlet_to_subscription,
            } => {
                my_required_fields.extend(required_fields);
                Self::Explicit {
                    name,
                    required_fields: my_required_fields,
                    optional_fields,
                    outlet_to_subscription,
                }
            },
        }
    }

    pub fn with_optional_fields<S: Into<String>>(self, optional_fields: HashSet<S>) -> Self {
        let optional_fields = optional_fields.into_iter().map(|s| s.into()).collect();
        match self {
            Self::All { name, outlet_to_subscription } => Self::Explicit {
                name,
                required_fields: HashSet::default(),
                optional_fields,
                outlet_to_subscription,
            },
            Self::Explicit {
                name,
                required_fields,
                optional_fields: mut my_optional_fields,
                outlet_to_subscription,
            } => {
                my_optional_fields.extend(optional_fields);
                Self::Explicit {
                    name,
                    required_fields,
                    optional_fields: my_optional_fields,
                    outlet_to_subscription,
                }
            },
        }
    }

    // pub fn remainder<S: Into<String>>(name: S) -> Self {
    //     let name = name.into();
    //     let outlet_to_subscription = Outlet::new(format!("outlet_for_subscription_{}", name));
    //     Self::Remainder { name, outlet_to_subscription }
    // }

    pub fn name(&self) -> &str {
        match self {
            Self::All { name, outlet_to_subscription: _ } => name.as_str(),
            Self::Explicit {
                name,
                required_fields: _,
                optional_fields: _,
                outlet_to_subscription: _,
            } => name.as_str(),
        }
    }

    pub fn outlet_to_subscription(&self) -> Outlet<Telemetry> {
        match self {
            Self::All { name: _, outlet_to_subscription } => outlet_to_subscription.clone(),
            Self::Explicit {
                name: _,
                required_fields: _,
                optional_fields: _,
                outlet_to_subscription,
            } => outlet_to_subscription.clone(),
        }
    }

    pub fn any_interest(&self, _available_fields: &HashSet<&String>, changed_fields: &HashSet<String>) -> bool {
        match self {
            Self::All { .. } => true,
            Self::Explicit {
                name: _,
                required_fields,
                optional_fields,
                outlet_to_subscription: _,
            } => {
                let mut interested = false;
                for changed in changed_fields {
                    if required_fields.contains(changed) || optional_fields.contains(changed) {
                        interested = true;
                        break;
                    }
                }
                interested
            },
        }
    }

    pub fn trim_to_subscription(&self, database: &Telemetry) -> Result<(Telemetry, HashSet<String>), CollectionError> {
        let mut db = database.clone();

        match self {
            Self::All { .. } => Ok((db, HashSet::default())),
            Self::Explicit {
                name: _,
                required_fields,
                optional_fields,
                outlet_to_subscription: _,
            } => {
                let mut missing = HashSet::default();
                for (key, _value) in database.iter() {
                    if !required_fields.contains(key) && !optional_fields.contains(key) {
                        let _ = db.remove(key);
                    }
                }

                for req in required_fields {
                    if !db.contains_key(req) {
                        missing.insert(req.clone());
                    }
                }

                for opt in optional_fields {
                    if !db.contains_key(opt) {
                        missing.insert(opt.clone());
                    }
                }

                Ok((db, missing))
            },
        }
    }

    #[tracing::instrument(level = "info")]
    pub fn fulfill(&self, database: &Telemetry) -> Option<Telemetry> {
        match self {
            Self::All { .. } => Some(database.clone()),
            Self::Explicit {
                name: _,
                required_fields,
                optional_fields,
                outlet_to_subscription: _,
            } => {
                let mut ready = Vec::new();
                let mut unfilled = Vec::new();

                for required in required_fields.iter() {
                    match database.get(required) {
                        Some(value) => ready.push((required.clone(), value)),
                        None => unfilled.push(required),
                    }
                }

                if unfilled.is_empty() {
                    for optional in optional_fields.iter() {
                        tracing::trace!(?optional, "looking for optional.");
                        if let Some(value) = database.get(optional) {
                            ready.push((optional.clone(), value))
                        }
                    }
                }

                tracing::trace!(?ready, ?unfilled, subscription=?self, "fulfilling required and optional fields.");
                if ready.is_empty() || !unfilled.is_empty() {
                    tracing::info!(
                        subscription=?self,
                        unfilled_fields=?unfilled,
                        "unsatisfided subscription - not publishing."
                    );

                    None
                } else {
                    let ready = ready.into_iter().map(|(k, v)| (k, v.clone())).collect();
                    Some(ready)
                }
            },
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn connect_to_receiver(&self, receiver: &Inlet<Telemetry>) {
        let outlet = self.outlet_to_subscription();
        (&outlet, receiver).connect().await;
    }

    pub async fn send(&self, telemetry: Telemetry) -> Result<(), CollectionError> {
        self.outlet_to_subscription().send(telemetry).await?;
        Ok(())
    }
}

impl TelemetrySubscription {
    #[tracing::instrument()]
    pub async fn close(self) { self.outlet_to_subscription().close().await; }
}

impl PartialEq for TelemetrySubscription {
    fn eq(&self, other: &Self) -> bool {
        use TelemetrySubscription::*;

        match (self, other) {
            (All { name: lhs_name, outlet_to_subscription: _ }, All { name: rhs_name, outlet_to_subscription: _ }) => {
                lhs_name == rhs_name
            },
            (
                Explicit {
                    name: lhs_name,
                    required_fields: lhs_required,
                    optional_fields: lhs_optional,
                    outlet_to_subscription: _,
                },
                Explicit {
                    name: rhs_name,
                    required_fields: rhs_required,
                    optional_fields: rhs_optional,
                    outlet_to_subscription: _,
                },
            ) => (lhs_name == rhs_name) && (lhs_required == rhs_required) && (lhs_optional == rhs_optional),
            _ => false,
        }
    }
}

/// Clearinghouse is a sink for collected telemetry data and a subscription-based source for
/// groups of telemetry fields.
///
pub struct Clearinghouse {
    name: String,
    subscriptions: Vec<TelemetrySubscription>,
    database: Telemetry,
    inlet: Inlet<Telemetry>,
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
            database: Telemetry::default(),
            inlet,
            tx_api,
            rx_api,
        }
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn add_subscription(&mut self, subscription: TelemetrySubscription, receiver: &Inlet<Telemetry>) {
        tracing::info!(stage=%self.name, ?subscription, "adding clearinghouse subscription.");
        subscription.connect_to_receiver(receiver).await;
        self.subscriptions.push(subscription);
    }

    #[tracing::instrument(level = "trace", skip(subscriptions, database,))]
    async fn handle_telemetry_data(
        data: Option<Telemetry>, subscriptions: &Vec<TelemetrySubscription>, database: &mut Telemetry,
    ) -> Result<bool, CollectionError> {
        match data {
            Some(d) => {
                let updated_fields = d.keys().cloned().collect::<HashSet<_>>();
                let interested =
                    Self::find_interested_subscriptions(subscriptions, database.keys().collect(), updated_fields);

                database.extend(d);
                Self::push_to_subscribers(database, interested).await?;
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
    fn find_interested_subscriptions<'a>(
        subscriptions: &'a Vec<TelemetrySubscription>, available: HashSet<&String>, changed: HashSet<String>,
    ) -> Vec<&'a TelemetrySubscription> {
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
        skip(database, subscribers),
        fields(subscribers = ?subscribers.iter().map(|s| s.name()).collect::<Vec<_>>(), ),
    )]
    async fn push_to_subscribers(
        database: &Telemetry, subscribers: Vec<&TelemetrySubscription>,
    ) -> Result<(), CollectionError> {
        if subscribers.is_empty() {
            tracing::info!("not publishing - no subscribers corresponding to field changes.");
            return Ok(());
        }

        let nr_subscribers = subscribers.len();
        let fulfilled = subscribers
            .into_iter()
            .map(|s| Self::fulfill_subscription(&s, database).map(|fulfillment| (s, fulfillment)))
            .flatten()
            .map(|(s, fulfillment)| {
                tracing::info!(subscription=%s.name(), "sending subscription data update.");
                s.send(fulfillment).map(move |send_status| (s, send_status))
            })
            .collect::<Vec<_>>();

        let nr_fulfilled = fulfilled.len();

        let statuses = futures::future::join_all(fulfilled).await;
        let result = if let Some((s, err)) = statuses.into_iter().find(|(_, status)| status.is_err()) {
            tracing::error!(subscription=%s.name(), "failed to send fulfilled subscription.");
            err
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

    #[tracing::instrument(level = "trace")]
    fn fulfill_subscription(subscription: &TelemetrySubscription, database: &Telemetry) -> Option<Telemetry> {
        subscription.fulfill(database)
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
                        ClearinghouseResp::Snapshot {
                            database: database.clone(),
                            missing: HashSet::default(),
                            subscriptions: subscriptions.clone(),
                        }
                    },

                    Some(name) => match subscriptions.iter().find(|s| s.name() == name.as_str()) {
                        Some(sub) => {
                            let (db, missing) = sub.trim_to_subscription(&database)?;

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
                        },

                        None => {
                            tracing::info!(requested_subscription=%name, "subscription not found - returning clearinghouse snapshot.");
                            ClearinghouseResp::Snapshot {
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
                subscriptions.push(subscription);
                let _ = tx.send(());
                Ok(true)
            },

            ClearinghouseCmd::Unsubscribe { name, tx } => {
                // let mut subs = subscriptions.lock().await;
                let dropped = subscriptions
                    .iter()
                    .position(|s| s.name() == name.as_str())
                    .map(|pos| subscriptions.remove(pos));

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

    #[inline]
    fn inlet(&self) -> Inlet<Self::In> { self.inlet.clone() }
}

impl UniformFanOutShape for Clearinghouse {
    type Out = Telemetry;

    #[inline]
    fn outlets(&self) -> OutletsShape<Self::Out> {
        self.subscriptions.iter().map(|s| s.outlet_to_subscription()).collect()
    }
}

#[dyn_upcast]
#[async_trait]
impl Stage for Clearinghouse {
    #[inline]
    fn name(&self) -> &str { self.name.as_str() }

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
    #[inline]
    async fn do_check(&self) -> Result<(), CollectionError> {
        self.inlet.check_attachment().await?;

        for s in self.subscriptions.iter() {
            s.outlet_to_subscription().check_attachment().await?;
        }

        Ok(())
    }

    #[inline]
    async fn do_run(&mut self) -> Result<(), CollectionError> {
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

    #[inline]
    async fn do_close(mut self: Box<Self>) -> Result<(), CollectionError> {
        tracing::warn!("DMR: closing clearinghouse.");
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
    fn tx_api(&self) -> Self::Sender { self.tx_api.clone() }
}

// /////////////////////////////////////////////////////
// // Unit Tests ///////////////////////////////////////
//
#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use lazy_static::lazy_static;
    use pretty_assertions::assert_eq;
    use tokio::sync::oneshot;
    use tokio_test::block_on;
    use tracing::Instrument;

    use super::*;
    use crate::elements::telemetry::ToTelemetry;
    use crate::graph::stage::{self, Stage, WithApi};
    use crate::graph::{Connect, SinkShape, SourceShape};

    lazy_static! {
        static ref SUBSCRIPTIONS: Vec<TelemetrySubscription> = vec![
            TelemetrySubscription::Explicit {
                name: "none".to_string(),
                required_fields: HashSet::default(),
                optional_fields: HashSet::default(),
                outlet_to_subscription: Outlet::new("none_outlet"),
            },
            TelemetrySubscription::Explicit {
                name: "cat_pos".to_string(),
                required_fields: maplit::hashset! {"pos".to_string(), "cat".to_string()},
                optional_fields: maplit::hashset! {"extra".to_string()},
                outlet_to_subscription: Outlet::new("cat_pos_outlet"),
            },
            TelemetrySubscription::Explicit {
                name: "all".to_string(),
                required_fields: maplit::hashset! {"pos".to_string(), "cat".to_string(), "value".to_string()},
                optional_fields: HashSet::default(),
                outlet_to_subscription: Outlet::new("all_outlet"),
            },
        ];
        static ref DB_ROWS: Vec<Telemetry> = vec![
            maplit::btreemap! {"pos".to_string() => 1.into(), "cat".to_string() => "Stella".into(), "extra".to_string() => "Ripley".into(),}.into_iter().collect(),
            maplit::btreemap! {"value".to_string() => 3.14159.into(), "cat".to_string() => "Otis".into(), "extra".to_string() => "Ripley".into(),}.into_iter().collect(),
            maplit::btreemap! {"pos".to_string() => 3.into(), "cat".to_string() => "Neo".into(), "extra".to_string() => "Ripley".into(),}.into_iter().collect(),
            maplit::btreemap! {"pos".to_string() => 4.into(), "value".to_string() => 2.71828.into(), "cat".to_string() => "Apollo".into(), "extra".to_string() => "Ripley".into(),}.into_iter().collect(),
        ];
        static ref EXPECTED: HashMap<String, Vec<Option<Telemetry>>> = maplit::hashmap! {
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
        };
    }

    #[test]
    fn test_create_with_subscriptions() -> anyhow::Result<()> {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_create_with_subscriptions");
        let _main_span_guard = main_span.enter();

        let mut clearinghouse = Clearinghouse::new("test");
        assert!(clearinghouse.database.is_empty());
        assert!(clearinghouse.subscriptions.is_empty());
        assert_eq!(clearinghouse.name, "test");

        let sub1_inlet = Inlet::new("sub1");
        block_on(async {
            assert!(!clearinghouse.inlet.is_attached().await);

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
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_api_add_subscriptions");
        let _main_span_guard = main_span.enter();

        block_on(async move {
            let data: Telemetry = maplit::hashmap! { "aaa".to_string() => 17.to_telemetry() }
                .into_iter()
                .collect();
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
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_api_remove_subscriptions");
        let _main_span_guard = main_span.enter();

        block_on(async move {
            let data: Telemetry = maplit::hashmap! { "dr".to_string() => 17.to_telemetry() }
                .into_iter()
                .collect();
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
                    .with_optional_fields(HashSet::<&str>::default()),
                inlet_1.clone(),
            );
            tx_api.send(add)?;

            let (get_1, rx_get_1) = ClearinghouseCmd::get_clearinghouse_snapshot();
            tx_api.send(get_1)?;

            rx_add.await?;
            let ClearinghouseResp::Snapshot { database: _, missing: _, subscriptions: subs1 } = rx_get_1.await?;
            assert_eq!(subs1.len(), 1);

            let name_1 = subs1[0].name();
            let (remove, rx_remove) = ClearinghouseCmd::unsubscribe(name_1);
            tx_api.send(remove)?;
            rx_remove.await?;

            let (get_2, rx_get_2) = ClearinghouseCmd::get_clearinghouse_snapshot();
            tx_api.send(get_2)?;
            let ClearinghouseResp::Snapshot { database: _, missing: _, subscriptions: subs2 } = rx_get_2.await?;
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
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
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
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_fulfill_subscription");
        let _main_span_guard = main_span.enter();

        for subscriber in 0..=2 {
            // let sub = &SUBSCRIPTIONS[subscriber];
            if let TelemetrySubscription::Explicit {
                name: sub_name,
                required_fields: sub_required_fields,
                optional_fields: sub_optional_fields,
                outlet_to_subscription: _,
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
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_push_to_subscribers");
        let _main_span_guard = main_span.enter();

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
                let receiver: Inlet<Telemetry> = Inlet::new(format!("recv_from_{}", s.name()));
                (&s.outlet_to_subscription(), &receiver).connect().await;
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
                let mut outlet = s.outlet_to_subscription();
                outlet.close().await;
            }

            for row in 0..DB_ROWS.len() {
                for (sub, receiver) in sub_receivers.iter_mut() {
                    let sub_name = sub.name();
                    tracing::warn!(%sub_name, "test iteration");
                    let expected = &all_expected[sub_name][row];
                    let actual: Option<Telemetry> = receiver.recv().await;
                    tracing::warn!(%sub_name, ?actual, ?expected, "asserting scenario");
                    assert_eq!((row, sub_name, &actual), (row, sub_name, expected));
                }
            }
        })
    }
}
