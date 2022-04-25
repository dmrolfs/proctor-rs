use std::time::Duration;

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use frunk::{Monoid, Semigroup};
use tokio::sync::Mutex;

use crate::graph::stage::Stage;
use crate::graph::{Inlet, Outlet, Port, SinkShape, SourceShape, PORT_DATA};
use crate::{AppData, Correlation, ProctorResult};

#[derive(Debug)]
pub struct ReduceWithin<T> {
    name: String,
    pub initial_delay: Duration,
    pub interval: Duration,
    inlet: Inlet<T>,
    outlet: Outlet<T>,
}

impl<T> ReduceWithin<T> {
    pub fn new(name: impl Into<String>, initial_delay: Duration, interval: Duration) -> Self {
        let name = name.into();
        let inlet = Inlet::new(name.clone(), PORT_DATA);
        let outlet = Outlet::new(name.clone(), PORT_DATA);
        Self { name, initial_delay, interval, inlet, outlet }
    }
}

impl<T> SinkShape for ReduceWithin<T> {
    type In = T;

    fn inlet(&self) -> Inlet<T> {
        self.inlet.clone()
    }
}

impl<T> SourceShape for ReduceWithin<T> {
    type Out = T;

    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<T> Stage for ReduceWithin<T>
where
    T: AppData + Correlation + Monoid,
{
    #[inline]
    fn name(&self) -> &str {
        &self.name
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.inlet.check_attachment().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", name = "run reduce_within through", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        let mut ticker = self.make_ticker();
        let batch = Mutex::new(<Option<T> as Monoid>::empty());

        loop {
            tokio::select! {
                _next_tick = ticker.tick() => {
                    let mut b_guard = batch.lock().await;
                    tracing::debug!(batch=?*b_guard, "tick");
                    if let Some(ref b) = *b_guard {
                        tracing::debug!(batch=?b, "publishing batch");
                        self.outlet.send(b.clone()).await?;
                        *b_guard = <Option<T> as Monoid>::empty();
                    }
                },

                data = self.inlet.recv() => {
                    match data {
                        Some(d) => {
                            let mut b = batch.lock().await;
                            tracing::debug!(data=?d, batch=?b, "combining data with batch");
                            *b = b.combine(&Some(d));
                        },
                        None => {
                            match &*batch.lock().await {
                                Some(b) => {
                                    tracing::warn!(batch=?b, "received None from inlet - flushing batch and stopping");
                                    self.outlet.send(b.clone()).await?;
                                },
                                None => tracing::warn!("received None from inlet and nothing to flush - stopping"),
                            }

                            break;
                        },
                    }
                },

                else => {
                    tracing::error!("DMR: reduce_within: unexpected message");
                    break
                },
            }
        }

        Ok(())
    }

    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        tracing::trace!(stage=%self.name(), "closing reduce_within ports.");
        self.inlet.close().await;
        self.outlet.close().await;
        Ok(())
    }
}

impl<T> ReduceWithin<T> {
    fn make_ticker(&self) -> tokio::time::Interval {
        let start = tokio::time::Instant::now() + self.initial_delay;
        tokio::time::interval_at(start, self.interval)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use claim::*;
    use pretty_assertions::assert_eq;
    use pretty_snowflake::Id;
    use tokio::sync::mpsc;
    use tokio_test::block_on;

    use super::*;
    use crate::elements::{Telemetry, TelemetryValue};

    #[derive(Debug, Clone)]
    struct TestData {
        pub correlation_id: Id<Self>,
        pub telemetry: Telemetry,
    }

    impl TestData {
        pub fn new(telemetry: Telemetry) -> Self {
            Self {
                correlation_id: Id::direct("TestData", 123, "ABC"),
                telemetry,
            }
        }
    }

    impl PartialEq for TestData {
        fn eq(&self, other: &Self) -> bool {
            self.telemetry == other.telemetry
        }
    }

    impl Correlation for TestData {
        type Correlated = Self;

        fn correlation(&self) -> &Id<Self::Correlated> {
            &self.correlation_id
        }
    }

    impl Monoid for TestData {
        fn empty() -> Self {
            Self::new(Telemetry::empty())
        }
    }

    impl Semigroup for TestData {
        fn combine(&self, other: &Self) -> Self {
            let mut telemetry = self.telemetry.clone();
            telemetry.extend(other.telemetry.clone());
            Self {
                correlation_id: self.correlation_id.clone(),
                telemetry,
            }
        }
    }

    impl From<HashMap<String, TelemetryValue>> for TestData {
        fn from(telemetry: HashMap<String, TelemetryValue>) -> Self {
            Self {
                correlation_id: Id::direct("TestData", 123, "ABC"),
                telemetry: telemetry.into(),
            }
        }
    }

    #[test]
    fn test_basic_reduce_within_usage() {
        once_cell::sync::Lazy::force(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_basic_reduce_within_usage");
        let _main_span_guard = main_span.enter();

        let phase_1_data: Vec<TestData> = vec![
            maplit::hashmap! {
                "name".to_string() => "Stella".into(),
                "age".to_string() => 16.into(),
                "has_tail".to_string() => false.into(),
            }
            .into(),
            maplit::hashmap! {
                "name".to_string() => "Otis".into(),
                "age".to_string() => 6.into(),
                "color".to_string() => "toasty".into()
            }
            .into(),
        ];

        let phase_2_data: Vec<TestData> = vec![maplit::hashmap! {
            "name".to_string() => "Neo".into(),
            "age".to_string() => 2.into(),
            "crazy-factor".to_string() => 11.into()
        }
        .into()];

        let (tx_in, rx_in) = mpsc::channel(8);
        let (tx_out, mut rx_out) = mpsc::channel(8);
        let p2_tx = tx_in.clone();

        let mut stage = ReduceWithin::new("test_batching", Duration::from_millis(10), Duration::from_millis(10));

        block_on(async {
            stage.inlet.attach("test_channel".into(), rx_in).await;
            stage.outlet.attach("test_channel".into(), tx_out).await;

            let phase_1_handle = tokio::spawn(async move {
                for d in phase_1_data {
                    assert_ok!(tx_in.send(d).await);
                }
            });

            let phase_2_handle = tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(55)).await;
                for d in phase_2_data {
                    assert_ok!(p2_tx.send(d).await);
                }
            });

            let stage_handle = tokio::spawn(async move {
                assert_ok!(stage.run().await);
            });

            assert_ok!(phase_1_handle.await);
            assert_ok!(phase_2_handle.await);
            assert_ok!(stage_handle.await);
            let mut actual = vec![];
            while let Some(data) = rx_out.recv().await {
                actual.push(data);
            }

            assert_eq!(
                actual,
                vec![
                    TestData::new(
                        maplit::hashmap! {
                            "name".to_string() => "Otis".into(),
                            "age".to_string() => 6.into(),
                            "has_tail".to_string() => false.into(),
                            "color".to_string() => "toasty".into(),
                        }
                        .into()
                    ),
                    TestData::new(
                        maplit::hashmap! {
                            "name".to_string() => "Neo".into(),
                            "age".to_string() => 2.into(),
                            "crazy-factor".to_string() => 11.into(),
                        }
                        .into()
                    ),
                ],
            );
        });
    }
}
