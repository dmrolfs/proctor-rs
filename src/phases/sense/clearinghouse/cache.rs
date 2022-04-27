use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashSet;
use serde::{Deserialize, Serialize};
use stretto::{AsyncCache, CacheError};

use crate::elements::{Telemetry, TelemetryValue};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct TelemetryCacheSettings {
    /// Optional setting for the time to live for each clearinghouse field. The default is 5
    /// minutes. The clearinghouse is cleared during rescaling, so this value should be set to a
    /// duration reflecting a break in connectivity or the job not running.
    #[serde(
        rename = "ttl_secs",
        default = "TelemetryCacheSettings::default_ttl",
        serialize_with = "crate::serde::serialize_duration_secs",
        deserialize_with = "crate::serde::deserialize_duration_secs"
    )]
    pub ttl: Duration,

    /// Optional setting for the cache access counter kept for admission and eviction. The default
    /// is 1000. A good  value is to set nr_counters to be 10x the number of *unique* fields
    /// expected to be kept in the clearinghouse cache when full, not necessarily "cost"
    /// related.
    pub nr_counters: usize,

    /// Optional setting for maximum cost of the items stored in the cache. The default is 100,
    /// which assumes the normal case where the per-item cost is 1.
    pub max_cost: i64,

    /// Derived field for the incremental cost of an item to the cache, which is used in an
    /// item-count cost model.
    pub incremental_item_cost: i64,

    /// Optional setting to direct how frequent the cache is checked for eviction. The default is 5
    /// seconds. A good setting should consider the frequency of data pushed into the clearinghouse.
    #[serde(
        rename = "cleanup_interval_millis",
        default = "TelemetryCacheSettings::default_cleanup_interval",
        serialize_with = "crate::serde::serialize_duration_millis",
        deserialize_with = "crate::serde::deserialize_duration_millis"
    )]
    pub cleanup_interval: Duration,
}

impl Default for TelemetryCacheSettings {
    fn default() -> Self {
        Self {
            ttl: Self::default_ttl(),
            nr_counters: 1_000,
            max_cost: 100, // 1e6 as i64,
            incremental_item_cost: 1,
            cleanup_interval: Self::default_cleanup_interval(),
        }
    }
}

impl TelemetryCacheSettings {
    pub const fn default_ttl() -> Duration {
        Duration::from_secs(5 * 60)
    }

    pub const fn default_cleanup_interval() -> Duration {
        Duration::from_secs(5)
    }
}

/// TelemetryCache wraps a thread-safe async implementation of a hashmap with a TinyLFU admission
/// policy and a Sampled LFU eviction policy. You can use the same TelemetryCache instance
/// from as many threads as you want.
///
///
/// # Features
/// * **Internal Mutability** - Do not need to use `Arc<RwLock<TelemetryCache>` for concurrent code,
///   you just need `Cache<...>`
/// * **Async** - Cache support async via `tokio`.
///   * In async, Cache starts two extra green threads. One is policy thread, the other is writing
///     thread.
/// * **High Hit Ratios** - with our unique admission/eviction policy pairing, Ristretto's
///   performance is best in class.
///     * **Eviction: SampledLFU** - on par with exact LRU and better performance on Search and
///       Database traces.
///     * **Admission: TinyLFU** - extra performance with little memory overhead (12 bits per
///       counter).
/// * **Fast Throughput** - we use a variety of techniques for managing contention and the result is
///   excellent throughput.
/// * **Cost-Based Eviction** - any large new item deemed valuable can evict multiple smaller items
///   (cost could be anything).
/// * **Fully Concurrent** - you can use as many threads as you want with little throughput
///   degradation.
/// * **Metrics** - optional performance metrics for throughput, hit ratios, and other stats.
pub struct TelemetryCache {
    cache: AsyncCache<String, TelemetryValue>,
    seen: Arc<DashSet<String>>,
}

impl fmt::Debug for TelemetryCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TelemetryCache").field("seen", &self.seen.len()).finish()
    }
}

impl std::ops::Deref for TelemetryCache {
    type Target = AsyncCache<String, TelemetryValue>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.cache
    }
}

#[allow(dead_code)]
impl TelemetryCache {
    pub fn new(settings: &TelemetryCacheSettings) -> Self {
        let cache = Self::make_cache(settings);
        Self { cache, seen: Arc::new(DashSet::default()) }
    }

    fn make_cache(cache_settings: &TelemetryCacheSettings) -> AsyncCache<String, TelemetryValue> {
        AsyncCache::builder(cache_settings.nr_counters, cache_settings.max_cost)
            .set_metrics(true)
            .set_cleanup_duration(cache_settings.cleanup_interval)
            .set_ignore_internal_cost(true)
            .finalize()
            .expect("failed creating clearinghouse cache")
    }

    #[tracing::instrument(level = "debug")]
    pub async fn insert(&mut self, key: String, val: TelemetryValue, cost: i64) -> bool {
        let result = self.cache.insert(key.clone(), val, cost).await;
        if result {
            self.seen.insert(key);
        }
        result
    }

    #[tracing::instrument(level = "debug")]
    pub async fn try_insert(&mut self, key: String, val: TelemetryValue, cost: i64) -> Result<bool, CacheError> {
        let result = self
            .cache
            .try_insert_with_ttl(key.clone(), val, cost, Duration::ZERO)
            .await;
        if let Ok(is_inserted) = result {
            if is_inserted {
                self.seen.insert(key);
            }
        }
        result
    }

    #[tracing::instrument(level = "debug")]
    pub async fn insert_with_ttl(&mut self, key: String, val: TelemetryValue, cost: i64, ttl: Duration) -> bool {
        let result = self.cache.insert_with_ttl(key.clone(), val, cost, ttl).await;
        if result {
            self.seen.insert(key);
        }
        result
    }

    #[tracing::instrument(level = "debug")]
    pub async fn try_insert_with_ttl(
        &mut self, key: String, val: TelemetryValue, cost: i64, ttl: Duration,
    ) -> Result<bool, CacheError> {
        let result = self.cache.try_insert_with_ttl(key.clone(), val, cost, ttl).await;
        if let Ok(is_inserted) = result {
            if is_inserted {
                self.seen.insert(key);
            }
        }
        result
    }

    #[tracing::instrument(level = "debug")]
    pub async fn insert_if_present(&mut self, key: String, val: TelemetryValue, cost: i64) -> bool {
        let result = self.cache.insert_if_present(key.clone(), val, cost).await;
        if result {
            self.seen.insert(key);
        }
        result
    }

    #[tracing::instrument(level = "debug")]
    pub async fn try_insert_if_present(
        &mut self, key: String, val: TelemetryValue, cost: i64,
    ) -> Result<bool, CacheError> {
        let result = self.cache.try_insert_if_present(key.clone(), val, cost).await;
        if let Ok(is_inserted) = result {
            if is_inserted {
                self.seen.insert(key);
            }
        }
        result
    }

    #[tracing::instrument(level = "debug")]
    pub async fn wait(&self) -> Result<(), CacheError> {
        let result = self.cache.wait().await;
        tracing::debug!(
            wait=?result,
            cost_added=?self.cache.metrics.get_cost_added(),
            cost_evicted=?self.cache.metrics.get_cost_evicted(),
            "cache metrics after cache operation wait"
        );
        result
    }

    pub fn clear(&self) -> Result<(), CacheError> {
        let result = self.cache.clear();
        if result.is_ok() {
            self.seen.clear();
        }
        result
    }

    pub fn seen(&self) -> HashSet<String> {
        self.seen.iter().map(|k| k.key().clone()).collect()
    }

    pub fn get_telemetry(&self) -> Telemetry {
        let telemetry: crate::elements::telemetry::TableType = self
            .seen
            .iter()
            .filter_map(|field| {
                self.cache
                    .get(field.key())
                    .map(|v| (field.key().clone(), v.value().clone()))
            })
            .collect();

        telemetry.into()
    }
}

#[cfg(test)]
mod tests {
    use claim::*;
    use pretty_assertions::assert_eq;
    use serde_test::{assert_tokens, Token};
    use tokio_test::block_on;

    use super::*;

    #[ignore = "stretto bug: use after clear does not work"]
    #[test]
    fn test_basic_cache_add_after_clear() {
        let ttl = Duration::from_secs(60);
        block_on(async {
            let cache = AsyncCache::builder(1000, 100)
                .set_metrics(true)
                .set_ignore_internal_cost(true)
                .finalize()
                .expect("failed creating cache");

            assert!(cache.insert_with_ttl("foo".to_string(), 17.to_string(), 1, ttl).await);
            assert!(cache.insert_with_ttl("bar".to_string(), "otis".to_string(), 1, ttl).await);
            assert_ok!(cache.wait().await);

            assert_eq!(assert_some!(cache.get(&"foo".to_string())).value(), &17.to_string());
            assert_eq!(assert_some!(cache.get(&"bar".to_string())).value(), &"otis".to_string());

            assert_ok!(cache.clear());
            assert_ok!(cache.wait().await);

            assert_none!(cache.get(&"foo".to_string()));

            assert!(cache.insert_with_ttl("zed".to_string(), 33.to_string(), 1, ttl).await);
            assert_ok!(cache.wait().await);

            assert_none!(cache.get(&"bar".to_string()));
            assert_eq!(assert_some!(cache.get(&"zed".to_string())).value(), &33.to_string());
        });
    }

    #[ignore = "stretto bug: use after clear does not work"]
    #[test]
    fn test_cache_add_after_clear() {
        let ttl = Duration::from_secs(60);
        block_on(async {
            // let cache = AsyncCache::builder(1000, 100)
            //     .set_metrics(true)
            //     .set_ignore_internal_cost(true)
            //     .finalize()
            //     .expect("failed creating clearinghouse cache");

            let settings = TelemetryCacheSettings {
                ttl,
                nr_counters: 1_000,
                max_cost: 100,
                incremental_item_cost: 1,
                cleanup_interval: Duration::from_millis(500),
            };

            let mut cache = TelemetryCache::new(&settings);
            assert!(cache.insert_with_ttl("foo".into(), 17.into(), 1, ttl).await);
            assert!(cache.insert_with_ttl("bar".into(), "otis".into(), 1, ttl).await);
            assert_ok!(cache.wait().await);

            assert_eq!(cache.seen(), maplit::hashset!["foo".into(), "bar".into()]);
            assert_eq!(
                cache.get_telemetry(),
                Telemetry::from(maplit::hashmap! {
                    "foo".into() => TelemetryValue::Integer(17),
                    "bar".into() => TelemetryValue::Text("otis".into()),
                })
            );

            assert_ok!(cache.clear());
            assert_ok!(cache.wait().await);

            assert!(cache.seen().is_empty());
            assert!(cache.get_telemetry().is_empty());

            assert!(
                cache
                    .insert_with_ttl("zed".into(), TelemetryValue::Seq(vec![17.into()]), 1, ttl)
                    .await
            );
            assert_ok!(cache.wait().await);

            assert_eq!(cache.seen(), maplit::hashset!["zed".into()]);
            assert_eq!(
                cache.get_telemetry(),
                Telemetry::from(maplit::hashmap! {
                    "zed".into() => TelemetryValue::Seq(vec![TelemetryValue::Integer(17)]),
                })
            )
        });
    }

    #[test]
    fn test_cache_settings_serde_tokens() {
        let settings = TelemetryCacheSettings::default();
        assert_tokens(
            &settings,
            &vec![
                Token::Struct { name: "TelemetryCacheSettings", len: 5 },
                Token::Str("ttl_secs"),
                Token::U64(300),
                Token::Str("nr_counters"),
                Token::U64(1_000),
                Token::Str("max_cost"),
                Token::I64(100),
                Token::Str("incremental_item_cost"),
                Token::I64(1),
                Token::Str("cleanup_interval_millis"),
                Token::U64(5000),
                Token::StructEnd,
            ],
        )
    }
}
