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
    #[serde(
        default = "TelemetryCacheSettings::default_ttl",
        serialize_with = "crate::serde::serialize_duration_secs",
        deserialize_with = "crate::serde::deserialize_duration_secs"
    )]
    pub ttl: Duration,
    pub num_counters: usize,
    pub max_cost: i64,
    #[serde(
        default = "TelemetryCacheSettings::default_cleanup_interval",
        serialize_with = "crate::serde::serialize_duration_secs",
        deserialize_with = "crate::serde::deserialize_duration_secs"
    )]
    pub cleanup_interval: Duration,
    pub ignore_memory_cost: bool,
}

impl Default for TelemetryCacheSettings {
    fn default() -> Self {
        Self {
            ttl: Self::default_ttl(),
            num_counters: 1_000,
            max_cost: 100, // 1e6 as i64,
            cleanup_interval: Self::default_cleanup_interval(),
            ignore_memory_cost: true,
        }
    }
}

impl TelemetryCacheSettings {
    pub const fn default_ttl() -> Duration {
        Duration::from_secs(5 * 60)
    }

    pub const fn default_cleanup_interval() -> Duration {
        Duration::from_secs(60)
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
    pub fn new(cache: AsyncCache<String, TelemetryValue>) -> Self {
        Self { cache, seen: Arc::new(DashSet::default()) }
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
