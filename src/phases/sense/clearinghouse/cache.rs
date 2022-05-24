use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashSet;
use serde::de::MapAccess;
use serde::{de, ser::SerializeMap, Deserialize, Deserializer, Serialize, Serializer};
use stretto::CacheError;

use crate::elements::{Telemetry, TelemetryValue};

type Cache = stretto::AsyncCache<String, TelemetryValue>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CacheTtl {
    /// Optional setting for the time to live for each clearinghouse field. The default is 5
    /// minutes. The clearinghouse is cleared during rescaling, so this value should be set to a
    /// duration reflecting a break in connectivity or the job not running.
    #[serde(
        rename = "default_ttl_secs",
        default = "CacheTtl::default_ttl",
        serialize_with = "crate::serde::serialize_duration_secs",
        deserialize_with = "crate::serde::deserialize_duration_secs"
    )]
    pub default_ttl: Duration,

    /// Optional setting to override specific clearinghouse fields' time to live. This can be used
    /// to set a different time to live or perhaps make a field never expire.
    #[serde(
        rename = "ttl_overrides",
        skip_serializing_if = "HashMap::is_empty",
        serialize_with = "TelemetryCacheSettings::serialize_ttl_overrides",
        deserialize_with = "TelemetryCacheSettings::deserialize_ttl_overrides"
    )]
    pub ttl_overrides: HashMap<String, Duration>,

    /// Optional setting to specify certain clearinghouse fields never expire.
    #[serde(skip_serializing_if = "HashSet::is_empty", default)]
    pub never_expire: HashSet<String>,
}

impl Default for CacheTtl {
    fn default() -> Self {
        Self {
            default_ttl: Self::default_ttl(),
            ttl_overrides: HashMap::default(),
            never_expire: HashSet::default(),
        }
    }
}

impl CacheTtl {
    pub const fn default_ttl() -> Duration {
        Duration::from_secs(5 * 60)
    }

    /// Returns the time to live for a given field considering the corresponding specification.
    pub fn for_field(&self, field: impl AsRef<str>) -> Option<Duration> {
        if self.never_expire.contains(field.as_ref()) {
            tracing::debug!(field=%field.as_ref(), "Clearinghouse field {} never expires", field.as_ref());
            None
        } else {
            self.ttl_overrides
                .get(field.as_ref())
                .copied()
                .map(|ttl| {
                    tracing::debug!(
                        field=%field.as_ref(),
                        override_ttl=?ttl,
                        "Clearinghouse field {} ovverride to expire in {} seconds", field.as_ref(), ttl.as_secs()
                    );
                    ttl
                })
                .or(Some(self.default_ttl))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TelemetryCacheSettings {
    pub ttl: CacheTtl,

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
            ttl: CacheTtl::default(),
            nr_counters: 1_000,
            max_cost: 100, // 1e6 as i64,
            incremental_item_cost: 1,
            cleanup_interval: Self::default_cleanup_interval(),
        }
    }
}

impl TelemetryCacheSettings {
    pub const fn default_cleanup_interval() -> Duration {
        Duration::from_secs(5)
    }

    pub fn serialize_ttl_overrides<S>(that: &HashMap<String, Duration>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(that.len()))?;
        for (k, v) in that {
            map.serialize_entry(k, &v.as_secs())?;
        }
        map.end()
    }

    pub fn deserialize_ttl_overrides<'de, D>(deserializer: D) -> Result<HashMap<String, Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct EntryVisitor;

        impl<'de> de::Visitor<'de> for EntryVisitor {
            type Value = HashMap<String, Duration>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a map of string to ttl in seconds")
            }

            fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut map = HashMap::with_capacity(access.size_hint().unwrap_or(0));

                while let Some((key, value)) = access.next_entry()? {
                    map.insert(key, Duration::from_secs(value));
                }

                Ok(map)
            }
        }

        deserializer.deserialize_map(EntryVisitor)
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
    cache: Cache,
    seen: Arc<DashSet<String>>,
    ttl: CacheTtl,
}

impl fmt::Debug for TelemetryCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TelemetryCache").field("seen", &self.seen.len()).finish()
    }
}

impl std::ops::Deref for TelemetryCache {
    type Target = Cache;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.cache
    }
}

#[allow(dead_code)]
impl TelemetryCache {
    pub fn new(settings: &TelemetryCacheSettings) -> Self {
        let cache = Self::make_cache(settings);
        Self {
            cache,
            seen: Arc::new(DashSet::default()),
            ttl: settings.ttl.clone(),
        }
    }

    fn make_cache(cache_settings: &TelemetryCacheSettings) -> Cache {
        Cache::builder(cache_settings.nr_counters, cache_settings.max_cost)
            .set_metrics(true)
            .set_cleanup_duration(cache_settings.cleanup_interval)
            .set_ignore_internal_cost(true)
            .finalize()
            .expect("failed creating clearinghouse cache")
    }

    /// insert honoring cache ttl specification.
    #[tracing::instrument(level = "trace")]
    pub async fn insert(&mut self, key: String, val: TelemetryValue, cost: i64) -> bool {
        let result = match self.ttl.for_field(&key) {
            Some(ttl) => self.cache.insert_with_ttl(key.clone(), val, cost, ttl).await,
            None => self.cache.insert(key.clone(), val, cost).await,
        };

        if result {
            self.seen.insert(key);
        }

        result
    }

    /// Tries to insert honoring cache ttl specification.
    #[tracing::instrument(level = "trace")]
    pub async fn try_insert(&mut self, key: String, val: TelemetryValue, cost: i64) -> Result<bool, CacheError> {
        let result = match self.ttl.for_field(&key) {
            Some(ttl) => self.cache.try_insert_with_ttl(key.clone(), val, cost, ttl).await,
            None => self.cache.try_insert(key.clone(), val, cost).await,
        };

        if let Ok(true) = result {
            self.seen.insert(key);
        }

        result
    }

    #[tracing::instrument(level = "trace")]
    pub async fn insert_with_ttl(&mut self, key: String, val: TelemetryValue, cost: i64, ttl: Duration) -> bool {
        let result = self.cache.insert_with_ttl(key.clone(), val, cost, ttl).await;

        if result {
            self.seen.insert(key);
        }

        result
    }

    #[tracing::instrument(level = "trace")]
    pub async fn try_insert_with_ttl(
        &mut self, key: String, val: TelemetryValue, cost: i64, ttl: Duration,
    ) -> Result<bool, CacheError> {
        let result = self.cache.try_insert_with_ttl(key.clone(), val, cost, ttl).await;

        if let Ok(true) = result {
            self.seen.insert(key);
        }

        result
    }

    #[tracing::instrument(level = "trace")]
    pub async fn insert_if_present(&mut self, key: String, val: TelemetryValue, cost: i64) -> bool {
        let result = self.cache.insert_if_present(key.clone(), val, cost).await;

        if result {
            self.seen.insert(key);
        }

        result
    }

    #[tracing::instrument(level = "trace")]
    pub async fn try_insert_if_present(
        &mut self, key: String, val: TelemetryValue, cost: i64,
    ) -> Result<bool, CacheError> {
        let result = self.cache.try_insert_if_present(key.clone(), val, cost).await;

        if let Ok(true) = result {
            self.seen.insert(key);
        }

        result
    }

    #[tracing::instrument(level = "trace")]
    pub async fn wait(&self) -> Result<(), CacheError> {
        let result = self.cache.wait().await;
        tracing::debug!(
            wait=?result,
            cost_added=?self.cache.metrics.get_cost_added(),
            cost_evicted=?self.cache.metrics.get_cost_evicted(),
            "clearinghouse telemetry cache metrics after wait operation"
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
