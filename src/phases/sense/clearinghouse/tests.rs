use std::time::Duration;

use crate::elements::TelemetryValue;

mod cache {
    use claim::*;
    use pretty_assertions::assert_eq;
    use serde_test::{assert_tokens, Token};
    use stretto::AsyncCache;
    use tokio_test::block_on;
    use trim_margin::MarginTrimmable;

    use super::*;
    use crate::phases::sense::clearinghouse::cache::CacheTtl;
    use crate::phases::sense::clearinghouse::*;

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
                ttl: CacheTtl { default_ttl: ttl, ..CacheTtl::default() },
                nr_counters: 1_000,
                max_cost: 100,
                incremental_item_cost: 1,
                cleanup_interval: Duration::from_millis(500),
            };

            let mut cache = TelemetryCache::new(&settings);
            assert!(cache.insert("foo".into(), 17.into(), 1).await);
            assert!(cache.insert("bar".into(), "otis".into(), 1).await);
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
    fn test_default_cache_settings_serde_tokens() {
        let settings = TelemetryCacheSettings::default();
        assert_tokens(
            &settings,
            &vec![
                Token::Struct { name: "TelemetryCacheSettings", len: 5 },
                Token::Str("ttl"),
                Token::Struct { name: "CacheTtl", len: 1 },
                Token::Str("default_ttl_secs"),
                Token::U64(300),
                Token::StructEnd,
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

    #[test]
    fn test_basic_cache_settings_serde_tokens() {
        let settings = TelemetryCacheSettings {
            ttl: CacheTtl {
                never_expire: maplit::hashset! { "foo".to_string(), },
                ttl_overrides: maplit::hashmap! {
                    "zed".to_string() => Duration::from_secs(17),
                },
                ..CacheTtl::default()
            },
            ..TelemetryCacheSettings::default()
        };

        assert_tokens(
            &settings,
            &vec![
                Token::Struct { name: "TelemetryCacheSettings", len: 5 },
                Token::Str("ttl"),
                Token::Struct { name: "CacheTtl", len: 3 },
                Token::Str("default_ttl_secs"),
                Token::U64(300),
                Token::Str("ttl_overrides"),
                Token::Map { len: Some(1) },
                Token::Str("zed"),
                Token::U64(17),
                Token::MapEnd,
                Token::Str("never_expire"),
                Token::Seq { len: Some(1) },
                Token::Str("foo"),
                Token::SeqEnd,
                Token::StructEnd,
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

    #[test]
    fn test_basic_cache_settings_serde_yaml() {
        let settings = TelemetryCacheSettings {
            ttl: CacheTtl {
                never_expire: maplit::hashset! { "foo".to_string(), },
                ttl_overrides: maplit::hashmap! {
                    "zed".to_string() => Duration::from_secs(17),
                },
                ..CacheTtl::default()
            },
            ..TelemetryCacheSettings::default()
        };

        let actual = assert_ok!(serde_yaml::to_string(&settings));
        let expected = r##"
        |---
        |ttl:
        |  default_ttl_secs: 300
        |  ttl_overrides:
        |    zed: 17
        |  never_expire:
        |    - foo
        |nr_counters: 1000
        |max_cost: 100
        |incremental_item_cost: 1
        |cleanup_interval_millis: 5000
        |"##
        .trim_margin_with("|")
        .unwrap();
        assert_eq!(actual, expected);
    }
}
