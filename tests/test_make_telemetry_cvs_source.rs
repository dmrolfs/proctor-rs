mod fixtures;

use anyhow::Result;
use cast_trait_object::DynCastExt;
use chrono::{DateTime, TimeZone, Utc};
use proctor::elements::{FromTelemetry, Telemetry};
use proctor::graph::{stage, Connect, Graph, SinkShape};
use proctor::phases::collection::make_telemetry_cvs_source;
use proctor::settings::SourceSetting;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq)]
struct Data {
    pub last_failure: Option<DateTime<Utc>>,
    pub is_deploying: bool,
    pub latest_deployment: DateTime<Utc>,
}

impl Default for Data {
    fn default() -> Self {
        Self {
            last_failure: None,
            is_deploying: true,
            latest_deployment: Utc
                .datetime_from_str("1970-08-30 11:32:09", "%Y-%m-%d %H:%M:%S")
                .unwrap(),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_make_telemetry_cvs_source() -> Result<()> {
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    // fixtures::init_tracing("test_make_telemetry_cvs_source");
    let main_span = tracing::info_span!("test_complex_multi_stage_merge_5");
    let _main_span_guard = main_span.enter();

    let base_path = std::env::current_dir()?;
    let path = base_path.join(PathBuf::from("tests/data/eligibility.csv"));
    let setting = SourceSetting::Csv { path };

    let source = make_telemetry_cvs_source("local", &setting)?;

    let mut sink = stage::Fold::<_, Telemetry, (Data, bool)>::new(
        "sink",
        (Data::default(), true),
        |(acc, mut is_first), rec: Telemetry| {
            let dt_format = "%+";

            let rec_last_failure = rec.get("task_last_failure").and_then(|r| {
                let rep = String::from_telemetry(r.clone()).unwrap();
                if rep.is_empty() {
                    None
                } else {
                    let lf = DateTime::parse_from_str(rep.as_str(), dt_format)
                        .unwrap()
                        .with_timezone(&Utc);
                    Some(lf)
                }
            });

            let rec_is_deploying = if is_first {
                tracing::info!("first record - set is_deploying.");
                is_first = false;
                rec.get("cluster.is_deploying")
                    .map(|v| bool::from_telemetry(v.clone()).unwrap())
            } else {
                tracing::info!("not first record - skip parsing is_deploying.");
                None
            };

            let rec_latest_deployment = DateTime::parse_from_str(
                String::from_telemetry(rec.get("cluster_last_deployment").unwrap().clone())
                    .unwrap()
                    .as_str(),
                dt_format,
            )
            .unwrap()
            .with_timezone(&Utc);

            let last_failure = match (acc.last_failure, rec_last_failure) {
                (None, None) => None,
                (Some(a), None) => Some(a),
                (None, Some(r)) => Some(r),
                (Some(a), Some(r)) if a < r => Some(r),
                (Some(a), _) => Some(a),
            };

            let is_deploying = rec_is_deploying.unwrap_or(acc.is_deploying);

            let latest_deployment = if acc.latest_deployment < rec_latest_deployment {
                rec_latest_deployment
            } else {
                acc.latest_deployment
            };

            (
                Data {
                    last_failure,
                    is_deploying,
                    latest_deployment,
                },
                is_first,
            )
        },
    );

    let rx_acc = sink.take_final_rx().unwrap();

    (source.outlet(), sink.inlet()).connect().await;

    let mut g = Graph::default();
    g.push_back(source.dyn_upcast()).await;
    g.push_back(Box::new(sink)).await;

    g.run().await?;

    match rx_acc.await {
        Ok((actual, _)) => {
            let expected = Data {
                last_failure: Some(
                    DateTime::parse_from_str("2014-11-28T12:45:59.324310806Z", "%+")?.with_timezone(&Utc),
                ),
                is_deploying: true,
                latest_deployment: DateTime::parse_from_str("2021-03-08T23:57:12.918473937Z", "%+")?
                    .with_timezone(&Utc),
            };

            assert_eq!(actual, expected);
            Ok(())
        }
        Err(err) => {
            tracing::error!(error=?err, "failed to receive final folded result.");
            Err(err.into())
        }
    }
}
