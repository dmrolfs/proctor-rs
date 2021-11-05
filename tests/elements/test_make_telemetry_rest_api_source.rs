use std::collections::HashMap;
use std::convert::TryFrom;
use std::time::Duration;

use anyhow::Result;
use cast_trait_object::DynCastExt;
use chrono::{DateTime, TimeZone, Utc};
use claim::*;
use pretty_assertions::assert_eq;
use proctor::elements::Telemetry;
use proctor::error::CollectionError;
use proctor::graph::stage;
use proctor::graph::stage::tick::TickMsg;
use proctor::graph::{Connect, Graph, SinkShape};
use proctor::phases::collection::{make_telemetry_rest_api_source, HttpQuery, SourceSetting};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpBinResponse {
    pub args: HashMap<String, String>,
    pub headers: HashMap<String, String>,
    pub origin: String,
    pub url: String,
}

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
            latest_deployment: Utc.datetime_from_str("1970-08-30 11:32:09", "%Y-%m-%d %H:%M:%S").unwrap(),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_make_telemetry_rest_api_source() -> Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    // fixtures::init_tracing("test_make_telemetry_rest_api_source");
    let main_span = tracing::info_span!("main");
    let _main_span_guard = main_span.enter();

    let setting = SourceSetting::RestApi(HttpQuery {
        interval: Duration::from_millis(25),
        method: reqwest::Method::GET,
        url: reqwest::Url::parse(
            "https://httpbin.org/get?is_deploying=false&last_deployment=1979-05-27T07%3A32%3A00Z",
        )?,
        headers: vec![
            ("authorization".to_string(), "Basic Zm9vOmJhcg==".to_string()),
            ("host".to_string(), "httpbin.org".to_string()),
        ],
        max_retries: 3,
    });

    let mut source = assert_ok!(make_telemetry_rest_api_source::<HttpBinResponse, _>("httpbin", &setting).await);

    let mut sink = stage::Fold::<_, Telemetry, (Data, usize)>::new(
        "sink",
        (Data::default(), 0),
        |(acc, count), rec: Telemetry| {
            let args: Telemetry = assert_some!(rec.get("args").cloned()).into();
            tracing::info!(record=?rec, ?acc, ?count, "folding latest record into acc...");
            let dt_format = "%+";
            let rec_last_failure = args
                .get("last_failure")
                .map(|r| String::try_from(r.clone()))
                .transpose()
                .unwrap()
                .and_then(|r| {
                    if r.is_empty() {
                        None
                    } else {
                        let lf = DateTime::parse_from_str(r.as_str(), dt_format)
                            .unwrap()
                            .with_timezone(&Utc);
                        Some(lf)
                    }
                });
            tracing::info!(?rec_last_failure, "parsed first record field.");

            tracing::warn!(record=?rec, "DMR: record.is_deploying={:?}", args.get("is_deploying"));
            let is_deploying = assert_ok!(bool::try_from(assert_some!(args.get("is_deploying").cloned())));
            tracing::info!(%is_deploying, "parsed second record field.");

            let rec_latest_deployment = assert_ok!(DateTime::parse_from_str(
                assert_ok!(String::try_from(assert_some!(args.get("last_deployment")).clone())).as_str(),
                dt_format,
            ))
            .with_timezone(&Utc);
            tracing::info!(?rec_latest_deployment, "parsed third record field.");

            let last_failure = match (acc.last_failure, rec_last_failure) {
                (None, None) => None,
                (Some(a), None) => Some(a),
                (None, Some(r)) => Some(r),
                (Some(a), Some(r)) if a < r => Some(r),
                (Some(a), _) => Some(a),
            };

            let latest_deployment = if acc.latest_deployment < rec_latest_deployment {
                rec_latest_deployment
            } else {
                acc.latest_deployment
            };

            (Data { last_failure, is_deploying, latest_deployment }, count + 1)
        },
    );

    let rx_acc = assert_some!(sink.take_final_rx());

    let (source_stage, tx_source_api) = assert_some!(source.take());
    (source_stage.outlet(), sink.inlet()).connect().await;

    let mut g = Graph::default();
    g.push_back(source_stage.dyn_upcast()).await;
    g.push_back(Box::new(sink)).await;

    let stop_handle = tokio::spawn(async move {
        let run_duration = Duration::from_millis(60);
        tracing::info!("tick-stop: waiting {:?} to stop...", run_duration);
        tokio::time::sleep(run_duration).await;

        tracing::info!("tick-stop: stopping tick source...");
        let (stop, rx_stop_ack) = TickMsg::stop();
        tx_source_api
            .unwrap()
            .send(stop)
            .map_err(|err| CollectionError::StageError(err.into()))?;
        rx_stop_ack
            .await
            .map_err(|err| CollectionError::StageError(err.into()))?
            .map_err(|err| CollectionError::StageError(err.into()))?;

        Result::<(), CollectionError>::Ok(())
    });

    assert_ok!(g.run().await);

    assert_ok!(assert_ok!(stop_handle.await));
    tracing::info!("tick-stop: tick source stop acknowledged");

    let (actual, count) = assert_ok!(rx_acc.await);
    let expected = Data {
        last_failure: None,
        is_deploying: false,
        latest_deployment: DateTime::parse_from_str("1979-05-27T07:32:00Z", "%+")?.with_timezone(&Utc),
    };
    assert_eq!(actual, expected);
    assert_eq!(count, 3);

    Ok(())
}
