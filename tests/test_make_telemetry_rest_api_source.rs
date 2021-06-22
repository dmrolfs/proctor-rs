mod fixtures;

use std::collections::HashMap;
use std::convert::TryFrom;
use std::time::Duration;

use anyhow::Result;
use cast_trait_object::DynCastExt;
use chrono::{DateTime, TimeZone, Utc};
use proctor::elements::Telemetry;
// use proctor::elements::telemetry::ToTelemetry;
use proctor::graph::stage::{self, tick};
use proctor::graph::{Connect, Graph, SinkShape};
use proctor::phases::collection::make_telemetry_rest_api_source;
use proctor::settings::{HttpQuery, SourceSetting};
use serde::Deserialize;
use tokio::sync::oneshot;

#[derive(Debug, Clone, Deserialize)]
pub struct HttpBinResponse {
    pub args: HashMap<String, String>,
    pub headers: HashMap<String, String>,
    pub origin: String,
    pub url: String,
}

impl Into<Telemetry> for HttpBinResponse {
    fn into(self) -> Telemetry {
        let mut data = Telemetry::default();
        if let Some(last_failure) = self.args.get("last_failure") {
            let _ = data.insert("last_failure".to_string(), last_failure.clone().into());
        }

        data.insert(
            "is_deploying".to_string(),
            self.args
                .get("is_deploying")
                .map(|rep| rep.parse::<bool>().unwrap_or(false))
                .unwrap_or(false)
                .into(),
        );

        data.insert(
            "last_deployment".to_string(),
            self.args
                .get("last_deployment")
                .unwrap_or(&"1970-08-30 11:32:09".to_string())
                .clone()
                .into(),
        );

        data
    }
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
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    // fixtures::init_tracing("test_make_telemetry_rest_api_source");
    let main_span = tracing::info_span!("main");
    let _main_span_guard = main_span.enter();

    let setting = SourceSetting::RestApi(HttpQuery {
        interval: Duration::from_millis(25),
        method: reqwest::Method::GET,
        url: reqwest::Url::parse(
            "https://httpbin.org/get?is_redeploying=false&last_deployment=1979-05-27T07%3A32%3A00Z",
        )?,
        headers: vec![
            ("authorization".to_string(), "Basic Zm9vOmJhcg==".to_string()),
            ("host".to_string(), "httpbin.org".to_string()),
        ],
    });

    let (source, tx_tick) = make_telemetry_rest_api_source::<HttpBinResponse, _>("httpbin", &setting).await?;

    let mut sink = stage::Fold::<_, Telemetry, (Data, usize)>::new(
        "sink",
        (Data::default(), 0),
        |(acc, count), rec: Telemetry| {
            tracing::info!(record=?rec, ?acc, ?count, "folding latest record into acc...");
            let dt_format = "%+";
            let rec_last_failure = rec
                .get("last_failure")
                .map(|r| String::try_from(r.clone()).unwrap())
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

            let is_deploying = bool::try_from(rec.get("is_deploying").unwrap().clone()).unwrap();
            tracing::info!(%is_deploying, "parsed second record field.");

            let rec_latest_deployment = DateTime::parse_from_str(
                String::try_from(rec.get("last_deployment").unwrap().clone())
                    .unwrap()
                    .as_str(),
                dt_format,
            )
            .unwrap()
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

    let rx_acc = sink.take_final_rx().unwrap();

    (source.outlet(), sink.inlet()).connect().await;

    let mut g = Graph::default();
    g.push_back(source.dyn_upcast()).await;
    g.push_back(Box::new(sink)).await;

    let (tx_stop, rx_stop) = oneshot::channel();
    let stop_handle = tokio::spawn(async move {
        let run_duration = Duration::from_millis(60);
        tracing::info!("tick-stop: waiting {:?} to stop...", run_duration);
        tokio::time::sleep(run_duration).await;

        tracing::info!("tick-stop: stopping tick source...");
        tx_tick
            .send(tick::TickMsg::Stop { tx: tx_stop })
            .expect("failed to send tick stop cmd.");
    });

    g.run().await?;

    let _ = stop_handle.await?;
    let _ = rx_stop.await??;
    tracing::info!("tick-stop: tick source stop acknowledged");

    // g.complete().await?;

    match rx_acc.await {
        Ok((actual, count)) => {
            let expected = Data {
                last_failure: None,
                is_deploying: false,
                latest_deployment: DateTime::parse_from_str("1979-05-27T07:32:00Z", "%+")?.with_timezone(&Utc),
            };

            assert_eq!(actual, expected);
            assert_eq!(count, 3);

            Ok(())
        },
        Err(err) => {
            tracing::error!(error=?err, "failed to receive final folded result.");
            Err(err.into())
        },
    }
}
