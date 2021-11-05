use ::anyhow::Result;
use ::cast_trait_object::DynCastExt;
use ::chrono::{DateTime, TimeZone, Utc};
use ::serde::{Deserialize, Serialize};
use ::std::path::PathBuf;
use claim::*;
use pretty_assertions::assert_eq;
use proctor::elements::Telemetry;
use proctor::graph::{stage, Connect, Graph, SinkShape};
use proctor::phases::collection::{make_telemetry_cvs_source, SourceSetting};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Data {
    #[serde(
        rename = "task.last_failure",
        default,
        serialize_with = "proctor::serde::date::serialize_optional_datetime_map",
        deserialize_with = "proctor::serde::date::deserialize_optional_datetime"
    )]
    pub last_failure: Option<DateTime<Utc>>,
    #[serde(rename = "cluster.is_deploying")]
    pub is_deploying: bool,
    #[serde(rename = "cluster.last_deployment", with = "proctor::serde")]
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

// impl Into<Telemetry> for Data {
//     fn into(self) -> Telemetry {
//         let mut telemetry = Telemetry::default();
//         telemetry.insert("task.last_failure".to_string(), self.last_failure.into());
//         telemetry.insert("cluster.is_deploying".to_string(), self.is_deploying.into());
//         telemetry.insert("cluster.latest_deployment".to_string(), self.latest_deployment.into());
//         telemetry
//     }
// }
//
// impl TryFrom<Telemetry> for Data {
//     type Error = TelemetryError;
//
//     fn try_from(telemetry: Telemetry) -> Result<Self, Self::Error> {
//         let last_failure = telemetry
//             .get("task.last_failure")
//             .and_then(|val| {
//                 // Option::<DateTime<Utc>>::try_from(val.clone())
//                 match val {
//                     TelemetryValue::Unit => None,
//                     value => Some(DateTime::<Utc>::try_from(value.clone())),
//                 }
//             })
//             .transpose()?;
//
//         let is_deploying = telemetry
//             .get("cluster.is_deploying")
//             .map(|val| bool::try_from(val.clone()))
//             .transpose()?
//             .unwrap_or(false);
//
//         let latest_deployment = telemetry
//             .get("cluster.latest_deployment")
//             .map(|val| DateTime::<Utc>::try_from(val.clone()))
//             .transpose()?
//             .unwrap_or(*DEFAULT_LAST_DEPLOYMENT);
//
//         Ok(Self { last_failure, is_deploying, latest_deployment })
//     }
// }

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_make_telemetry_cvs_source() -> Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_complex_multi_stage_merge_5");
    let _main_span_guard = main_span.enter();

    let base_path = std::env::current_dir()?;
    let path = base_path.join(PathBuf::from("./tests/data/eligibility.csv"));
    let setting = SourceSetting::Csv { path };

    let mut source = assert_ok!(make_telemetry_cvs_source::<Data, _>("local", &setting));

    let mut sink = stage::Fold::<_, Telemetry, (Data, bool)>::new(
        "sink",
        (Data::default(), true),
        |(acc, mut is_first), rec: Telemetry| {
            // let latest = Data::try_from(rec).expect("failed to read Data from Telemetry");
            let latest: Data = assert_ok!(Telemetry::try_into(rec));
            // let dt_format = "%+";

            let rec_last_failure = latest.last_failure;
            // let rec_last_failure = rec.get("task.last_failure").and_then(|r| {
            //
            //     let rep = Option::<DateTime<Utc>>::try_from(r.clone()).unwrap();
            //     if rep.is_empty() {
            //         None
            //     } else {
            //         let lf = DateTime::parse_from_str(rep.as_str(), dt_format)
            //             .unwrap()
            //             .with_timezone(&Utc);
            //         Some(lf)
            //     }
            // });

            let rec_is_deploying = if is_first {
                tracing::info!("first record - set is_deploying.");
                is_first = false;
                Some(latest.is_deploying)
                // rec.get("cluster.is_deploying").map(|v| bool::try_from(v.clone()).unwrap())
            } else {
                tracing::info!("not first record - skip parsing is_deploying.");
                None
            };

            let rec_latest_deployment = latest.latest_deployment;
            // let rec_latest_deployment = DateTime::parse_from_str(
            //     String::try_from(rec.get("cluster.last_deployment").unwrap().clone())
            //         .unwrap()
            //         .as_str(),
            //     dt_format,
            // )
            // .unwrap()
            // .with_timezone(&Utc);

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

            (Data { last_failure, is_deploying, latest_deployment }, is_first)
        },
    );

    let rx_acc = assert_some!(sink.take_final_rx());

    let source_stage = assert_some!(source.take()).0;
    (source_stage.outlet(), sink.inlet()).connect().await;

    let mut g = Graph::default();
    g.push_back(source_stage.dyn_upcast()).await;
    g.push_back(Box::new(sink)).await;

    assert_ok!(g.run().await);

    let (actual, _) = assert_ok!(rx_acc.await);
    let expected = Data {
        last_failure: Some(DateTime::parse_from_str("2014-11-28T12:45:59.324310806Z", "%+")?.with_timezone(&Utc)),
        is_deploying: true,
        latest_deployment: DateTime::parse_from_str("2021-03-08T23:57:12.918473937Z", "%+")?.with_timezone(&Utc),
    };

    assert_eq!(actual, expected);
    Ok(())
}
