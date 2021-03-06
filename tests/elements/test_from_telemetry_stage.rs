use std::path::PathBuf;

use super::DEFAULT_LAST_DEPLOYMENT;
use anyhow::Result;
use cast_trait_object::DynCastExt;
use chrono::{DateTime, Utc};
use claim::*;
use lazy_static::lazy_static;
use pretty_assertions::assert_eq;
use proctor::elements::make_from_telemetry;
use proctor::graph::{stage, Connect, Graph, SinkShape};
use proctor::phases::collection::{make_telemetry_cvs_source, SourceSetting};
use serde::{Deserialize, Serialize};
use serde_test::{assert_tokens, Token};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Data {
    #[serde(
        default,
        rename = "task.last_failure",
        serialize_with = "proctor::serde::date::serialize_optional_datetime_format",
        deserialize_with = "proctor::serde::date::deserialize_optional_datetime"
    )]
    pub last_failure: Option<DateTime<Utc>>,

    #[serde(rename = "cluster.is_deploying")]
    pub is_deploying: bool,

    #[serde(
        rename = "cluster.last_deployment",
        serialize_with = "proctor::serde::date::serialize_format",
        deserialize_with = "proctor::serde::date::deserialize"
    )]
    pub last_deployment: DateTime<Utc>,
}

impl Default for Data {
    fn default() -> Self {
        Self {
            last_failure: None,
            is_deploying: true,
            last_deployment: *DEFAULT_LAST_DEPLOYMENT,
        }
    }
}

// impl Into<Telemetry> for Data {
//     fn into(self) -> Telemetry {
//         let mut telemetry = Telemetry::default();
//         let t_last_failure = self.last_failure.into();
//         let t_is_deploying = self.is_deploying.into();
//         let t_last_deployment = self.last_deployment.into();
//         tracing::warn!(
//             ?t_last_failure,
//             ?t_is_deploying,
//             ?t_last_deployment,
//             "DMR: converting data to telemetry."
//         );
//         telemetry.insert("task.last_failure".to_string(), t_last_failure);
//         telemetry.insert("cluster.is_deploying".to_string(), t_is_deploying);
//         telemetry.insert("cluster.last_deployment".to_string(), t_last_deployment);
//         telemetry
//     }
// }
//
// impl TryFrom<Telemetry> for Data {
//     type Error = TelemetryError;
//
//     fn try_from(telemetry: Telemetry) -> Result<Self, Self::Error> {
//         let last_failure: Option<DateTime<Utc>> = telemetry
//             .get("task.last_failure")
//             .map(|val| DateTime::<Utc>::try_from(val.clone()))
//             .transpose()?;
//
//         let is_deploying = telemetry
//             .get("cluster.is_deploying")
//             .map(|val| bool::try_from(val.clone()))
//             .transpose()?
//             .unwrap_or(false);
//
//         let last_deployment = telemetry
//             .get("cluster.last_deployment")
//             .map(|val| DateTime::<Utc>::try_from(val.clone()))
//             .transpose()?
//             .unwrap_or(*DEFAULT_LAST_DEPLOYMENT);
//
//         tracing::warn!(
//             ?last_failure,
//             ?is_deploying,
//             ?last_deployment,
//             "DMR: converting from telemetry into Data."
//         );
//         Ok(Self { last_failure, is_deploying, last_deployment })
//     }
// }

lazy_static! {
    static ref NOW: DateTime<Utc> = Utc::now();
    static ref NOW_REP: String = format!("{}", NOW.format("%+"));
}

#[test]
fn test_data_serde() {
    let data = Data {
        last_failure: Some(NOW.clone()),
        is_deploying: true,
        last_deployment: NOW.clone(),
    };

    assert_tokens(
        &data,
        &vec![
            Token::Struct { name: "Data", len: 3 },
            Token::Str("task.last_failure"),
            Token::Some,
            Token::Str(&NOW_REP),
            Token::Str("cluster.is_deploying"),
            Token::Bool(true),
            Token::Str("cluster.last_deployment"),
            Token::Str(&NOW_REP),
            Token::StructEnd,
        ],
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_make_from_telemetry_stage() -> Result<()> {
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    // fixtures::init_tracing("test_make_from_telemetry_stage");
    let main_span = tracing::info_span!("test_make_from_telemetry_stage");
    let _main_span_guard = main_span.enter();

    let base_path = assert_ok!(std::env::current_dir());
    let path = base_path.join(PathBuf::from("./tests/data/eligibility.csv"));
    let setting = SourceSetting::Csv { path };

    let mut source = assert_ok!(make_telemetry_cvs_source::<Data, _>("local", &setting));
    let convert = make_from_telemetry("convert", true).await;

    let mut sink = stage::Fold::<_, Data, Vec<Data>>::new("sink", Vec::default(), |mut acc, item| {
        acc.push(item);
        acc
    });

    let rx_acc = assert_some!(sink.take_final_rx());

    let source_stage = assert_some!(source.take()).0;
    (source_stage.outlet(), convert.inlet()).connect().await;
    (convert.outlet(), sink.inlet()).connect().await;

    let mut g = Graph::default();
    g.push_back(source_stage.dyn_upcast()).await;
    g.push_back(convert.dyn_upcast()).await;
    g.push_back(Box::new(sink)).await;

    assert_ok!(g.run().await);

    let actual = assert_ok!(rx_acc.await);

    let expected = vec![
        Data {
            last_failure: None,
            is_deploying: true,
            last_deployment: DateTime::parse_from_str("2014-11-28T10:11:37.246310806Z", "%+")?.with_timezone(&Utc),
        },
        Data {
            last_failure: Some(DateTime::parse_from_str("2014-11-28T12:45:59.324310806Z", "%+")?.with_timezone(&Utc)),
            is_deploying: false,
            last_deployment: DateTime::parse_from_str("2021-03-08T23:57:12.918473937Z", "%+")?.with_timezone(&Utc),
        },
    ];

    assert_eq!(actual, expected);
    Ok(())
}
