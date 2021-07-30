use std::path::PathBuf;

use anyhow::Result;
use cast_trait_object::DynCastExt;
use chrono::{DateTime, TimeZone, Utc};
use lazy_static::lazy_static;
use proctor::elements::make_from_telemetry;
use proctor::graph::{stage, Connect, Graph, SinkShape};
use proctor::phases::collection::make_telemetry_cvs_source;
use proctor::settings::SourceSetting;
use serde::{Deserialize, Serialize};
use serde_test::{assert_tokens, Token};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Data {
    #[serde(
        default,
        rename = "task.last_failure",
        serialize_with = "proctor::serde::serialize_optional_datetime",
        deserialize_with = "proctor::serde::deserialize_optional_datetime"
    )]
    pub last_failure: Option<DateTime<Utc>>,
    #[serde(rename = "cluster.is_deploying")]
    pub is_deploying: bool,
    #[serde(rename = "cluster.last_deployment", with = "proctor::serde")]
    pub last_deployment: DateTime<Utc>,
}

impl Default for Data {
    fn default() -> Self {
        Self {
            last_failure: None,
            is_deploying: true,
            last_deployment: Utc.datetime_from_str("1970-08-30 11:32:09", "%Y-%m-%d %H:%M:%S").unwrap(),
        }
    }
}

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

    let base_path = std::env::current_dir()?;
    let path = base_path.join(PathBuf::from("./tests/data/eligibility.csv"));
    let setting = SourceSetting::Csv { path };

    let source = make_telemetry_cvs_source::<Data, _>("local", &setting)?;
    let convert = make_from_telemetry("convert", true).await;

    let mut sink = stage::Fold::<_, Data, Vec<Data>>::new("sink", Vec::default(), |mut acc, item| {
        acc.push(item);
        acc
    });

    let rx_acc = sink.take_final_rx().unwrap();

    (source.outlet(), convert.inlet()).connect().await;
    (convert.outlet(), sink.inlet()).connect().await;

    let mut g = Graph::default();
    g.push_back(source.dyn_upcast()).await;
    g.push_back(convert.dyn_upcast()).await;
    g.push_back(Box::new(sink)).await;

    g.run().await?;

    let actual = rx_acc.await?;

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
