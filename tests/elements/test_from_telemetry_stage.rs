use std::path::PathBuf;

use anyhow::Result;
use cast_trait_object::DynCastExt;
use chrono::{DateTime, Utc};
use claim::*;
use once_cell::sync::Lazy;
use pretty_assertions::assert_eq;
use pretty_snowflake::{Label, MakeLabeling};
use proctor::elements;
use proctor::graph::{stage, Connect, Graph, SinkShape, SourceShape};
use proctor::phases::sense::{make_telemetry_cvs_sensor, SensorSetting};
use proctor::phases::DataSet;
use serde::{Deserialize, Serialize};
use serde_test::{assert_tokens, Token};

use super::DEFAULT_LAST_DEPLOYMENT;

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

impl Label for Data {
    type Labeler = MakeLabeling<Self>;

    fn labeler() -> Self::Labeler {
        MakeLabeling::default()
    }
}

static NOW: Lazy<DateTime<Utc>> = Lazy::new(|| Utc::now());
static NOW_REP: Lazy<String> = Lazy::new(|| format!("{}", NOW.format("%+")));

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
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    // fixtures::init_tracing("test_make_from_telemetry_stage");
    let main_span = tracing::info_span!("test_make_from_telemetry_stage");
    let _main_span_guard = main_span.enter();

    let base_path = assert_ok!(std::env::current_dir());
    let path = base_path.join(PathBuf::from("./tests/data/eligibility.csv"));
    let setting = SensorSetting::Csv { path };

    let mut source = assert_ok!(make_telemetry_cvs_sensor::<Data, _>("local", &setting));
    let add_metadata = stage::AndThen::new("add_metadata", |telemetry| DataSet::new(telemetry));
    let convert = elements::make_from_telemetry("convert".into(), true).await;

    let mut sink = stage::Fold::<_, DataSet<Data>, Vec<Data>>::new("sink", Vec::default(), |mut acc, item| {
        acc.push(item.into_inner());
        acc
    });

    let rx_acc = assert_some!(sink.take_final_rx());

    let source_stage = assert_some!(source.stage.take());
    (source_stage.outlet(), add_metadata.inlet()).connect().await;
    (add_metadata.outlet(), convert.inlet()).connect().await;
    (convert.outlet(), sink.inlet()).connect().await;

    let mut g = Graph::default();
    g.push_back(source_stage.dyn_upcast()).await;
    g.push_back(Box::new(add_metadata)).await;
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
