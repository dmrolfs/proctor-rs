use std::collections::HashSet;
use std::fmt::Debug;

use chrono::{DateTime, Utc};
use oso::PolarClass;
use serde::{Deserialize, Serialize};

use crate::elements::telemetry;
use crate::ProctorContext;

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlinkEligibilityContext {
    #[polar(attribute)]
    #[serde(flatten)]
    pub task_status: TaskStatus,
    #[polar(attribute)]
    #[serde(flatten)]
    pub cluster_status: ClusterStatus,

    #[polar(attribute)]
    #[serde(flatten)]
    pub custom: telemetry::Table,
}

impl ProctorContext for FlinkEligibilityContext {
    fn required_context_fields() -> HashSet<&'static str> {
        maplit::hashset! {
            "task.last_failure",
            "cluster.is_deploying",
            "cluster.last_deployment",
        }
    }

    fn custom(&self) -> telemetry::Table { self.custom.clone() }
}

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskStatus {
    #[serde(default)]
    #[serde(
        rename = "task.last_failure",
        serialize_with = "crate::serde::serialize_optional_datetime",
        deserialize_with = "crate::serde::deserialize_optional_datetime"
    )]
    pub last_failure: Option<DateTime<Utc>>,
}

impl TaskStatus {
    pub fn last_failure_within_seconds(&self, seconds: i64) -> bool {
        self.last_failure.map_or(false, |last_failure| {
            let boundary = Utc::now() - chrono::Duration::seconds(seconds);
            boundary < last_failure
        })
    }
}

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterStatus {
    #[polar(attribute)]
    #[serde(rename = "cluster.is_deploying")]
    pub is_deploying: bool,
    #[serde(with = "crate::serde", rename = "cluster.last_deployment")]
    pub last_deployment: DateTime<Utc>,
}

impl ClusterStatus {
    pub fn last_deployment_within_seconds(&self, seconds: i64) -> bool {
        let boundary = Utc::now() - chrono::Duration::seconds(seconds);
        boundary < self.last_deployment
    }
}

// /////////////////////////////////////////////////////
// // Unit Tests ///////////////////////////////////////

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use lazy_static::lazy_static;
    use serde_test::{assert_tokens, Token};

    use super::*;
    use crate::elements::telemetry::ToTelemetry;
    use crate::elements::Telemetry;

    lazy_static! {
        static ref DT_1: DateTime<Utc> = Utc::now();
        static ref DT_1_STR: String = format!("{}", DT_1.format("%+"));
        static ref DT_2: DateTime<Utc> = Utc::now() - chrono::Duration::hours(5);
        static ref DT_2_STR: String = format!("{}", DT_2.format("%+"));
    }

    #[test]
    fn test_serde_flink_eligibility_context() {
        let context = FlinkEligibilityContext {
            task_status: TaskStatus { last_failure: Some(DT_1.clone()) },
            cluster_status: ClusterStatus { is_deploying: false, last_deployment: DT_2.clone() },
            custom: maplit::hashmap! {
                "custom_foo".to_string() => "fred flintstone".into(),
                "custom_bar".to_string() => "The Happy Barber".into(),
            },
        };

        let mut expected = vec![
            Token::Map { len: None },
            Token::Str("task.last_failure"),
            Token::Some,
            Token::Str(&DT_1_STR),
            Token::Str("cluster.is_deploying"),
            Token::Bool(false),
            Token::Str("cluster.last_deployment"),
            Token::Str(&DT_2_STR),
            Token::Str("custom_foo"),
            Token::Str("fred flintstone"),
            Token::Str("custom_bar"),
            Token::Str("The Happy Barber"),
            Token::MapEnd,
        ];

        let result = std::panic::catch_unwind(|| {
            assert_tokens(&context, expected.as_slice());
        });

        if result.is_err() {
            expected.swap(8, 10);
            expected.swap(9, 11);
            assert_tokens(&context, expected.as_slice());
        }
    }

    #[test]
    fn test_serde_flink_eligibility_context_from_telemetry() {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);

        let data: Telemetry = maplit::hashmap! {
            "task.last_failure" => DT_1_STR.as_str().to_telemetry(),
            "cluster.is_deploying" => false.to_telemetry(),
            "cluster.last_deployment" => DT_2_STR.as_str().to_telemetry(),
            "foo" => "bar".to_telemetry(),
        }
        .into_iter()
        .collect();

        tracing::info!(telemetry=?data, "created telemetry");

        let actual = data.try_into::<FlinkEligibilityContext>();
        tracing::info!(?actual, "converted into FlinkEligibilityContext");
        let expected = FlinkEligibilityContext {
            task_status: TaskStatus { last_failure: Some(DT_1.clone()) },
            cluster_status: ClusterStatus { is_deploying: false, last_deployment: DT_2.clone() },
            custom: maplit::hashmap! {"foo".to_string() => "bar".into(),},
        };
        tracing::info!("actual: {:?}", actual);
        tracing::info!("expected: {:?}", expected);
        assert_eq!(actual.unwrap(), expected);
    }
}
