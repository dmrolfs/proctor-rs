use crate::elements::telemetry;
use crate::ProctorContext;
use oso::PolarClass;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt::Debug;

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlinkDecisionContext {
    #[polar(attribute)]
    pub all_sinks_healthy: bool,

    #[polar(attribute)]
    #[serde(flatten)]
    pub custom: telemetry::Table,
}

impl ProctorContext for FlinkDecisionContext {
    fn required_context_fields() -> HashSet<&'static str> {
        maplit::hashset! {
            "all_sinks_healthy",
        }
    }

    fn custom(&self) -> telemetry::Table {
        self.custom.clone()
    }
}


// /////////////////////////////////////////////////////
// // Unit Tests ///////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::elements::telemetry::ToTelemetry;
    use crate::elements::Telemetry;
    use chrono::{DateTime, Utc};
    use lazy_static::lazy_static;
    use serde_test::{assert_tokens, Token};


    #[test]
    fn test_serde_flink_decision_context() {
        let context = FlinkDecisionContext {
            all_sinks_healthy: true,
            custom: maplit::hashmap! {
                "custom_foo".to_string() => "fred flintstone".into(),
                "custom_bar".to_string() => "The Happy Barber".into(),
            },
        };

        let mut expected = vec![
            Token::Map { len: None },
            Token::Str("all_sinks_healthy"),
            Token::Bool(true),
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
            expected.swap(3, 5);
            expected.swap(4, 6);
            assert_tokens(&context, expected.as_slice());
        }
    }

    #[test]
    fn test_serde_flink_decision_context_from_telemetry() {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);

        let data: Telemetry = maplit::hashmap! {
            "all_sinks_healthy" => false.to_telemetry(),
            "foo" => "bar".to_telemetry(),
        }
            .into_iter()
            .collect();

        tracing::info!(telemetry=?data, "created telemetry");

        let actual = data.try_into::<FlinkDecisionContext>();
        tracing::info!(?actual, "converted into FlinkDecisionContext");
        let expected = FlinkDecisionContext {
            all_sinks_healthy: false,
            custom: maplit::hashmap! {"foo".to_string() => "bar".into(),},
        };
        tracing::info!("actual: {:?}", actual);
        tracing::info!("expected: {:?}", expected);
        assert_eq!(actual.unwrap(), expected);
    }
}
