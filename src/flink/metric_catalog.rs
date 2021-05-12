use crate::elements::{telemetry, TelemetryValue};
use crate::graph::GraphResult;
use oso::PolarClass;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::ops::Add;

#[derive(PolarClass, Debug, Default, PartialEq, Clone, Serialize, Deserialize)]
pub struct MetricCatalog {
    #[polar(attribute)]
    #[serde(flatten)]
    pub flow: FlowMetrics,

    #[polar(attribute)]
    #[serde(flatten)]
    pub utilization: UtilizationMetrics,

    #[polar(attribute)]
    #[serde(flatten)]
    pub custom: telemetry::Table,
}

#[derive(PolarClass, Debug, Default, PartialEq, Clone, Serialize, Deserialize)]
pub struct FlowMetrics {
    // this will need to be in context:  historical_input_messages_per_sec: VecDeque<(f32, DateTime<Utc>)>,
    #[polar(attribute)]
    pub input_messages_per_sec: f32,

    #[polar(attribute)]
    pub input_consumer_lag: f32,

    #[polar(attribute)]
    pub records_out_per_sec: f32,

    #[polar(attribute)]
    pub max_message_latency: f32,

    #[polar(attribute)]
    pub net_in_utilization: f32,

    #[polar(attribute)]
    pub net_out_utilization: f32,

    #[polar(attribute)]
    pub sink_health_metrics: f32,

    #[polar(attribute)]
    pub task_nr_records_in_per_sec: f32,

    #[polar(attribute)]
    pub task_nr_records_out_per_sec: f32,
}

#[derive(PolarClass, Debug, Default, PartialEq, Clone, Serialize, Deserialize)]
pub struct UtilizationMetrics {
    #[polar(attribute)]
    pub task_cpu_load: f32,

    #[polar(attribute)]
    pub network_io_utilization: f32,
}

impl MetricCatalog {
    pub fn new(custom: telemetry::Table) -> Self {
        Self {
            flow: FlowMetrics::default(),
            utilization: UtilizationMetrics::default(),
            custom,
        }
    }

    //todo limited usefulness by itself; keys? iter support for custom and for entire catalog?
    pub fn custom<R: std::convert::TryFrom<TelemetryValue>>(&self, metric: &str) -> Option<GraphResult<R>>
    where
        R: std::convert::TryFrom<TelemetryValue>,
        <R as std::convert::TryFrom<TelemetryValue>>::Error: Into<crate::error::GraphError>,
    {
        self.custom
            .get(metric)
            .map(|m| R::try_from(m.clone()).map_err(|err| err.into()))
    }
}

impl Add for MetricCatalog {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let mut lhs = self.custom;
        lhs.extend(rhs.custom);
        MetricCatalog { custom: lhs, ..self }
    }
}

impl Add<&Self> for MetricCatalog {
    type Output = Self;

    fn add(self, rhs: &Self) -> Self::Output {
        let mut lhs = self.custom;
        lhs.extend(rhs.custom.clone());
        Self { custom: lhs, ..self }
    }
}

// /////////////////////////////////////////////////////
// // Unit Tests ///////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::elements::telemetry::ToTelemetry;
    use pretty_assertions::assert_eq;
    use std::convert::TryFrom;

    #[derive(PartialEq, Debug)]
    struct Bar(String);

    impl TryFrom<TelemetryValue> for Bar {
        type Error = crate::error::GraphError;

        fn try_from(value: TelemetryValue) -> Result<Self, Self::Error> {
            match value {
                TelemetryValue::Text(rep) => Ok(Bar(rep)),
                v => Err(crate::error::GraphError::Unexpected(v.into())),
            }
        }
    }
    // impl FromStr for Bar {
    //     type Err = ();
    //     fn from_str(s: &str) -> Result<Self, Self::Err> {
    //         Ok(Bar(s.to_string()))
    //     }
    // }

    #[test]
    fn test_custom_metric() {
        let cdata = maplit::hashmap! {
            "foo".to_string() => "17".to_telemetry(),
            "otis".to_string() => "Otis".to_telemetry(),
            "bar".to_string() => "Neo".to_telemetry(),
        };
        let data = MetricCatalog::new(cdata);
        assert_eq!(data.custom::<i32>("foo").unwrap().unwrap(), 17_i32);
        assert_eq!(data.custom::<f64>("foo").unwrap().unwrap(), 17.0_f64);
        assert_eq!(data.custom::<String>("otis").unwrap().unwrap(), "Otis".to_string());
        assert_eq!(data.custom::<Bar>("bar").unwrap().unwrap(), Bar("Neo".to_string()));
        assert_eq!(data.custom::<String>("bar").unwrap().unwrap(), "Neo".to_string());
        assert!(data.custom::<i32>("zed").is_none());
    }

    #[test]
    fn test_metric_add() {
        let data = MetricCatalog::default();
        let am1 = maplit::hashmap! {
            "foo.1".to_string() => "f-1".to_telemetry(),
            "bar.1".to_string() => "b-1".to_telemetry(),
        };
        let a1 = MetricCatalog::new(am1.clone());
        let d1 = data.clone() + a1.clone();
        assert_eq!(d1.custom, am1);

        let am2 = maplit::hashmap! {
            "foo.2".to_string() => "f-2".to_telemetry(),
            "bar.2".to_string() => "b-2".to_telemetry(),
        };
        let a2 = MetricCatalog::new(am2.clone());
        let d2 = d1.clone() + a2.clone();
        let mut exp2 = am1.clone();
        exp2.extend(am2.clone());
        assert_eq!(d2.custom, exp2);
    }

    // #[test]
    // fn test_metric_to_f32() {
    //     let expected = 3.14159_f32;
    //     let m: Metric<f32> = Metric::new("pi", expected);
    //
    //     let actual: f32 = m.into();
    //     assert_eq!(actual, expected);
    // }
}
