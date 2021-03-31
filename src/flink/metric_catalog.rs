use oso::PolarClass;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Add;
use std::str::FromStr;

#[derive(PolarClass, Debug, Default, PartialEq, Clone, Serialize, Deserialize)]
pub struct MetricCatalog {
    #[polar(attribute)]
    pub flow: FlowMetrics,
    #[polar(attribute)]
    pub utilization: UtilizationMetrics,
    #[polar(attribute)]
    pub custom: HashMap<String, String>,
}

#[derive(PolarClass, Debug, Default, PartialEq, Clone, Serialize, Deserialize)]
pub struct FlowMetrics {
    // this will need to be in context:  historical_input_messages_per_sec: VecDeque<(f32, DateTime<Utc>)>,
    #[polar(attribute)]
    input_messages_per_sec: f32,
    #[polar(attribute)]
    input_consumer_lag: f32,
    #[polar(attribute)]
    records_out_per_sec: f32,
    #[polar(attribute)]
    max_message_latency: f32,
    #[polar(attribute)]
    net_in_utilization: f32,
    #[polar(attribute)]
    net_out_utilization: f32,
    #[polar(attribute)]
    sink_health_metrics: f32,

    #[polar(attribute)]
    pub task_nr_records_in_per_sec: f32,
    #[polar(attribute)]
    pub task_nr_records_out_per_sec: f32,
}

#[derive(PolarClass, Debug, Default, PartialEq, Clone, Serialize, Deserialize)]
pub struct UtilizationMetrics {
    #[polar(attribute)]
    task_cpu_load: f32,
    #[polar(attribute)]
    network_io_utilization: f32,
}

impl MetricCatalog {
    pub fn new(custom: HashMap<String, String>) -> Self {
        Self {
            flow: FlowMetrics::default(),
            utilization: UtilizationMetrics::default(),
            custom,
        }
    }

    //todo limited usefulness by itself; keys? iter support for custom and for entire catalog?
    pub fn custom<M: AsRef<str>, R: FromStr>(&self, metric: M) -> Option<Result<R, R::Err>> {
        self.custom.get(metric.as_ref()).map(|m| R::from_str(m.as_str()))
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
    use crate::elements::Metric;

    #[derive(PartialEq, Debug)]
    struct Bar(String);

    impl FromStr for Bar {
        type Err = ();
        fn from_str(s: &str) -> Result<Self, Self::Err> {
            Ok(Bar(s.to_string()))
        }
    }

    #[test]
    fn test_custom_metric() {
        let cdata = maplit::hashmap! {
            "foo".to_string() => "17".to_string(),
            "otis".to_string() => "Otis".to_string(),
            "bar".to_string() => "Neo".to_string(),
        };
        let data = MetricCatalog::new(cdata);
        assert_eq!(data.custom("foo"), Some(Ok(17_i32)));
        assert_eq!(data.custom("foo"), Some(Ok(17.0_f32)));
        assert_eq!(data.custom("otis"), Some(Ok("Otis".to_string())));
        assert_eq!(data.custom("bar"), Some(Ok(Bar("Neo".to_string()))));
        assert_eq!(data.custom("bar"), Some(Ok("Neo".to_string())));
        assert_eq!(data.custom::<_, i32>("zed"), None);
    }

    #[test]
    fn test_metric_add() {
        let data = MetricCatalog::default();
        let am1 = maplit::hashmap! {
            "foo.1".to_string() => "f-1".to_string(),
            "bar.1".to_string() => "b-1".to_string(),
        };
        let a1 = MetricCatalog::new(am1.clone());
        let d1 = data.clone() + a1.clone();
        assert_eq!(d1.custom, am1);

        let am2 = maplit::hashmap! {
            "foo.2".to_string() => "f-2".to_string(),
            "bar.2".to_string() => "b-2".to_string(),
        };
        let a2 = MetricCatalog::new(am2.clone());
        let d2 = d1.clone() + a2.clone();
        let mut exp2 = am1.clone();
        exp2.extend(am2.clone());
        assert_eq!(d2.custom, exp2);
    }

    #[test]
    fn test_metric_to_f32() {
        let expected = 3.14159_f32;
        let m: Metric<f32> = Metric::new("pi", expected);

        let actual: f32 = m.into();
        assert_eq!(actual, expected);
    }
}
