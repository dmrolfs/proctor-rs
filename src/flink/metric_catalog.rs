use std::collections::HashSet;
use std::fmt::Debug;
use std::ops::Add;

use ::serde_with::{serde_as, TimestampMilliSeconds};
use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use oso::PolarClass;
use serde::{Deserialize, Serialize};

use crate::elements::{telemetry, TelemetryValue};
use crate::error::TelemetryError;

// todo: replace this with reflection approach if one identified.
// tried serde-reflection, which failed since serde identifiers (flatten) et.al., are not supported.
// tried Oso PolarClass, but Class::attributes is private.
// could write a rpcedural macro, but want to list effective serde names, such as for flattened.
lazy_static! {
    pub static ref METRIC_CATALOG_REQ_SUBSCRIPTION_FIELDS: HashSet<&'static str> = maplit::hashset! {
    "timestamp",

    // FlowMetrics
    "input_messages_per_sec",
    "input_consumer_lag",
    "records_out_per_sec",
    "max_message_latency",
    "net_in_utilization",
    "net_out_utilization",
    "sink_health_metrics",
    "task_nr_records_in_per_sec",
    "task_nr_records_out_per_sec",

    // UtilizationMetrics
    "task_cpu_load",
    "network_io_utilization",
};
}

#[serde_as]
#[derive(PolarClass, Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct MetricCatalog {
    #[serde_as(as = "TimestampMilliSeconds")]
    pub timestamp: DateTime<Utc>,

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
    // this will need to be in context:  historical_input_messages_per_sec: VecDeque<(f64, DateTime<Utc>)>,
    #[polar(attribute)]
    pub input_messages_per_sec: f64,

    #[polar(attribute)]
    pub input_consumer_lag: f64,

    #[polar(attribute)]
    pub records_out_per_sec: f64,

    #[polar(attribute)]
    pub max_message_latency: f64,

    #[polar(attribute)]
    pub net_in_utilization: f64,

    #[polar(attribute)]
    pub net_out_utilization: f64,

    #[polar(attribute)]
    pub sink_health_metrics: f64,

    #[polar(attribute)]
    pub task_nr_records_in_per_sec: f64,

    #[polar(attribute)]
    pub task_nr_records_out_per_sec: f64,
}

#[derive(PolarClass, Debug, Default, PartialEq, Clone, Serialize, Deserialize)]
pub struct UtilizationMetrics {
    #[polar(attribute)]
    pub task_cpu_load: f64,

    #[polar(attribute)]
    pub network_io_utilization: f64,
}

impl MetricCatalog {
    pub fn new(timestamp: DateTime<Utc>, custom: telemetry::Table) -> Self {
        Self {
            timestamp,
            flow: FlowMetrics::default(),
            utilization: UtilizationMetrics::default(),
            custom,
        }
    }

    // todo limited usefulness by itself; keys? iter support for custom and for entire catalog?
    pub fn custom<T: std::convert::TryFrom<TelemetryValue>>(&self, metric: &str) -> Option<Result<T, TelemetryError>>
    where
        T: std::convert::TryFrom<TelemetryValue>,
        TelemetryError: From<<T as std::convert::TryFrom<TelemetryValue>>::Error>,
    {
        self.custom.get(metric).map(|telemetry| {
            let value = T::try_from(telemetry.clone())?;
            Ok(value)
        })
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
    use std::convert::TryFrom;

    use pretty_assertions::assert_eq;

    use super::*;
    use crate::elements::telemetry::ToTelemetry;
    use crate::error::TypeExpectation;

    #[derive(PartialEq, Debug)]
    struct Bar(String);

    impl TryFrom<TelemetryValue> for Bar {
        type Error = TelemetryError;

        fn try_from(value: TelemetryValue) -> Result<Self, Self::Error> {
            match value {
                TelemetryValue::Text(rep) => Ok(Bar(rep)),
                v => Err(TelemetryError::TypeError {
                    expected: format!("telementry value {}", TypeExpectation::Text),
                    actual: Some(format!("{:?}", v)),
                }),
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
        let data = MetricCatalog::new(Utc::now(), cdata);
        assert_eq!(data.custom::<i64>("foo").unwrap().unwrap(), 17_i64);
        assert_eq!(data.custom::<f64>("foo").unwrap().unwrap(), 17.0_f64);
        assert_eq!(data.custom::<String>("otis").unwrap().unwrap(), "Otis".to_string());
        assert_eq!(data.custom::<Bar>("bar").unwrap().unwrap(), Bar("Neo".to_string()));
        assert_eq!(data.custom::<String>("bar").unwrap().unwrap(), "Neo".to_string());
        assert!(data.custom::<i64>("zed").is_none());
    }

    #[test]
    fn test_metric_add() {
        let ts = Utc::now();
        let data = MetricCatalog::new(ts.clone(), std::collections::HashMap::default());
        let am1 = maplit::hashmap! {
            "foo.1".to_string() => "f-1".to_telemetry(),
            "bar.1".to_string() => "b-1".to_telemetry(),
        };
        let a1 = MetricCatalog::new(ts.clone(), am1.clone());
        let d1 = data.clone() + a1.clone();
        assert_eq!(d1.custom, am1);

        let am2 = maplit::hashmap! {
            "foo.2".to_string() => "f-2".to_telemetry(),
            "bar.2".to_string() => "b-2".to_telemetry(),
        };
        let a2 = MetricCatalog::new(ts.clone(), am2.clone());
        let d2 = d1.clone() + a2.clone();
        let mut exp2 = am1.clone();
        exp2.extend(am2.clone());
        assert_eq!(d2.custom, exp2);
    }

    // #[test]
    // fn test_metric_to_f64() {
    //     let expected = 3.14159_f64;
    //     let m: Metric<f64> = Metric::new("pi", expected);
    //
    //     let actual: f64 = m.into();
    //     assert_eq!(actual, expected);
    // }
}
