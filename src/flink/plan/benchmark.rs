use std::cmp::Ordering;
use std::convert::TryFrom;

use ::serde_with::serde_as;
use serde::{Deserialize, Serialize};

use crate::elements::{TelemetryValue, ToTelemetry};
use crate::error::{PlanError, TelemetryError, TypeExpectation};
use crate::flink::plan::RecordsPerSecond;
use crate::flink::MetricCatalog;
use approx::{AbsDiffEq, RelativeEq};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkRange {
    pub nr_task_managers: u8,
    pub lo_rate: Option<RecordsPerSecond>,
    pub hi_rate: Option<RecordsPerSecond>,
}

impl BenchmarkRange {
    pub fn lo_from(b: Benchmark) -> Self {
        Self::new(b.nr_task_managers, Some(b.records_out_per_sec), None)
    }

    pub fn hi_from(b: Benchmark) -> Self {
        Self::new(b.nr_task_managers, None, Some(b.records_out_per_sec))
    }

    pub fn new(nr_task_managers: u8, lo_rate: Option<RecordsPerSecond>, hi_rate: Option<RecordsPerSecond>) -> Self {
        Self { nr_task_managers, lo_rate, hi_rate }
    }
}

impl BenchmarkRange {
    pub fn hi_mark(&self) -> Option<Benchmark> {
        self.hi_rate.map(|records_out_per_sec| Benchmark {
            nr_task_managers: self.nr_task_managers,
            records_out_per_sec,
        })
    }

    pub fn lo_mark(&self) -> Option<Benchmark> {
        self.lo_rate.map(|records_out_per_sec| Benchmark {
            nr_task_managers: self.nr_task_managers,
            records_out_per_sec,
        })
    }
}

impl AbsDiffEq for BenchmarkRange {
    type Epsilon = <Benchmark as AbsDiffEq>::Epsilon;

    #[inline]
    fn default_epsilon() -> Self::Epsilon { <Benchmark as AbsDiffEq>::default_epsilon() }

    fn abs_diff_eq(&self, other: &Self, epsilon: Self::Epsilon) -> bool {
        // couldn't use nested fn due to desire to use generic outer variable, epsilon.
        let do_abs_diff_eq = |lhs: Option<&RecordsPerSecond>, rhs: Option<&RecordsPerSecond>| {
            match (lhs, rhs) {
                (None, None) => true,
                (Some(lhs), Some(rhs)) => lhs.abs_diff_eq(rhs, epsilon),
                _ => false,
            }
        };

        (self.nr_task_managers == other.nr_task_managers)
            && do_abs_diff_eq(self.lo_rate.as_ref(), other.lo_rate.as_ref())
            && do_abs_diff_eq(self.hi_rate.as_ref(), other.hi_rate.as_ref())
    }
}

impl RelativeEq for BenchmarkRange {
    #[inline]
    fn default_max_relative() -> Self::Epsilon { <Benchmark as RelativeEq>::default_max_relative() }

    fn relative_eq(&self, other: &Self, epsilon: Self::Epsilon, max_relative: Self::Epsilon) -> bool {
        let do_relative_eq = |lhs: Option<&RecordsPerSecond>, rhs: Option<&RecordsPerSecond>| {
            match (lhs, rhs) {
                (None, None) => true,
                (Some(lhs), Some(rhs)) => lhs.relative_eq(rhs, epsilon, max_relative),
                _ => false,
            }
        };

        (self.nr_task_managers == other.nr_task_managers)
        && do_relative_eq(self.lo_rate.as_ref(), other.lo_rate.as_ref())
        && do_relative_eq(self.hi_rate.as_ref(), other.hi_rate.as_ref())
    }
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Benchmark {
    pub nr_task_managers: u8,

    pub records_out_per_sec: RecordsPerSecond,
    /* #[serde_as(as = "TimestampMilliSeconds")]
     * pub timestamp: DateTime<Utc>, */
}

impl Benchmark {
    pub fn new(nr_task_managers: u8, records_out_per_sec: RecordsPerSecond) -> Self {
        Self { nr_task_managers, records_out_per_sec, }
    }
}

impl From<MetricCatalog> for Benchmark {
    fn from(that: MetricCatalog) -> Self {
        Self::from(&that)
    }
}

impl From<&MetricCatalog> for Benchmark {
    fn from(that: &MetricCatalog) -> Self {
        Self {
            nr_task_managers: that.cluster.nr_task_managers,
            records_out_per_sec: RecordsPerSecond(that.flow.records_out_per_sec),
        }
    }
}

impl PartialOrd for Benchmark {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.nr_task_managers.cmp(&other.nr_task_managers))
    }
}

impl AbsDiffEq for Benchmark {
    type Epsilon = <RecordsPerSecond as AbsDiffEq>::Epsilon;

    #[inline]
    fn default_epsilon() -> Self::Epsilon { <RecordsPerSecond as AbsDiffEq>::default_epsilon() }

    #[inline]
    fn abs_diff_eq(&self, other: &Self, epsilon: Self::Epsilon) -> bool {
        (self.nr_task_managers == other.nr_task_managers)
            && (self.records_out_per_sec.abs_diff_eq(&other.records_out_per_sec, epsilon))
    }
}

impl RelativeEq for Benchmark {
    #[inline]
    fn default_max_relative() -> Self::Epsilon { <RecordsPerSecond as RelativeEq>::default_max_relative() }

    fn relative_eq(&self, other: &Self, epsilon: Self::Epsilon, max_relative: Self::Epsilon) -> bool {
        (self.nr_task_managers == other.nr_task_managers)
        && (self.records_out_per_sec.relative_eq(&other.records_out_per_sec, epsilon, max_relative))
    }
}

// const T_TIMESTAMP: &'static str = "timestamp";
const T_NR_TASK_MANAGERS: &'static str = "nr_task_managers";
const T_RECORDS_OUT_PER_SEC: &'static str = "records_out_per_sec";

impl From<Benchmark> for TelemetryValue {
    fn from(that: Benchmark) -> Self {
        TelemetryValue::Table(maplit::hashmap! {
            // T_TIMESTAMP.to_string() => that.timestamp.timestamp().to_telemetry(),
            T_NR_TASK_MANAGERS.to_string() => that.nr_task_managers.to_telemetry(),
            T_RECORDS_OUT_PER_SEC.to_string() => that.records_out_per_sec.to_telemetry(),
        })
    }
}

impl TryFrom<TelemetryValue> for Benchmark {
    type Error = PlanError;

    fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
        if let TelemetryValue::Table(rep) = telemetry {
            // let timestamp = rep
            //     .get(T_TIMESTAMP)
            //     .map(|v| i64::try_from(v.clone()).map(|secs| Utc.timestamp(secs, 0)))
            //     .ok_or(PlanError::DataNotFound(T_NR_TASK_MANAGERS.to_string()))??;

            let nr_task_managers = rep
                .get(T_NR_TASK_MANAGERS)
                .map(|v| u8::try_from(v.clone()))
                .ok_or(PlanError::DataNotFound(T_NR_TASK_MANAGERS.to_string()))??;

            let records_out_per_sec = rep
                .get(T_RECORDS_OUT_PER_SEC)
                .map(|v| f64::try_from(v.clone()))
                .ok_or(PlanError::DataNotFound(T_RECORDS_OUT_PER_SEC.to_string()))??;

            // Ok(Benchmark { timestamp, nr_task_managers, records_out_per_sec })
            Ok(Benchmark {
                nr_task_managers,
                records_out_per_sec: RecordsPerSecond(records_out_per_sec),
            })
        } else {
            Err(TelemetryError::TypeError {
                expected: format!("{}", TypeExpectation::Table),
                actual: Some(format!("{:?}", telemetry)),
            }
            .into())
        }
    }
}
