// use crate::elements::{TelemetryValue, ToTelemetry};
// use crate::error::{DecisionError, TelemetryError, TypeExpectation};
// use oso::PolarClass;
// use serde::{Deserialize, Serialize};
// use std::cmp::Ordering;
// use std::convert::TryFrom;
// use std::fmt::Debug;
// use std::collections::BTreeMap;
// use std::time::Instant;
//
// #[derive(Debug, Default, Clone)]
// pub struct PerformanceHistory {
//     benchmarks: BTreeMap<i32, Benchmark>,
// }
//
// impl PerformanceHistory {
//     pub fn add_benchmark(&mut self, b: Benchmark) {
//         self.benchmarks.insert(b.nr_task_managers, b);
//     }
//
//     pub fn benchmark_for_nr_task_managers(&self, nr_task_managers: i32) -> Option<Benchmark> {
//         if let Some(bench) = self.benchmarks.get(&nr_task_managers) {
//             Some(bench.clone())
//         } else {
//             let (lo, hi) = self.find_nearest_to(nr_task_managers);
//             Self::interpolate_benchmark(lo, hi)
//         }
//     }
//
//     fn find_nearest_to(&self, nr_task_managers: i32) -> (Option<&Benchmark>, Option<&Benchmark>) {
//         if self.benchmarks.is_empty() {
//             return (None, None);
//         }
//
//         let mut lo = None;
//         let mut hi = None;
//
//         for key in self.benchmarks.keys() {
//             if key <= &nr_task_managers { lo = Some(key); }
//             if &nr_task_managers <= key {
//                 hi = Some(key);
//                 break;
//             }
//         }
//
//         let lo_bench = lo.and_then(|idx| self.benchmarks.get(idx));
//         let hi_bench = hi.and_then(|idx| self.benchmarks.get(idx));
//         (lo_bench, hi_bench)
//     }
//
//     fn interpolate_benchmark(lo: Option<&Benchmark>, hi: Option<&Benchmark>) -> Option<Benchmark> {
//
//     }
// }
//
//
// // --- Predict Workload ---
// impl PerformanceHistory {
//     fn predict_workload_at(&self, instant: Instant) -> f32 {
//         // predict based on calculated regression fn.
//         todo!()
//     }
// }
//
// // --- Taregt Events per Second ---
// impl PerformanceHistory {
//
// }
//
// // --- Cluster Size Lookup ---
// impl PerformanceHistory {
//
// }
//
// #[derive(Debug, PolarClass, Default, Clone, PartialEq, Serialize, Deserialize)]
// pub struct Benchmark {
//     #[polar(attribute)]
//     pub nr_task_managers: i32,
//
//     #[polar(attribute)]
//     pub records_in_per_sec: f32,
//
//     #[polar(attribute)]
//     pub records_out_per_sec: f32,
// }
//
// impl Benchmark {
//     pub const ZERO: Benchmark = Benchmark {
//         nr_task_managers: 0,
//         records_in_per_sec: 0.0,
//         records_out_per_sec: 0.0,
//     };
// }
//
// // impl Ord for Benchmark {
// //     fn cmp(&self, other: &Self) -> Ordering {
// //         self.nr_task_managers.cmp(&other.nr_task_managers)
// //     }
// // }
//
// impl PartialOrd for Benchmark {
//     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//         Some(self.nr_task_managers.cmp(&other.nr_task_managers))
//     }
// }
//
// impl From<Benchmark> for TelemetryValue {
//     fn from(that: Benchmark) -> Self {
//         TelemetryValue::Table(maplit::hashmap! {
//             T_NR_TASK_MANAGERS.to_string() => that.nr_task_managers.to_telemetry(),
//             T_RECORDS_IN_PER_SEC.to_string() => that.records_in_per_sec.to_telemetry(),
//             T_RECORDS_OUT_PER_SEC.to_string() => that.records_out_per_sec.to_telemetry(),
//         })
//     }
// }
//
// const T_NR_TASK_MANAGERS: &'static str = "nr_task_managers";
// const T_RECORDS_OUT_PER_SEC: &'static str = "records_out_per_sec";
// const T_RECORDS_IN_PER_SEC: &'static str = "records_in_per_sec";
//
// impl TryFrom<TelemetryValue> for Benchmark {
//     type Error = DecisionError;
//
//     fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
//         if let TelemetryValue::Table(rep) = telemetry {
//             let nr_task_managers = rep
//                 .get(T_NR_TASK_MANAGERS)
//                 .map(|v| i32::try_from(v.clone()))
//                 .ok_or(DecisionError::DataNotFound(T_NR_TASK_MANAGERS.to_string()))??;
//
//             let records_in_per_sec = rep
//                 .get(T_RECORDS_IN_PER_SEC)
//                 .map(|v| f32::try_from(v.clone()))
//                 .ok_or(DecisionError::DataNotFound(T_RECORDS_IN_PER_SEC.to_string()))??;
//
//             let records_out_per_sec = rep
//                 .get(T_RECORDS_OUT_PER_SEC)
//                 .map(|v| f32::try_from(v.clone()))
//                 .ok_or(DecisionError::DataNotFound(T_RECORDS_OUT_PER_SEC.to_string()))??;
//
//             Ok(Benchmark {
//                 nr_task_managers,
//                 records_in_per_sec,
//                 records_out_per_sec,
//             })
//         } else {
//             Err(TelemetryError::TypeError {
//                 expected: format!("{}", TypeExpectation::Table),
//                 actual: Some(format!("{:?}", telemetry)),
//             }
//             .into())
//         }
//     }
// }
