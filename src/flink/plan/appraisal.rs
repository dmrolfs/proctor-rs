use std::collections::BTreeMap;
use std::fmt::Debug;

use async_trait::async_trait;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use super::Benchmark;
use crate::error::PlanError;
use crate::flink::plan::benchmark::BenchmarkRange;
use crate::flink::plan::RecordsPerSecond;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Appraisal(BTreeMap<u8, BenchmarkRange>);

impl Appraisal {
    pub fn add_lower_benchmark(&mut self, b: Benchmark) {
        if let Some(entry) = self.0.get_mut(&b.nr_task_managers) {
            entry.lo_rate = Some(b.records_out_per_sec);
        } else {
            let entry = BenchmarkRange::lo_from(b);
            self.0.insert(entry.nr_task_managers, entry);
        }
    }

    pub fn add_upper_benchmark(&mut self, b: Benchmark) {
        if let Some(entry) = self.0.get_mut(&b.nr_task_managers) {
            entry.hi_rate = Some(b.records_out_per_sec);
        } else {
            let entry = BenchmarkRange::hi_from(b);
            self.0.insert(entry.nr_task_managers, entry);
        }
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }
}

impl Appraisal {
    pub fn cluster_size_for_workload(&self, workload_rate: &RecordsPerSecond) -> Option<usize> {
        self.evaluate_neighbors(workload_rate)
            .map(|neighbors| neighbors.cluster_size_for_workload_rate(workload_rate))
    }

    fn evaluate_neighbors(&self, workload_rate: &RecordsPerSecond) -> Option<BenchNeighbors> {
        let mut lo = None;
        let mut hi = None;

        for (_, benchmark_range) in self.0.iter() {
            if let Some(ref entry_hi) = benchmark_range.hi_mark() {
                if &entry_hi.records_out_per_sec <= workload_rate {
                    lo = Some(entry_hi.clone());
                } else {
                    hi = Some(entry_hi.clone());
                    if let Some(ref entry_lo) = benchmark_range.lo_mark() {
                        if &entry_lo.records_out_per_sec <= workload_rate {
                            lo = Some(entry_lo.clone());
                        }
                    }
                    break;
                }
            }
        }

        self.make_neighbors(lo, hi)
    }

    fn make_neighbors(&self, lo: Option<Benchmark>, hi: Option<Benchmark>) -> Option<BenchNeighbors> {
        match (lo, hi) {
            (None, None) => None,
            (Some(mark), None) => Some(BenchNeighbors::AboveHighest(mark)),
            (None, Some(mark)) => Some(BenchNeighbors::BelowLowest(mark)),
            (Some(lo), Some(hi)) => Some(BenchNeighbors::Between { lo, hi }),
        }
    }
}

#[derive(Debug, Clone)]
enum BenchNeighbors {
    BelowLowest(Benchmark),
    AboveHighest(Benchmark),
    Between { lo: Benchmark, hi: Benchmark },
}

const MINIMAL_CLUSTER_SIZE: usize = 1;

impl BenchNeighbors {
    fn cluster_size_for_workload_rate(&self, workload_rate: &RecordsPerSecond) -> usize {
        match self {
            BenchNeighbors::BelowLowest(lo) => Self::extrapolate_lo(workload_rate, lo),
            BenchNeighbors::AboveHighest(hi) => Self::extrapolate_hi(workload_rate, hi),
            BenchNeighbors::Between { lo, hi } => Self::interpolate(workload_rate, lo, hi),
        }
    }

    fn extrapolate_lo(workload_rate: &RecordsPerSecond, lo: &Benchmark) -> usize {
        let workload_rate: f64 = workload_rate.into();
        let lo_rate: f64 = lo.records_out_per_sec.into();

        let ratio: f64 = lo_rate / (lo.nr_task_managers as f64);
        let calculated = (ratio * workload_rate).ceil() as usize;
        std::cmp::max(MINIMAL_CLUSTER_SIZE, calculated)
    }

    fn extrapolate_hi(workload_rate: &RecordsPerSecond, hi: &Benchmark) -> usize {
        let hi_rate: f64 = hi.records_out_per_sec.into();

        let ratio: f64 = hi_rate / (hi.nr_task_managers as f64);
        (ratio * workload_rate.0).ceil() as usize
    }

    fn interpolate(workload_rate: &RecordsPerSecond, lo: &Benchmark, hi: &Benchmark) -> usize {
        let workload_rate: f64 = workload_rate.into();
        let lo_rate: f64 = lo.records_out_per_sec.into();
        let hi_rate: f64 = hi.records_out_per_sec.into();

        let ratio: f64 = (workload_rate - lo_rate) / (hi_rate - lo_rate);
        (ratio * (lo.nr_task_managers as f64 + hi.nr_task_managers as f64)).ceil() as usize
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bench_neighbors_interpolate() -> anyhow::Result<()> {

    }
}