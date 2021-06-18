use super::Benchmark;
use itertools::Itertools;
use oso::PolarClass;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Default, PolarClass, Clone, Serialize, Deserialize)]
pub struct ClusterPerformance(BTreeMap<usize, Benchmark>);

impl ClusterPerformance {
    pub fn add_benchmark(&mut self, b: Benchmark) {
        let _ = self.0.insert(b.nr_task_managers, b);
    }

    pub fn clear(&mut self) {
        self.0.clear()
    }
}

impl std::ops::Add<Benchmark> for ClusterPerformance {
    type Output = ClusterPerformance;

    fn add(mut self, rhs: Benchmark) -> Self::Output {
        self.add_benchmark(rhs);
        self
    }
}

impl ClusterPerformance {
    pub fn cluster_size_for_workload(&self, workload_rate: f64) -> Option<usize> {
        self.evaluate_neighbors(workload_rate)
            .map(|neighbors| neighbors.cluster_size_for_workload_rate(workload_rate))
    }

    fn evaluate_neighbors(&self, workload_rate: f64) -> Option<BenchNeighbors> {
        if self.0.is_empty() {
            return None;
        }

        let mut lo = None;
        let mut hi = None;

        for (_, benchmark) in self.0.iter() {
            if benchmark.records_out_per_sec <= workload_rate {
                lo = Some(benchmark);
            } else {
                hi = Some(benchmark);
                break;
            }
        }

        let neighbors = match (lo, hi) {
            (None, None) => None,

            (Some(_), None) => {
                //todo - switch to once stable: self.0.last_key_value().map(|(_, hi)| BenchNeighbors::AboveHighest(hi))
                // self.0
                //     .last_key_value()
                //     .map(|(_, hi)| BenchNeighbors::AboveHighest(hi)p

                self.0
                    .iter()
                    .fold1(|_, latest| latest)
                    .map(|(_, hi)| BenchNeighbors::AboveHighest(hi))
            }

            (None, Some(mark)) => Some(BenchNeighbors::BelowLowest(mark)),

            (Some(lo), Some(hi)) => Some(BenchNeighbors::Between { lo, hi }),
        };

        tracing::trace!(?neighbors, %workload_rate, "nearest benchmarks to target workload rate.");
        neighbors
    }
}

#[derive(Debug, Clone)]
enum BenchNeighbors<'a> {
    BelowLowest(&'a Benchmark),
    AboveHighest(&'a Benchmark),
    Between { lo: &'a Benchmark, hi: &'a Benchmark },
}

const MINIMAL_CLUSTER_SIZE: usize = 2;

impl<'a> BenchNeighbors<'a> {
    fn cluster_size_for_workload_rate(&self, workload_rate: f64) -> usize {
        match self {
            BenchNeighbors::BelowLowest(lo) => Self::extrapolate_lo(workload_rate, lo),
            BenchNeighbors::AboveHighest(hi) => Self::extrapolate_hi(workload_rate, hi),
            BenchNeighbors::Between { lo, hi } => Self::interpolate(workload_rate, lo, hi),
        }
    }

    fn extrapolate_lo(workload_rate: f64, lo: &Benchmark) -> usize {
        let ratio = lo.records_out_per_sec / (lo.nr_task_managers as f64);
        let calculated = (ratio * workload_rate).ceil() as usize;
        std::cmp::max(MINIMAL_CLUSTER_SIZE, calculated)
    }

    fn extrapolate_hi(workload_rate: f64, hi: &Benchmark) -> usize {
        let ratio = hi.records_out_per_sec / (hi.nr_task_managers as f64);
        (ratio * workload_rate).ceil() as usize
    }

    fn interpolate(workload_rate: f64, lo: &Benchmark, hi: &Benchmark) -> usize {
        let ratio = (workload_rate - lo.records_out_per_sec) / (hi.records_out_per_sec - lo.records_out_per_sec);
        (ratio * (lo.nr_task_managers as f64 + hi.nr_task_managers as f64)).ceil() as usize
    }
}