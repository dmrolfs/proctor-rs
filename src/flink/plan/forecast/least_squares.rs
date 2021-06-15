use crate::error::PlanError;
use crate::flink::plan::forecast::{Workload, WorkloadForecast};
use crate::flink::MetricCatalog;
use statrs::statistics::Statistics;
use std::collections::VecDeque;
use super::signal::SignalDetector;

#[derive(Debug)]
pub struct LeastSquaresWorkloadForecast {
    data: VecDeque<(f64, f64)>,
    spike_detector: SignalDetector,
    consecutive_spikes: usize,
}

const MAX_RECENT_OBSERVATIONS: usize = 20;

impl Default for LeastSquaresWorkloadForecast {
    fn default() -> Self {
        Self {
            data: VecDeque::with_capacity(MAX_RECENT_OBSERVATIONS),
            spike_detector: SignalDetector::new(5, 5., 0.),
            consecutive_spikes: 0,
        }
    }
}

impl std::ops::Add<MetricCatalog> for LeastSquaresWorkloadForecast {
    type Output = LeastSquaresWorkloadForecast;

    fn add(mut self, rhs: MetricCatalog) -> Self::Output {
        self.add_observation(rhs);
        self
    }
}

impl WorkloadForecast for LeastSquaresWorkloadForecast {
    fn add_observation(&mut self, metrics: MetricCatalog) {
        let observation = Self::workload_observation_from(metrics);
        if self.workload_is_spiking(observation) {
            self.consecutive_spikes += 1;
        } else {
            self.consecutive_spikes = 0;

            if self.data.len() == MAX_RECENT_OBSERVATIONS {
                let _ = self.data.pop_front();
            }

            self.data.push_back(observation);
        }
    }

    fn clear(&mut self) {
        self.data.clear();
    }

    fn predict_workload(&self) -> Result<Workload, PlanError> {
        //todo: detect a peak or trough?
        //todo: spike detected?
        //todo: have enough data?
        //todo:
        todo!()
    }
}

const NR_PTS_FOR_PREDICTION: usize = 3;
const SPIKE_THRESHOLD: usize = 3;

impl LeastSquaresWorkloadForecast {
    fn have_enough_data(&self) -> bool {
        self.data.len() < NR_PTS_FOR_PREDICTION
    }

    fn workload_is_spiking(&mut self, observation: (f64, f64)) -> bool {
        self.spike_detector.signal(observation.1).is_some()
        // let values: Vec<f64> = self.data.iter().map(|pt| pt.1).collect();
        // let stddev = values.clone().std_dev();
        // let mean = values.mean();
        // observation.1 < (mean - 3. * stddev) || (mean + 3. * stddev) < observation.1
    }

    #[inline]
    fn exceeded_spike_threshold(&self) -> bool {
        SPIKE_THRESHOLD <= self.consecutive_spikes
    }

    fn near_extrema(&self) -> bool {
        todo!()
    }

    fn spline_slopes(&self) -> Vec<f64> {
        if self.data.is_empty() {
            return Vec::default();
        }

        let mut spline_slopes = Vec::with_capacity(self.data.len() - 1);
        let mut prev: Option<(f64, f64)> = None;

        for (x2, y2) in self.data.iter() {
            match prev {
                None => prev = Some((*x2, *y2)),
                Some((x1, y1)) => {
                    spline_slopes.push((y2 - y1) / (x2 - x1));
                    prev = Some((*x2, *y2));
                }
            }
        }

        spline_slopes
    }
}
