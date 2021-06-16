use statrs::statistics::Statistics;
use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Anomaly {
    Low,
    High,
}

/// Signal detection using the z-score algorithm.
///
/// Per original author (see: https://stackoverflow.com/questions/22583391/peak-signal-detection-in-realtime-timeseries-data/22640362#22640362)
/// Guidance for configuring algorithm:
///
/// `lag`: the lag parameter determines how much your data will be smoothed and how adaptive the
/// algorithm is to changes in the long-term average of the data. The more stationary your data
/// is, the more lags you should include (this should improve the robustness of the algorithm).
/// If your data contains time-varying trends, you should consider how quickly you want the
/// algorithm to adapt to these trends. I.e., if you put lag at 10, it takes 10 'periods' before
/// the algorithm's threshold is adjusted to any systematic changes in the long-term average.
/// So choose the lag parameter based on the trending behavior of your data and how adaptive you
/// want the algorithm to be.
///
/// `influence`: this parameter determines the influence of signals on the algorithm's detection
/// threshold. If put at 0, signals have no influence on the threshold, such that future signals
/// are detected based on a threshold that is calculated with a mean and standard deviation that
/// is not influenced by past signals. If put at 0.5, signals have half the influence of normal
/// data points. Another way to think about this is that if you put the influence at 0, you
/// implicitly assume stationarity (i.e. no matter how many signals there are, you always expect
/// the time series to return to the same average over the long term). If this is not the case,
/// you should put the influence parameter somewhere between 0 and 1, depending on the extent to
/// which signals can systematically influence the time-varying trend of the data. E.g., if
/// signals lead to a structural break of the long-term average of the time series, the
/// influence parameter should be put high (close to 1) so the threshold can react to structural
/// breaks quickly.
///
/// `threshold`: the threshold parameter is the number of standard deviations from the moving mean
/// above which the algorithm will classify a new datapoint as being a signal. For example, if a
/// new datapoint is 4.0 standard deviations above the moving mean and the threshold parameter
/// is set as 3.5, the algorithm will identify the datapoint as a signal. This parameter should
/// be set based on how many signals you expect. For example, if your data is normally
/// distributed, a threshold (or: z-score) of 3.5 corresponds to a signaling probability of
/// 0.00047 (from this table), which implies that you expect a signal once every 2128 datapoints
/// (1/0.00047). The threshold therefore directly influences how sensitive the algorithm is and
/// thereby also determines how often the algorithm signals. Examine your own data and choose a
/// sensible threshold that makes the algorithm signal when you want it to (some trial-and-error
/// might be needed here to get to a good threshold for your purpose).
///
/// Brakel, J.P.G. van (2014). "Robust peak detection algorithm using z-scores".
/// Stack Overflow. Available at: https://stackoverflow.com/questions/22583391/peak-signal-detection-in-realtime-timeseries-data/22640362#22640362
/// (version: 2020-11-08).
#[derive(Debug)]
pub struct SignalDetector {
    threshold: f64,
    influence: f64,
    window: VecDeque<f64>,
}

impl SignalDetector {
    /// Create an signal detector applying the z-score algorithm.
    ///
    /// ### `lag`
    /// moving window lag used to smooth data
    ///
    /// ### `threshold`
    /// the z-score at which the algorithm signals in terms of how many standard deviations away
    /// from moving mean
    ///
    /// ### `influence`
    /// the influence (between 0 and 1) of new signals on the mean and standard deviation.
    /// 0 ignores signals completely for recalculating the new threshold (most robust option, but
    /// assumes stationarity).
    /// 0.5 gives the signal half of the influence that normal data points have.
    /// 1 gives the signal full influence that other data points have (least robust option).
    /// For non-stationary data, the influence option should be set somewhere between 0 and 1.
    ///
    pub fn new(lag: usize, threshold: f64, influence: f64) -> Self {
        Self {
            threshold,
            influence,
            window: VecDeque::with_capacity(lag),
        }
    }

    pub fn clear(&mut self) {
        self.window.clear();
    }

    pub fn signal(&mut self, value: f64) -> Option<Anomaly> {
        if self.window.len() < self.window.capacity() {
            self.window.push_back(value);
            return None;
        }

        if let (Some((mean, std_dev)), Some(&window_last)) = (self.stats(), self.window.iter().last()) {
            let _ = self.window.pop_front();

            if (self.threshold * std_dev) < (value - mean).abs() {
                let next_value = (value * self.influence) + ((1. - self.influence) * window_last);
                self.window.push_back(next_value);
                if mean < value {
                    Some(Anomaly::High)
                } else {
                    Some(Anomaly::Low)
                }
            } else {
                self.window.push_back(value);
                None
            }
        } else {
            None
        }
    }

    pub fn stats(&self) -> Option<(f64, f64)> {
        if self.window.is_empty() {
            None
        } else {
            let mean = self.window.clone().mean();
            let std_dev = self.window.clone().std_dev();
            Some((mean, std_dev))
        }
    }
}

pub struct SignalIterator<I, F> {
    source: I,
    signal: F,
    detector: SignalDetector,
}

pub trait SignalFilter<I: Iterator> {
    fn peaks<F>(self, detector: SignalDetector, signal: F) -> SignalIterator<I, F>
    where
        F: FnMut(&I::Item) -> f64;
}

impl<I: Iterator> SignalFilter<I> for I {
    fn peaks<F>(self, detector: SignalDetector, signal: F) -> SignalIterator<I, F>
    where
        F: FnMut(&I::Item) -> f64,
    {
        SignalIterator {
            source: self,
            signal,
            detector,
        }
    }
}

impl<I, F> Iterator for SignalIterator<I, F>
where
    I: Iterator,
    F: FnMut(&I::Item) -> f64,
{
    type Item = (I::Item, Anomaly);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(item) = self.source.next() {
            let value = (self.signal)(&item);
            let peak = self.detector.signal(value);

            if peak.is_some() {
                return peak.map(|p| (item, p));
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::{Anomaly, SignalDetector, SignalFilter};
    use pretty_assertions::assert_eq;

    #[test]
    fn anomaly_sample_data() {
        let input = vec![
            1.0, 1.0, 1.1, 1.0, 0.9, 1.0, 1.0, 1.1, 1.0, 0.9, 1.0, 1.1, 1.0, 1.0, 0.9, 1.0, 1.0, 1.1, 1.0, 1.0, 1.0,
            1.0, 1.1, 0.9, 1.0, 1.1, 1.0, 1.0, 0.9, 1.0, 1.1, 1.0, 1.0, 1.1, 1.0, 0.8, 0.9, 1.0, 1.2, 0.9, 1.0, 1.0,
            1.1, 1.2, 1.0, 1.5, 1.0, 3.0, 2.0, 5.0, 3.0, 2.0, 1.0, 1.0, 1.0, 0.9, 1.0, 1.0, 3.0, 2.6, 4.0, 3.0, 3.2,
            2.0, 1.0, 1.0, 0.8, 4.0, 4.0, 2.0, 2.5, 1.0, 1.0, 1.0,
        ];

        let output: Vec<_> = input
            .into_iter()
            .enumerate()
            .peaks(SignalDetector::new(30, 5.0, 0.0), |e| e.1)
            .map(|((i, _), p)| (i, p))
            .collect();

        assert_eq!(
            output,
            vec![
                (45, Anomaly::High),
                (47, Anomaly::High),
                (48, Anomaly::High),
                (49, Anomaly::High),
                (50, Anomaly::High),
                (51, Anomaly::High),
                (58, Anomaly::High),
                (59, Anomaly::High),
                (60, Anomaly::High),
                (61, Anomaly::High),
                (62, Anomaly::High),
                (63, Anomaly::High),
                (67, Anomaly::High),
                (68, Anomaly::High),
                (69, Anomaly::High),
                (70, Anomaly::High),
            ]
        );
    }

    #[test]
    fn anomaly_sin_wave() {
        let input: Vec<f64> = vec![
            50, 53, 56, 59, 62, 65, 68, 71, 74, 77, 79, 82, 84, 86, 89, 90, 92, 94, 95, 96, 98, 98, 99, 100, 100, 100,
            100, 100, 99, 98, 98, 96, 95, 94, 92, 90, 89, 86, 84, 82, 79, 77, 74, 71, 68, 650, 62, 59, 56, 53, 50, 47,
            44, 41, 38, 35, 32, 29, 26, 23, 21, 18, 16, 14, 11, 10, 8, 6, 5, 4, 2, 2, 1, 0, 0, 0, 0, 0, 1, 2, 2, 4, 5,
            6, 8, 10, 11, 14, 16, 18, 21, 23, 26, 29, 32, 35, 38, 41, 44, 47, 50,
        ]
        .into_iter()
        .map(|v| v as f64)
        .collect();

        let output: Vec<_> = input
            .into_iter()
            .enumerate()
            .peaks(SignalDetector::new(5, 5.0, 0.0), |e| e.1)
            .map(|((i, _), p)| (i, p))
            .collect();

        assert_eq!(output, vec![(45, Anomaly::High),]);
    }
}
