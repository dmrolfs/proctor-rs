use super::signal::SignalDetector;
use super::{Point, Workload, WorkloadForecast};
use crate::error::PlanError;
use crate::flink::MetricCatalog;
use chrono::{TimeZone, Utc};
use nalgebra::{Matrix3, Matrix3x1, SMatrix};
use num_traits::pow;
use statrs::statistics::Statistics;
use std::collections::VecDeque;

#[derive(Debug)]
pub struct LeastSquaresWorkloadForecast {
    data: VecDeque<Point>,
    spike_detector: SignalDetector,
    consecutive_spikes: usize,
}

const OBSERVATION_WINDOW_SIZE: usize = 20;

impl Default for LeastSquaresWorkloadForecast {
    fn default() -> Self {
        LeastSquaresWorkloadForecast::new(OBSERVATION_WINDOW_SIZE, 5., 0.)
    }
}

impl LeastSquaresWorkloadForecast {
    pub fn new(window: usize, spike_threshold: f64, spike_influence: f64) -> Self {
        Self {
            data: VecDeque::with_capacity(window),
            spike_detector: SignalDetector::new(window, spike_threshold, spike_influence),
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

const MIN_REQ_SAMPLES_FOR_PREDICTION: usize = 5;
const SPIKE_THRESHOLD: usize = 3;

impl WorkloadForecast for LeastSquaresWorkloadForecast {
    fn add_observation(&mut self, metrics: MetricCatalog) {
        let observation = Self::workload_observation_from(metrics);
        self.measure_spike(observation);

        while OBSERVATION_WINDOW_SIZE <= self.data.len() {
            let _ = self.data.pop_front();
        }
        self.data.push_back(observation);
    }

    fn clear(&mut self) {
        self.data.clear();
        self.spike_detector.clear();
        self.consecutive_spikes = 0;
    }

    fn predict_next_workload(&mut self) -> Result<Workload, PlanError> {
        // drop up to start of spike in order to establish new prediction function
        if self.exceeded_spike_threshold() {
            self.drop_data(..(self.data.len() - SPIKE_THRESHOLD));
        }

        if self.have_enough_data_for_prediction() {
            return Ok(Workload::NotEnoughData);
        }

        self.do_predict_workload()
    }
}

pub(crate) trait RegressionStrategy {
    fn calculate(&self, x: f64) -> Result<Workload, PlanError>;
    fn correlation_coefficient(&self) -> f64;
}

pub(crate) struct LinearRegression {
    slope: f64,
    y_intercept: f64,
    correlation_coefficient: f64,
}

impl LinearRegression {
    pub fn from_data(data: &[Point]) -> Result<Self, PlanError> {
        let (n, sum_x, sum_y, sum_xy, sum_x2, sum_y2) = Self::components(data);
        let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - pow(sum_x, 2));
        let y_intercept = (sum_y - slope * sum_x) / n;
        let correlation_coefficient =
            (n * sum_xy - sum_x * sum_y) / ((n * sum_x2 - pow(sum_x, 2)) * (n * sum_y2 - pow(sum_y, 2))).sqrt();
        Ok(Self { slope, y_intercept, correlation_coefficient })
    }

    fn components(data: &[Point]) -> (f64, f64, f64, f64, f64, f64) {
        let (sum_x, sum_y, sum_xy, sum_x2, sum_y2) = data.iter().fold(
            (0., 0., 0., 0., 0.),
            |(acc_x, acc_y, acc_xy, acc_x2, acc_y2), (x, y)| {
                (
                    acc_x + x,
                    acc_y + y,
                    acc_xy + x * y,
                    acc_x2 + pow(*x, 2),
                    acc_y2 + pow(*y, 2),
                )
            },
        );

        (data.len() as f64, sum_x, sum_y, sum_xy, sum_x2, sum_y2)
    }
}

impl RegressionStrategy for LinearRegression {
    fn calculate(&self, x: f64) -> Result<Workload, PlanError> {
        Ok(Workload::RecordsPerSecond(self.slope * x + self.y_intercept))
    }

    #[inline]
    fn correlation_coefficient(&self) -> f64 {
        self.correlation_coefficient
    }
}

pub(crate) struct QuadraticRegression {
    a: f64,
    b: f64,
    c: f64,
    correlation_coefficient: f64,
}

impl QuadraticRegression {
    pub fn from_data(data: &[Point]) -> Result<Self, PlanError> {
        let n = data.len() as f64;
        let (sum_x, sum_y, sum_x2, sum_x3, sum_x4, sum_xy, sum_x2y) = data
            .iter()
            .map(|(x, y)| (x, y, pow(*x, 2), pow(*x, 3), pow(*x, 4), x * y, pow(*x, 2) * y))
            .fold(
                (0., 0., 0., 0., 0., 0., 0.),
                |(acc_x, acc_y, acc_x2, acc_x3, acc_x4, acc_xy, acc_x2y), (x, y, x2, x3, x4, xy, x2y)| {
                    (
                        acc_x + x,
                        acc_y + y,
                        acc_x2 + x2,
                        acc_x3 + x3,
                        acc_x4 + x4,
                        acc_xy + xy,
                        acc_x2y + x2y,
                    )
                },
            );

        tracing::trace!(%sum_x, %sum_y, %sum_x2, %sum_x3, %sum_x4, %sum_xy, %sum_x2y, %n, "intermediate calculations");

        let m_x = Matrix3::new(sum_x4, sum_x3, sum_x2, sum_x3, sum_x2, sum_x, sum_x2, sum_x, n);
        tracing::trace!(X=?m_x, "Matrix X");

        let m_y = Matrix3x1::new(sum_x2y, sum_xy, sum_y);
        tracing::trace!(Y=?m_y, "Matrix Y");

        let decomp = m_x.lu();
        let coefficients = decomp.solve(&m_y).ok_or(PlanError::LinearResolutionFailed(
            Utc.timestamp_millis(data[data.len() - 1].0 as i64),
        ))?;

        let a = coefficients[(0, 0)];
        let b = coefficients[(1, 0)];
        let c = coefficients[(2, 0)];

        let y_mean = sum_y / n;
        let sse = data
            .iter()
            .fold(0., |acc, (x, y)| acc + pow(y - (a * pow(*x, 2) + b * x + c), 2));

        let sst = data.iter().fold(0., |acc, (x, y)| acc + pow(y - y_mean, 2));

        let correlation_coefficient = (1. - sse / sst).sqrt();

        Ok(Self { a, b, c, correlation_coefficient })
    }
}

impl RegressionStrategy for QuadraticRegression {
    fn calculate(&self, x: f64) -> Result<Workload, PlanError> {
        Ok(Workload::RecordsPerSecond(self.a * pow(x, 2) + self.b * x + self.c))
    }

    #[inline]
    fn correlation_coefficient(&self) -> f64 {
        self.correlation_coefficient
    }
}

impl LeastSquaresWorkloadForecast {
    fn do_predict_workload(&self) -> Result<Workload, PlanError> {
        let data: Vec<Point> = self.data.iter().cloned().collect();
        if let Some(next_timestamp) = Self::estimate_next_timestamp(&data) {
            let model = Self::do_select_model(&data)?;
            model.calculate(next_timestamp)
        } else {
            Ok(Workload::NotEnoughData)
        }
    }

    fn do_select_model(data: &[Point]) -> Result<Box<dyn RegressionStrategy>, PlanError> {
        let linear = LinearRegression::from_data(&data)?;
        let quadratic = QuadraticRegression::from_data(&data)?;
        let model: Box<dyn RegressionStrategy> =
            if quadratic.correlation_coefficient() <= linear.correlation_coefficient {
                Box::new(linear)
            } else {
                Box::new(quadratic)
            };
        Ok(model)
    }

    fn do_calculate<R: RegressionStrategy>(regression: R, timestamp: f64) -> Result<Workload, PlanError> {
        regression.calculate(timestamp)
    }

    fn estimate_next_timestamp(data: &[Point]) -> Option<f64> {
        data.get(data.len() - 1).map(|(x, y)| {
            // calculate average only if there's data.
            let avg_ts_delta = Self::splines(data)
                .into_iter()
                .map(|((x1, _), (x2, _))| x2 - x1)
                .collect::<Vec<_>>()
                .mean();
            x + avg_ts_delta
        })
    }

    fn splines(data: &[Point]) -> Vec<(Point, Point)> {
        if data.len() < 2 {
            return Vec::default();
        }

        let mut result = Vec::with_capacity(data.len() - 1);

        let mut prev: Option<Point> = None;
        for (x2, y2) in data.iter() {
            if let Some((x1, y1)) = prev {
                result.push(((x1, y1), (*x2, *y2)));
            }

            prev = Some((*x2, *y2));
        }

        result
    }
}

impl LeastSquaresWorkloadForecast {
    fn have_enough_data_for_prediction(&self) -> bool {
        self.data.len() < MIN_REQ_SAMPLES_FOR_PREDICTION
    }

    fn measure_spike(&mut self, observation: Point) -> usize {
        if let Some(spike) = self.spike_detector.signal(observation.1) {
            self.consecutive_spikes += 1;
        } else {
            self.consecutive_spikes = 0;
        }

        self.consecutive_spikes
    }

    #[inline]
    fn exceeded_spike_threshold(&self) -> bool {
        SPIKE_THRESHOLD <= self.consecutive_spikes
    }

    fn drop_data<R>(&mut self, range: R) -> Vec<Point>
    where
        R: std::ops::RangeBounds<usize>,
    {
        self.data.drain(range).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flink::plan::forecast::least_squares::LeastSquaresWorkloadForecast;
    use crate::flink::plan::forecast::{Point, Workload};
    use approx::{assert_abs_diff_eq, assert_relative_eq, assert_ulps_eq};
    use num_traits::pow;
    use pretty_assertions::assert_eq;
    use statrs::statistics::Statistics;

    #[test]
    fn test_measure_spike() -> anyhow::Result<()> {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_measure_spike");
        let _ = main_span.enter();

        let data: Vec<Point> = vec![
            1.0, 1.0, 1.1, 1.0, 0.9, 1.0, 1.0, 1.1, 1.0, 0.9, // 00 - 09
            1.0, 1.1, 1.0, 1.0, 0.9, 1.0, 1.0, 1.1, 1.0, 1.0, // 10 - 19
            1.0, 1.0, 1.1, 0.9, 1.0, 1.1, 1.0, 1.0, 0.9, 1.0, // 20 - 29
            1.1, 1.0, 1.0, 1.1, 1.0, 0.8, 0.9, 1.0, 1.2, 0.9, // 30 - 39
            1.0, 1.0, 1.1, 1.2, 1.0, 1.5, 1.0, 3.0, 2.0, 5.0, // 40 - 49
            3.0, 2.0, 1.0, 1.0, 1.0, 0.9, 1.0, 1.0, 3.0, 2.6, // 50 - 59
            4.0, 3.0, 3.2, 2.0, 1.0, 1.0, 0.8, 4.0, 4.0, 2.0, // 60 - 69
            2.5, 1.0, 1.0, 1.0, // 70 - 73
        ]
        .into_iter()
        .enumerate()
        .map(|(x, y)| (x as f64, y))
        .collect();

        let test_scenario = |influence: f64, measurements: Vec<usize>| {
            let test_data: Vec<(Point, usize)> = data.clone().into_iter().zip(measurements).collect();
            let mut forecast = LeastSquaresWorkloadForecast::new(30, 5., influence);

            for (pt, expected) in test_data.into_iter() {
                let actual = forecast.measure_spike(pt);
                assert_eq!((influence, pt, actual), (influence, pt, expected));
            }
        };

        let spike_measure_0 = vec![
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 00 - 09
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 10 - 19
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 20 - 29
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 30 - 39
            0, 0, 0, 0, 0, 1, 0, 1, 2, 3, // 40 - 49
            4, 5, 0, 0, 0, 0, 0, 0, 1, 2, // 50 - 59
            3, 4, 5, 6, 0, 0, 0, 1, 2, 3, // 60 - 69
            4, 0, 0, 0, // 70 - 73
        ];
        test_scenario(0., spike_measure_0);

        let spike_measure_0_10 = vec![
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 00 - 09
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 10 - 19
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 20 - 29
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 30 - 39
            0, 0, 0, 0, 0, 1, 0, 1, 2, 3, // 40 - 49
            4, 0, 0, 0, 0, 0, 0, 0, 1, 2, // 50 - 59
            3, 4, 5, 0, 0, 0, 0, 1, 2, 0, // 60 - 69
            0, 0, 0, 0, // 70 - 73
        ];
        test_scenario(0.1, spike_measure_0_10);

        let spike_measure_0_25 = vec![
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 00 - 09
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 10 - 19
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 20 - 29
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 30 - 39
            0, 0, 0, 0, 0, 1, 0, 1, 2, 3, // 40 - 49
            4, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 50 - 59
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 60 - 69
            0, 0, 0, 0, // 70 - 73
        ];
        test_scenario(0.25, spike_measure_0_25);

        let spike_measure_0_5 = vec![
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 00 - 09
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 10 - 19
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 20 - 29
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 30 - 39
            0, 0, 0, 0, 0, 1, 0, 1, 0, 1, // 40 - 49
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 50 - 59
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 60 - 69
            0, 0, 0, 0, // 70 - 73
        ];
        test_scenario(0.5, spike_measure_0_5);

        let spike_measure_1 = vec![
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 00 - 09
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 10 - 19
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 20 - 29
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 30 - 39
            0, 0, 0, 0, 0, 1, 0, 1, 0, 1, // 40 - 49
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 50 - 59
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 60 - 69
            0, 0, 0, 0, // 70 - 73
        ];
        test_scenario(1., spike_measure_1);

        Ok(())
    }

    #[test]
    fn test_quadratic_regression_coefficients() -> anyhow::Result<()> {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_quadratic_regression_coefficients");
        let _ = main_span.enter();

        // example take from https://www.symbolab.com/solver/system-of-equations-calculator/9669a%2B1225b%2B165c%3D5174.3%2C%201225a%2B165b%2B25c%3D809.7%2C%20165a%2B25b%2B5c%3D167.1#
        let data = vec![(1., 32.5), (3., 37.3), (5., 36.4), (7., 32.4), (9., 28.5)];

        let actual = QuadraticRegression::from_data(&data)?;
        assert_relative_eq!(actual.a, (-41. / 112.), epsilon = 1.0e-10);
        assert_relative_eq!(actual.b, (2111. / 700.), epsilon = 1.0e-10);
        assert_relative_eq!(actual.c, (85181. / 2800.), epsilon = 1.0e-10);
        Ok(())
    }

    #[test]
    fn test_quadratic_regression_prediction() -> anyhow::Result<()> {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_quadratic_regression_prediction");
        let _ = main_span.enter();

        // example take from https://www.symbolab.com/solver/system-of-equations-calculator/9669a%2B1225b%2B165c%3D5174.3%2C%201225a%2B165b%2B25c%3D809.7%2C%20165a%2B25b%2B5c%3D167.1#
        let data = vec![(1., 32.5), (3., 37.3), (5., 36.4), (7., 32.4), (9., 28.5)];
        let regression = QuadraticRegression::from_data(&data)?;
        if let Workload::RecordsPerSecond(actual) = regression.calculate(11.)? {
            let expected = (-41. / 112.) * pow(11., 2) + (2111. / 700.) * 11. + (85181. / 2800.);
            assert_relative_eq!(actual, expected, epsilon = 1.0e-10);
        } else {
            panic!("failed to calculate regression");
        }
        Ok(())
    }

    #[test]
    fn test_estimate_next_timestamp() -> anyhow::Result<()> {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_estimate_next_timestamp");
        let _ = main_span.enter();

        let data = vec![(1., 32.5), (3., 37.3), (5., 36.4), (7., 32.4), (9., 28.5)];
        let actual = LeastSquaresWorkloadForecast::estimate_next_timestamp(&data).unwrap();
        assert_relative_eq!(actual, 11., epsilon = 1.0e-10);
        Ok(())
    }

    #[test]
    fn test_linear_coefficients() -> anyhow::Result<()> {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_linear_coefficients");
        let _ = main_span.enter();

        // example taken from https://tutorme.com/blog/post/quadratic-regression/
        let data = vec![(1., 1.), (2., 3.), (3., 2.), (4., 3.), (5., 5.)];
        let actual = LinearRegression::from_data(&data)?;
        assert_relative_eq!(actual.slope, 0.8, epsilon = 1.0e-10);
        assert_relative_eq!(actual.y_intercept, 0.4, epsilon = 1.0e-10);
        Ok(())
    }

    #[test]
    fn test_linear_regression() -> anyhow::Result<()> {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_linear_regression");
        let _ = main_span.enter();

        // example taken from https://tutorme.com/blog/post/quadratic-regression/
        let data = vec![(1., 1.), (2., 3.), (3., 2.), (4., 3.), (5., 5.)];
        let regression = LinearRegression::from_data(&data)?;

        let accuracy = 0.69282037;
        assert_relative_eq!(regression.correlation_coefficient(), 0.853, epsilon = 1.0e-3);

        if let Workload::RecordsPerSecond(actual) = regression.calculate(3.)? {
            assert_relative_eq!(actual, 3., epsilon = accuracy);
        } else {
            panic!("failed to calculate regression");
        }
        Ok(())
    }
}
