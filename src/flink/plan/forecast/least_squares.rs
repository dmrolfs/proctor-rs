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
        Self {
            data: VecDeque::with_capacity(OBSERVATION_WINDOW_SIZE),
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

    fn predict_workload(&mut self) -> Result<Workload, PlanError> {
        // drop up to start of spike in order to establish new prediction function
        if self.exceeded_spike_threshold() {
            self.drop_data(..(self.data.len() - SPIKE_THRESHOLD));
        }

        let mut strategy = PredictionStrategy::LinearRegression;

        if let Some(extrema_idx) = self.extrema_position() {
            self.drop_data(..(extrema_idx));
        }

        if self.have_enough_data_for_prediction() {
            return Ok(Workload::NotEnoughData);
        }

        self.do_predict_workload(strategy)
    }
}

enum PredictionStrategy {
    LinearRegression,
    QuadraticRegression,
}

impl LeastSquaresWorkloadForecast {
    fn do_predict_workload(&self, strategy: PredictionStrategy) -> Result<Workload, PlanError> {
        let data: Vec<Point> = self.data.iter().cloned().collect();
        if let Some(next_timestamp) = Self::estimate_next_timestamp(&data) {
            match strategy {
                PredictionStrategy::LinearRegression => Self::do_linear_regression(&data, next_timestamp),
                PredictionStrategy::QuadraticRegression => Self::do_quadratic_regression(&data, next_timestamp),
            }
        } else {
            Ok(Workload::NotEnoughData)
        }
    }

    fn do_linear_regression(data: &[Point], timestamp: f64) -> Result<Workload, PlanError> {
        let (slope, y_intercept) = Self::do_calculate_linear_coefficients(data)?;
        let linear_regression = |x: f64| slope * x + y_intercept;
        Ok(Workload::RecordsPerSecond(linear_regression(timestamp)))
    }

    fn do_calculate_linear_coefficients(data: &[Point]) -> Result<(f64, f64), PlanError> {
        // let xs: Vec<f64> = self.data.iter().map(|(x, _)| *x).collect();
        // let ys: Vec<f64> = self.data.iter().map(|(_, y)| *y).collect();
        //
        // let x_mean = xs.mean();
        // let y_mean = ys.mean();
        //
        // let slope_num: f64 = self.data.iter().map(|(x, y)| (x - x_mean) * (y - y_mean)).sum();
        // let slope_denom: f64 = self.data.iter().map(|(x, y)| pow(x - x_mean, 2)).sum();
        // let slope: f64 = slope_num / slope_denom;
        // let y_intercept = y_mean - slope * x_mean;

        let n = data.len() as f64;
        let (sum_x, sum_y, sum_xy, sum_xx) = data
            .iter()
            .map(|(x, y)| (x, y, x * y, x * x))
            .fold((0., 0., 0., 0.), |(acc_x, acc_y, acc_xy, acc_xx), (x, y, xy, xx)| {
                (acc_x + x, acc_y + y, acc_xy + xy, acc_xx + xx)
            });
        let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_xx - pow(sum_x, 2));
        let y_intercept = (sum_y - slope * sum_x) / n;
        Ok((slope, y_intercept))
    }

    fn do_quadratic_regression(data: &[Point], timestamp: f64) -> Result<Workload, PlanError> {
        let (a, b, c) = Self::do_calculate_quadratic_coefficients(data)?;
        let quadratic_regression = |x: f64| a * pow(x, 2) + b * x + c;
        Ok(Workload::RecordsPerSecond(quadratic_regression(timestamp)))
    }

    fn do_calculate_quadratic_coefficients(data: &[Point]) -> Result<(f64, f64, f64), PlanError> {
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

        tracing::trace!(abc=?coefficients, ?a, ?b, ?c, "solution found.");
        Ok((a, b, c))
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

    fn spline_slopes(data: &[Point]) -> Vec<f64> {
        Self::splines(data)
            .into_iter()
            .map(|((x1, y1), (x2, y2))| (y2 - y1) / (x2 - x1))
            .collect()
    }

    fn estimate_next_timestamp(data: &[Point]) -> Option<f64> {
        data.iter().last().map(|(x, y)| {
            // calculate average only if there's data.
            let avg_ts_delta = Self::splines(data)
                .into_iter()
                .map(|((x1, _), (x2, _))| x2 - x1)
                .collect::<Vec<_>>()
                .mean();
            x + avg_ts_delta
        })
    }
}

impl LeastSquaresWorkloadForecast {
    fn have_enough_data_for_prediction(&self) -> bool {
        self.data.len() < MIN_REQ_SAMPLES_FOR_PREDICTION
    }

    fn measure_spike(&mut self, observation: Point) {
        if let Some(spike) = self.spike_detector.signal(observation.1) {
            self.consecutive_spikes += 1;
        } else {
            self.consecutive_spikes = 0;
        }
        // let values: Vec<f64> = self.data.iter().map(|pt| pt.1).collect();
        // let stddev = values.clone().std_dev();
        // let mean = values.mean();
        // observation.1 < (mean - 3. * stddev) || (mean + 3. * stddev) < observation.1
    }

    #[inline]
    fn exceeded_spike_threshold(&self) -> bool {
        SPIKE_THRESHOLD <= self.consecutive_spikes
    }

    fn extrema_position(&self) -> Option<usize> {
        let data: Vec<Point> = self.data.iter().map(|pt| *pt).collect();
        let mut slopes = Self::spline_slopes(&data);
        if slopes.is_empty() {
            return None;
        }

        let first = slopes.remove(0);
        if first == 0. {
            Some(1) // position 1 represents endpoint of 2-pt spline
        } else {
            let direction = first.is_sign_positive();
            // identify first spline whose slope crosses zero
            slopes
                .into_iter()
                .enumerate()
                .find(|(_, slope)| slope.is_sign_positive() != direction)
                .map(|v| v.0 + 2) // offset by 2 since we pull first slope and spline slopes are for 2-pts
        }
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
    use crate::flink::plan::forecast::least_squares::LeastSquaresWorkloadForecast;
    use crate::flink::plan::forecast::Workload;
    use approx::{assert_abs_diff_eq, assert_relative_eq, assert_ulps_eq};
    use num_traits::pow;
    use pretty_assertions::assert_eq;
    use statrs::statistics::Statistics;

    #[test]
    fn test_quadratic_regression_coefficients() -> anyhow::Result<()> {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_quadratic_regression_coefficients");
        let _ = main_span.enter();

        // example take from https://www.symbolab.com/solver/system-of-equations-calculator/9669a%2B1225b%2B165c%3D5174.3%2C%201225a%2B165b%2B25c%3D809.7%2C%20165a%2B25b%2B5c%3D167.1#
        let data = vec![(1., 32.5), (3., 37.3), (5., 36.4), (7., 32.4), (9., 28.5)];

        let (a, b, c) = LeastSquaresWorkloadForecast::do_calculate_quadratic_coefficients(&data)?;
        assert_relative_eq!(a, (-41. / 112.), epsilon = 1.0e-10);
        assert_relative_eq!(b, (2111. / 700.), epsilon = 1.0e-10);
        assert_relative_eq!(c, (85181. / 2800.), epsilon = 1.0e-10);
        Ok(())
    }

    #[test]
    fn test_quadratic_regression_prediction() -> anyhow::Result<()> {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_quadratic_regression_prediction");
        let _ = main_span.enter();

        // example take from https://www.symbolab.com/solver/system-of-equations-calculator/9669a%2B1225b%2B165c%3D5174.3%2C%201225a%2B165b%2B25c%3D809.7%2C%20165a%2B25b%2B5c%3D167.1#
        let data = vec![(1., 32.5), (3., 37.3), (5., 36.4), (7., 32.4), (9., 28.5)];

        match LeastSquaresWorkloadForecast::do_quadratic_regression(&data, 11.)? {
            Workload::RecordsPerSecond(actual) => {
                let expected = (-41. / 112.) * pow(11., 2) + (2111. / 700.) * 11. + (85181. / 2800.);
                assert_relative_eq!(actual, expected, epsilon = 1.0e-10);
            }
            w => panic!("failed to predict via quadratic regression: {:?}", w),
        };

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

        let (actual_slope, actual_y_intercept) = LeastSquaresWorkloadForecast::do_calculate_linear_coefficients(&data)?;

        assert_relative_eq!(actual_slope, 0.8, epsilon = 1.0e-10);
        assert_relative_eq!(actual_y_intercept, 0.4, epsilon = 1.0e-10);

        Ok(())
    }

    #[test]
    fn test_linear_regression() -> anyhow::Result<()> {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_linear_regression");
        let _ = main_span.enter();

        // example taken from https://tutorme.com/blog/post/quadratic-regression/
        let data = vec![(1., 1.), (2., 3.), (3., 2.), (4., 3.), (5., 5.)];

        let accuracy = 0.69282037;

        match LeastSquaresWorkloadForecast::do_linear_regression(&data, 4.)? {
            Workload::RecordsPerSecond(actual) => {
                assert_relative_eq!(actual, 3., epsilon = accuracy);
            }

            w => panic!("failed to predict: {:?}", w),
        }

        Ok(())
    }
}
