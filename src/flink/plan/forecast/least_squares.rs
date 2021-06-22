use std::collections::VecDeque;
use std::fmt::Debug;

use statrs::statistics::Statistics;

use super::signal::SignalDetector;
use super::{Point, Workload, WorkloadForecast};
use crate::error::PlanError;
use crate::flink::plan::forecast::regression::{LinearRegression, QuadraticRegression, RegressionStrategy};
use crate::flink::MetricCatalog;

#[derive(Debug)]
pub struct LeastSquaresWorkloadForecast {
    window_size: usize,
    data: VecDeque<Point>,
    spike_detector: SignalDetector,
    consecutive_spikes: usize,
}

const OBSERVATION_WINDOW_SIZE: usize = 20;

impl Default for LeastSquaresWorkloadForecast {
    fn default() -> Self { LeastSquaresWorkloadForecast::new(OBSERVATION_WINDOW_SIZE, 5., 0.) }
}

impl LeastSquaresWorkloadForecast {
    pub fn new(window: usize, spike_threshold: f64, spike_influence: f64) -> Self {
        Self {
            window_size: window,
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

const SPIKE_THRESHOLD: usize = 3;

impl WorkloadForecast for LeastSquaresWorkloadForecast {
    #[tracing::instrument(
        level="debug",
        skip(self, metrics),
        fields(consecutive_spikes=%self.consecutive_spikes)
    )]
    fn add_observation(&mut self, metrics: MetricCatalog) {
        let observation = Self::workload_observation_from(metrics);
        self.measure_spike(observation);
        // drop up to start of spike in order to establish new prediction function
        if self.exceeded_spike_threshold() {
            let remaining = self.drop_data(..(self.data.len() - SPIKE_THRESHOLD));
            tracing::debug!(
                ?remaining,
                "exceeded spike threshold - dropping observation before spike."
            );
        }

        while self.window_size <= self.data.len() {
            self.data.pop_front();
        }
        self.data.push_back(observation);
    }

    fn clear(&mut self) {
        self.data.clear();
        self.spike_detector.clear();
        self.consecutive_spikes = 0;
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn predict_next_workload(&mut self) -> Result<Workload, PlanError> {
        if !self.have_enough_data_for_prediction() {
            return Ok(Workload::NotEnoughData);
        }

        let prediction = self.do_predict_workload()?;
        tracing::debug!(?prediction, "workload prediction calculated");
        Ok(prediction)
    }
}

impl LeastSquaresWorkloadForecast {
    fn do_predict_workload(&self) -> Result<Workload, PlanError> {
        let data: Vec<Point> = self.data.iter().copied().collect();
        if let Some(next_timestamp) = Self::estimate_next_timestamp(&data) {
            let model = Self::do_select_model(&data)?;
            model.calculate(next_timestamp)
        } else {
            Ok(Workload::NotEnoughData)
        }
    }

    #[tracing::instrument(level = "debug", skip(data))]
    fn do_select_model(data: &[Point]) -> Result<Box<dyn RegressionStrategy>, PlanError> {
        let linear = LinearRegression::from_data(&data)?;
        let quadratic = QuadraticRegression::from_data(&data)?;

        let model: Box<dyn RegressionStrategy> =
            match (linear.correlation_coefficient, quadratic.correlation_coefficient) {
                (linear_coeff, _) if linear_coeff.is_nan() => Box::new(quadratic),
                (_, quad_coeff) if quad_coeff.is_nan() => Box::new(linear),
                (l, q) if l < q => Box::new(quadratic),
                _ => Box::new(linear),
            };

        tracing::debug!(
            ?linear,
            ?quadratic,
            "selected workload prediction model: {}",
            model.name()
        );
        Ok(model)
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
        tracing::debug!(observations=%self.data.len(), required=%self.window_size, "have enough data ?= {}", self.data.len() == self.data.capacity());
        self.window_size <= self.data.len()
    }

    fn measure_spike(&mut self, observation: Point) -> usize {
        if let Some(spike) = self.spike_detector.signal(observation.1) {
            self.consecutive_spikes += 1;
            tracing::debug!(consecutive_spikes=%self.consecutive_spikes, "anomaly detected at {:?}", observation);
        } else {
            self.consecutive_spikes = 0;
        }

        self.consecutive_spikes
    }

    #[inline]
    fn exceeded_spike_threshold(&self) -> bool { SPIKE_THRESHOLD <= self.consecutive_spikes }

    fn drop_data<R>(&mut self, range: R) -> Vec<Point>
    where
        R: std::ops::RangeBounds<usize>,
    {
        self.data.drain(range).collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use approx::{assert_abs_diff_eq, assert_relative_eq, assert_ulps_eq};
    use chrono::{DateTime, TimeZone, Utc};
    use num_traits::pow;
    use pretty_assertions::assert_eq;
    use statrs::statistics::Statistics;

    use super::*;
    use crate::flink::plan::forecast::least_squares::LeastSquaresWorkloadForecast;
    use crate::flink::plan::forecast::{Point, Workload};
    use crate::flink::{FlowMetrics, UtilizationMetrics};

    #[test]
    fn test_plan_forecast_measure_spike() -> anyhow::Result<()> {
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
    fn test_plan_forecast_quadratic_regression_coefficients() -> anyhow::Result<()> {
        // example take from https://www.symbolab.com/solver/system-of-equations-calculator/9669a%2B1225b%2B165c%3D5174.3%2C%201225a%2B165b%2B25c%3D809.7%2C%20165a%2B25b%2B5c%3D167.1#
        let data = vec![(1., 32.5), (3., 37.3), (5., 36.4), (7., 32.4), (9., 28.5)];

        let actual = QuadraticRegression::from_data(&data)?;
        assert_relative_eq!(actual.a, (-41. / 112.), epsilon = 1.0e-10);
        assert_relative_eq!(actual.b, (2111. / 700.), epsilon = 1.0e-10);
        assert_relative_eq!(actual.c, (85181. / 2800.), epsilon = 1.0e-10);
        Ok(())
    }

    #[test]
    fn test_plan_forecast_quadratic_regression_prediction() -> anyhow::Result<()> {
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
    fn test_plan_forecast_estimate_next_timestamp() -> anyhow::Result<()> {
        let data = vec![(1., 32.5), (3., 37.3), (5., 36.4), (7., 32.4), (9., 28.5)];
        let actual = LeastSquaresWorkloadForecast::estimate_next_timestamp(&data).unwrap();
        assert_relative_eq!(actual, 11., epsilon = 1.0e-10);
        Ok(())
    }

    #[test]
    fn test_plan_forecast_linear_coefficients() -> anyhow::Result<()> {
        // example taken from https://tutorme.com/blog/post/quadratic-regression/
        let data = vec![(1., 1.), (2., 3.), (3., 2.), (4., 3.), (5., 5.)];
        let actual = LinearRegression::from_data(&data)?;
        assert_relative_eq!(actual.slope, 0.8, epsilon = 1.0e-10);
        assert_relative_eq!(actual.y_intercept, 0.4, epsilon = 1.0e-10);
        Ok(())
    }

    #[test]
    fn test_plan_forecast_linear_regression() -> anyhow::Result<()> {
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

    #[test]
    fn test_plan_forecast_model_selection() -> anyhow::Result<()> {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_plan_forecast_model_selection");
        let _ = main_span.enter();

        let data_1 = vec![
            (-5., 15.88),
            (-4., 12.63),
            (-3., 12.50),
            (-2., 11.78),
            (-1., 11.38),
            (0., 9.18),
            (1., 10.43),
            (2., 11.02),
            (3., 11.57),
            (4., 11.97),
        ];

        let model_1 = LeastSquaresWorkloadForecast::do_select_model(&data_1)?;
        assert_eq!(model_1.name(), "QuadraticRegression");

        let data_2 = vec![
            (1., 1.),
            (2., 2.),
            (3., 3.),
            (4., 4.),
            (5., 5.),
            (6., 6.),
            (7., 7.),
            (8., 8.),
            (9., 9.),
            (10., 10.),
        ];

        let model_2 = LeastSquaresWorkloadForecast::do_select_model(&data_2)?;
        assert_eq!(model_2.name(), "LinearRegression");

        let data_3 = vec![
            (1., 0.),
            (2., 0.),
            (3., 0.),
            (4., 0.),
            (5., 0.),
            (6., 0.),
            (7., 0.),
            (8., 0.),
            (9., 0.),
            (10., 0.),
        ];

        let model_3 = LeastSquaresWorkloadForecast::do_select_model(&data_3)?;
        assert_eq!(model_3.name(), "LinearRegression");

        let data_4 = vec![
            (1., 17.),
            (2., 17.),
            (3., 17.),
            (4., 17.),
            (5., 17.),
            (6., 17.),
            (7., 17.),
            (8., 17.),
            (9., 17.),
            (10., 17.),
        ];

        let model_4 = LeastSquaresWorkloadForecast::do_select_model(&data_4)?;
        assert_eq!(model_4.name(), "LinearRegression");

        Ok(())
    }

    fn make_metric_catalog(timestamp: DateTime<Utc>, workload: f64) -> MetricCatalog {
        MetricCatalog {
            timestamp,
            flow: FlowMetrics {
                input_messages_per_sec: workload,
                input_consumer_lag: 0.,
                records_out_per_sec: 0.,
                max_message_latency: 0.,
                net_in_utilization: 0.,
                net_out_utilization: 0.,
                sink_health_metrics: 0.,
                task_nr_records_in_per_sec: workload,
                task_nr_records_out_per_sec: 0.,
            },
            utilization: UtilizationMetrics { task_cpu_load: 0., network_io_utilization: 0. },
            custom: HashMap::default(),
        }
    }

    #[test]
    fn test_plan_forecast_predict_next_workload() -> anyhow::Result<()> {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_plan_forecast_model_selection");
        let _ = main_span.enter();

        let now: i64 = 1624061766;
        tracing::error!("NOW: {}", now);
        let step = 15;
        // let workload_expected: Vec<(f64, Option<f64>)> = vec![
        //     (15.88, None),
        //     (12.63, None),
        //     (12.50, None),
        //     (11.78, None),
        //     (11.38, None),
        //     (9.18, None),
        //     (10.43, Some(7.06637)),
        //     (11.02, Some(9.140279)),
        //     (11.57, Some(10.311776)),
        //     (11.97, Some(11.58355)),
        // ];

        let workload_expected: Vec<(f64, Option<f64>)> = vec![
            (1., None),
            (2., None),
            (3., None),
            (4., None),
            (5., None),
            (6., None),
            (7., None),
            (8., None),
            (9., None),
            (10., None),
            (11., None),
            (12., None),
            (13., None),
            (14., None),
            (15., None),
            (16., None),
            (17., None),
            (18., None),
            (19., None),
            (20., Some(21.16895)),
            (21., Some(22.22487)),
            (22., Some(23.3613)),
            (23., Some(24.12737)),
            (24., Some(25.7717)),
            (25., Some(26.5299)),
            (26., Some(27.8993)),
            (27., Some(28.4287)),
            (28., Some(29.3014)),
            (29., Some(30.159987)),
            (30., Some(31.30939)),
        ];

        let mut forecast = LeastSquaresWorkloadForecast::new(20, 5., 0.5);

        for (i, (workload, expected)) in workload_expected.into_iter().enumerate() {
            let ts = Utc.timestamp(now + (i as i64) * step, 0);
            tracing::info!("timestamp: {:?}", ts);
            let metrics = make_metric_catalog(ts, workload);
            forecast.add_observation(metrics);

            match (forecast.predict_next_workload(), expected) {
                (Ok(Workload::RecordsPerSecond(actual)), Some(e)) => {
                    tracing::info!(%actual, expected=%e, "[{}] testing workload prediction.", i);
                    assert_relative_eq!(actual, e, epsilon = 1.0e-4)
                },
                (Ok(Workload::NotEnoughData), None) => tracing::info!("[{}] okay - not enough data", i),
                (workload, Some(e)) => panic!("[{}] failed to predict:{:?} -- expected:{}", i, workload, e),
                (workload, None) => panic!("[{}] expected not enough data but calculated:{:?}", i, workload),
            }
        }

        Ok(())
    }
}
