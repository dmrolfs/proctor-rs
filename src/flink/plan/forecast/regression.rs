use std::fmt::Debug;

use chrono::{TimeZone, Utc};
use nalgebra::{Matrix3, Matrix3x1};
use num_traits::pow;

use crate::error::PlanError;
use crate::flink::plan::forecast::{Point, Workload};
use crate::flink::plan::RecordsPerSecond;

pub trait RegressionStrategy: Debug {
    fn name(&self) -> &str;
    fn calculate(&self, x: f64) -> Result<Workload, PlanError>;
    fn correlation_coefficient(&self) -> f64;
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct LinearRegression {
    pub slope: f64,
    pub y_intercept: f64,
    pub correlation_coefficient: f64,
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

        let n = data.len() as f64;
        tracing::trace!(%sum_x, %sum_y, %sum_xy, %sum_x2, %sum_y2, %n, "intermediate linear regression calculations");
        (n, sum_x, sum_y, sum_xy, sum_x2, sum_y2)
    }
}

impl RegressionStrategy for LinearRegression {
    #[inline]
    fn name(&self) -> &str {
        "LinearRegression"
    }

    #[inline]
    fn calculate(&self, x: f64) -> Result<Workload, PlanError> {
        Ok(RecordsPerSecond(self.slope * x + self.y_intercept).into())
    }

    #[inline]
    fn correlation_coefficient(&self) -> f64 {
        self.correlation_coefficient
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct QuadraticRegression {
    pub a: f64,
    pub b: f64,
    pub c: f64,
    pub correlation_coefficient: f64,
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

        tracing::trace!(%sum_x, %sum_y, %sum_x2, %sum_x3, %sum_x4, %sum_xy, %sum_x2y, %n, "intermediate quadratic regression calculations");

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

        let sst = data.iter().fold(0., |acc, (_, y)| acc + pow(y - y_mean, 2));

        let correlation_coefficient = (1. - sse / sst).sqrt();

        Ok(Self { a, b, c, correlation_coefficient })
    }
}

impl RegressionStrategy for QuadraticRegression {
    #[inline]
    fn name(&self) -> &str {
        "QuadraticRegression"
    }

    fn calculate(&self, x: f64) -> Result<Workload, PlanError> {
        Ok(Workload::RecordsInPerSecond(RecordsPerSecond(
            self.a * pow(x, 2) + self.b * x + self.c,
        )))
    }

    #[inline]
    fn correlation_coefficient(&self) -> f64 {
        self.correlation_coefficient
    }
}
