use super::{TelemetryValue, Timestamp};
use approx::{AbsDiffEq, RelativeEq};
use frunk::{Monoid, Semigroup};
use oso::PolarClass;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug};
use std::iter::Sum;
use std::ops::{Add, Div, Sub};

#[derive(PolarClass, Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct RatePoint(pub Timestamp, pub RecordsPerSecond);

impl fmt::Display for RatePoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {})", self.0, self.1)
    }
}

#[derive(PolarClass, Debug, Copy, Clone, Default, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct RecordsPerSecond(f64);

impl RecordsPerSecond {
    pub const ZERO: Self = Self(0.0);

    pub const fn new(recs_per_sec: f64) -> Self {
        Self(recs_per_sec)
    }

    pub fn max(lhs: Self, rhs: Self) -> Self {
        f64::max(lhs.0, rhs.0).into()
    }

    pub fn min(lhs: Self, rhs: Self) -> Self {
        f64::min(lhs.0, rhs.0).into()
    }
}

impl fmt::Display for RecordsPerSecond {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{:.5?}_records/s", self.0))
    }
}

impl AsRef<f64> for RecordsPerSecond {
    fn as_ref(&self) -> &f64 {
        &self.0
    }
}

impl From<f64> for RecordsPerSecond {
    fn from(rate: f64) -> Self {
        Self(rate)
    }
}

impl From<RecordsPerSecond> for f64 {
    fn from(rate: RecordsPerSecond) -> Self {
        rate.0
    }
}

impl From<&RecordsPerSecond> for f64 {
    fn from(rate: &RecordsPerSecond) -> Self {
        rate.0
    }
}

impl From<RecordsPerSecond> for TelemetryValue {
    fn from(that: RecordsPerSecond) -> Self {
        Self::Float(that.0)
    }
}

impl Add for RecordsPerSecond {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl Sub for RecordsPerSecond {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self(self.0 - rhs.0)
    }
}

impl Div for RecordsPerSecond {
    type Output = f64;

    fn div(self, rhs: Self) -> Self::Output {
        self.0 / rhs.0
    }
}

impl Sum for RecordsPerSecond {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        let mut total = Self::empty();
        for item in iter {
            total = Self::combine(&total, &item);
        }
        total
    }
}

impl Monoid for RecordsPerSecond {
    fn empty() -> Self {
        Self::ZERO
    }
}

impl Semigroup for RecordsPerSecond {
    fn combine(&self, other: &Self) -> Self {
        *self + *other
    }
}

impl AbsDiffEq for RecordsPerSecond {
    type Epsilon = f64;

    fn default_epsilon() -> Self::Epsilon {
        f64::default_epsilon()
    }

    fn abs_diff_eq(&self, other: &Self, epsilon: Self::Epsilon) -> bool {
        f64::abs_diff_eq(&self.0, &other.0, epsilon)
    }
}

impl RelativeEq for RecordsPerSecond {
    fn default_max_relative() -> Self::Epsilon {
        f64::default_max_relative()
    }

    fn relative_eq(&self, other: &Self, epsilon: Self::Epsilon, max_relative: Self::Epsilon) -> bool {
        f64::relative_eq(&self.0, &other.0, epsilon, max_relative)
    }
}
