// #![feature(specialization)]

use super::TelemetryValue;
use oso::PolarValue;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, LinkedList, VecDeque};

pub trait ToTelemetry {
    #[allow(clippy::wrong_self_convention)]
    fn to_telemetry(self) -> TelemetryValue;
}

// impl<C: PolarClass + Send + Sync> ToTelemetry for C {
//     fn to_telemetry(self) -> TelemetryValue {
//         let polar_value = self.to_polar();
//         polar_value.to_telemetry()
//     }
// }

// impl<T: ToTelemetry> From<T> for TelemetryValue {
//     fn from(that: T) -> Self {
//         that.to_telemetry()
//     }
// }

impl ToTelemetry for TelemetryValue {
    fn to_telemetry(self) -> TelemetryValue {
        self
    }
}

impl ToTelemetry for bool {
    fn to_telemetry(self) -> TelemetryValue {
        TelemetryValue::Boolean(self)
    }
}

impl ToTelemetry for usize {
    fn to_telemetry(self) -> TelemetryValue {
        TelemetryValue::Integer(self as i64)
    }
}

macro_rules! int_to_telemetry {
    ($i:ty) => {
        impl ToTelemetry for $i {
            fn to_telemetry(self) -> TelemetryValue {
                TelemetryValue::Integer(self.into())
            }
        }
    };
}

int_to_telemetry!(u8);
int_to_telemetry!(i8);
int_to_telemetry!(u16);
int_to_telemetry!(i16);
int_to_telemetry!(u32);
int_to_telemetry!(i32);
int_to_telemetry!(i64);

macro_rules! float_to_telemetry {
    ($i:ty) => {
        impl ToTelemetry for $i {
            fn to_telemetry(self) -> TelemetryValue {
                TelemetryValue::Float(self.into())
            }
        }
    };
}

float_to_telemetry!(f32);
float_to_telemetry!(f64);

impl ToTelemetry for String {
    fn to_telemetry(self) -> TelemetryValue {
        TelemetryValue::Text(self)
    }
}

impl<'a> ToTelemetry for &'a str {
    fn to_telemetry(self) -> TelemetryValue {
        TelemetryValue::Text(self.to_string())
    }
}

impl<T: ToTelemetry> ToTelemetry for Vec<T> {
    fn to_telemetry(self) -> TelemetryValue {
        TelemetryValue::Seq(self.into_iter().map(|v| v.to_telemetry()).collect())
    }
}

impl<T: ToTelemetry> ToTelemetry for VecDeque<T> {
    fn to_telemetry(self) -> TelemetryValue {
        TelemetryValue::Seq(self.into_iter().map(|v| v.to_telemetry()).collect())
    }
}

impl<T: ToTelemetry> ToTelemetry for LinkedList<T> {
    fn to_telemetry(self) -> TelemetryValue {
        TelemetryValue::Seq(self.into_iter().map(|v| v.to_telemetry()).collect())
    }
}

impl<T: ToTelemetry> ToTelemetry for HashSet<T> {
    fn to_telemetry(self) -> TelemetryValue {
        TelemetryValue::Seq(self.into_iter().map(|v| v.to_telemetry()).collect())
    }
}

impl<T: ToTelemetry> ToTelemetry for BTreeSet<T> {
    fn to_telemetry(self) -> TelemetryValue {
        TelemetryValue::Seq(self.into_iter().map(|v| v.to_telemetry()).collect())
    }
}

impl<T: ToTelemetry> ToTelemetry for BinaryHeap<T> {
    fn to_telemetry(self) -> TelemetryValue {
        TelemetryValue::Seq(self.into_iter().map(|v| v.to_telemetry()).collect())
    }
}

impl<'a, T: Clone + ToTelemetry> ToTelemetry for &'a [T] {
    fn to_telemetry(self) -> TelemetryValue {
        TelemetryValue::Seq(self.iter().cloned().map(|v| v.to_telemetry()).collect())
    }
}

impl<T: ToTelemetry> ToTelemetry for HashMap<String, T> {
    fn to_telemetry(self) -> TelemetryValue {
        TelemetryValue::Table(self.into_iter().map(|(k, v)| (k, v.to_telemetry())).collect())
    }
}

impl<T: ToTelemetry> ToTelemetry for HashMap<&str, T> {
    fn to_telemetry(self) -> TelemetryValue {
        TelemetryValue::Table(
            self.into_iter()
                .map(|(k, v)| (k.to_string(), v.to_telemetry()))
                .collect(),
        )
    }
}

impl<T: ToTelemetry> ToTelemetry for BTreeMap<String, T> {
    fn to_telemetry(self) -> TelemetryValue {
        TelemetryValue::Table(self.into_iter().map(|(k, v)| (k, v.to_telemetry())).collect())
    }
}

impl<T: ToTelemetry> ToTelemetry for BTreeMap<&str, T> {
    fn to_telemetry(self) -> TelemetryValue {
        TelemetryValue::Table(
            self.into_iter()
                .map(|(k, v)| (k.to_string(), v.to_telemetry()))
                .collect(),
        )
    }
}

// impl<T: ToTelemetry> ToTelemetry for Option<T> {
//     fn to_telemetry(self) -> TelemetryValue {
//         TelemetryValue::new_from_instance(self.map(|t| t.to_telemetry()))
//     }
// }

impl ToTelemetry for PolarValue {
    fn to_telemetry(self) -> TelemetryValue {
        match self {
            Self::Instance(_) => TelemetryValue::Unit,
            Self::Variable(_) => TelemetryValue::Unit,
            Self::Boolean(value) => TelemetryValue::Boolean(value),
            Self::Integer(value) => TelemetryValue::Integer(value),
            Self::Float(value) => TelemetryValue::Float(value),
            Self::String(rep) => TelemetryValue::Text(rep),
            Self::List(values) => {
                let vs = values.into_iter().map(|v| v.to_telemetry()).collect();
                TelemetryValue::Seq(vs)
            }
            Self::Map(table) => {
                let vs = table.into_iter().map(|(k, v)| (k, v.to_telemetry())).collect();
                TelemetryValue::Table(vs)
            }
        }
    }
}
