use super::TelemetryValue;
use crate::error::GraphError;
use crate::graph::GraphResult;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, LinkedList, VecDeque};
use std::convert::TryFrom;
use std::hash::Hash;

pub trait FromTelemetry: Clone {
    fn from_telemetry(val: TelemetryValue) -> GraphResult<Self>;
}

impl FromTelemetry for TelemetryValue {
    fn from_telemetry(val: TelemetryValue) -> GraphResult<Self> {
        Ok(val)
    }
}

macro_rules! telemetry_to_int {
    ($i:ty) => {
        impl FromTelemetry for $i {
            fn from_telemetry(val: TelemetryValue) -> GraphResult<Self> {
                if let TelemetryValue::Integer(i) = val {
                    <$i>::try_from(i).map_err(|err| err.into())
                } else {
                    Err(GraphError::TypeError("Integer".to_string()))
                }
            }
        }
    };
}

telemetry_to_int!(usize);
telemetry_to_int!(u8);
telemetry_to_int!(i8);
telemetry_to_int!(u16);
telemetry_to_int!(i16);
telemetry_to_int!(u32);
telemetry_to_int!(i32);
telemetry_to_int!(i64);

impl FromTelemetry for f64 {
    fn from_telemetry(val: TelemetryValue) -> GraphResult<Self> {
        if let TelemetryValue::Float(f) = val {
            Ok(f)
        } else {
            Err(GraphError::TypeError("Float".to_string()))
        }
    }
}

impl FromTelemetry for String {
    fn from_telemetry(val: TelemetryValue) -> GraphResult<Self> {
        if let TelemetryValue::Text(rep) = val {
            Ok(rep)
        } else {
            Err(GraphError::TypeError("Text".to_string()))
        }
    }
}

impl FromTelemetry for bool {
    fn from_telemetry(val: TelemetryValue) -> GraphResult<Self> {
        if let TelemetryValue::Boolean(b) = val {
            Ok(b)
        } else {
            Err(GraphError::TypeError("Boolean".to_string()))
        }
    }
}

impl<T: FromTelemetry> FromTelemetry for HashMap<String, T> {
    fn from_telemetry(val: TelemetryValue) -> GraphResult<Self> {
        if let TelemetryValue::Map(table) = val {
            let mut result = HashMap::new();
            for (k, v) in table {
                let t = T::from_telemetry(v)?;
                result.insert(k, t);
            }
            Ok(result)
        } else {
            Err(GraphError::TypeError("Map".to_string()))
        }
    }
}

impl<T: FromTelemetry> FromTelemetry for BTreeMap<String, T> {
    fn from_telemetry(val: TelemetryValue) -> GraphResult<Self> {
        if let TelemetryValue::Map(table) = val {
            let mut result = BTreeMap::new();
            for (k, v) in table {
                let t = T::from_telemetry(v)?;
                result.insert(k, t);
            }
            Ok(result)
        } else {
            Err(GraphError::TypeError("Map".to_string()))
        }
    }
}

impl<T: FromTelemetry> FromTelemetry for Vec<T> {
    fn from_telemetry(val: TelemetryValue) -> GraphResult<Self> {
        if let TelemetryValue::List(values) = val {
            let mut result = vec![];
            for v in values {
                let t = T::from_telemetry(v)?;
                result.push(t);
            }
            Ok(result)
        } else {
            Err(GraphError::TypeError("List".to_string()))
        }
    }
}

impl<T: FromTelemetry> FromTelemetry for VecDeque<T> {
    fn from_telemetry(val: TelemetryValue) -> GraphResult<Self> {
        if let TelemetryValue::List(values) = val {
            let mut result = VecDeque::new();
            for v in values {
                let t = T::from_telemetry(v)?;
                result.push_back(t);
            }
            Ok(result)
        } else {
            Err(GraphError::TypeError("List".to_string()))
        }
    }
}

impl<T: FromTelemetry> FromTelemetry for LinkedList<T> {
    fn from_telemetry(val: TelemetryValue) -> GraphResult<Self> {
        if let TelemetryValue::List(values) = val {
            let mut result = LinkedList::new();
            for v in values {
                let t = T::from_telemetry(v)?;
                result.push_back(t);
            }
            Ok(result)
        } else {
            Err(GraphError::TypeError("List".to_string()))
        }
    }
}

impl<T: Eq + Hash + FromTelemetry> FromTelemetry for HashSet<T> {
    fn from_telemetry(val: TelemetryValue) -> GraphResult<Self> {
        if let TelemetryValue::List(values) = val {
            let mut result = HashSet::new();
            for v in values {
                let t = T::from_telemetry(v)?;
                result.insert(t);
            }
            Ok(result)
        } else {
            Err(GraphError::TypeError("List".to_string()))
        }
    }
}

impl<T: Eq + Ord + FromTelemetry> FromTelemetry for BTreeSet<T> {
    fn from_telemetry(val: TelemetryValue) -> GraphResult<Self> {
        if let TelemetryValue::List(values) = val {
            let mut result = BTreeSet::new();
            for v in values {
                let t = T::from_telemetry(v)?;
                result.insert(t);
            }
            Ok(result)
        } else {
            Err(GraphError::TypeError("List".to_string()))
        }
    }
}

impl<T: Ord + FromTelemetry> FromTelemetry for BinaryHeap<T> {
    fn from_telemetry(val: TelemetryValue) -> GraphResult<Self> {
        if let TelemetryValue::List(values) = val {
            let mut result = BinaryHeap::new();
            for v in values {
                let t = T::from_telemetry(v)?;
                result.push(t);
            }
            Ok(result)
        } else {
            Err(GraphError::TypeError("List".to_string()))
        }
    }
}

//todo replace macro with this once stable:
// impl<U: FromTelemetry> TryFrom<U> for TelemetryValue {
//     type Error = GraphError;
//     fn try_from(v: TelemetryValue) -> Result<Self, Self::Error> {
//         U::from_telemetry(v)
//     }
// }

macro_rules! try_from_telemetry {
    ($i:ty) => {
        impl TryFrom<TelemetryValue> for $i {
            type Error = GraphError;

            fn try_from(v: TelemetryValue) -> Result<Self, Self::Error> {
                Self::from_telemetry(v)
            }
        }
    };
}

try_from_telemetry!(usize);
try_from_telemetry!(u8);
try_from_telemetry!(i8);
try_from_telemetry!(u16);
try_from_telemetry!(i16);
try_from_telemetry!(u32);
try_from_telemetry!(i32);
try_from_telemetry!(i64);
try_from_telemetry!(f64);
try_from_telemetry!(String);
try_from_telemetry!(bool);

impl<T: FromTelemetry> TryFrom<TelemetryValue> for HashMap<String, T> {
    type Error = GraphError;
    fn try_from(v: TelemetryValue) -> Result<Self, Self::Error> {
        Self::from_telemetry(v)
    }
}

impl<T: FromTelemetry> TryFrom<TelemetryValue> for Vec<T> {
    type Error = GraphError;
    fn try_from(v: TelemetryValue) -> Result<Self, Self::Error> {
        Self::from_telemetry(v)
    }
}
