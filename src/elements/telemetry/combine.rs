use std::cmp::Ordering;

use super::TelemetryValue;
use crate::error::TelemetryError;

pub trait TelemetryCombinator {
    fn combine(&self, lhs: &TelemetryValue, rhs: &TelemetryValue) -> Result<TelemetryValue, TelemetryError>;
}

pub struct First;

impl TelemetryCombinator for First {
    fn combine(&self, lhs: &TelemetryValue, _rhs: &TelemetryValue) -> Result<TelemetryValue, TelemetryError> {
        Ok(lhs.clone())
    }
}

pub struct Max;

impl TelemetryCombinator for Max {
    fn combine(&self, lhs: &TelemetryValue, rhs: &TelemetryValue) -> Result<TelemetryValue, TelemetryError> {
        use super::TelemetryValue as TV;

        let lhs_type = lhs.as_telemetry_type();
        let rhs = rhs.clone().try_cast(lhs_type)?;
        match (lhs, rhs) {
            (TV::Unit, TV::Unit) => Err(TelemetryError::NotSupported(
                "combining Unit telemetry values is not supported".to_string(),
            )),
            (TV::Boolean(l), TV::Boolean(r)) => Ok(TV::Boolean(*l || r)),
            (TV::Integer(l), TV::Integer(r)) => Ok(TV::Integer((*l).max(r))),
            (TV::Float(l), TV::Float(r)) => Ok(TV::Float((*l).max(r))),
            (TV::Text(l), TV::Text(r)) => Ok(TV::Text(l.clone().max(r))),
            (TV::Seq(l), TV::Seq(r)) => PartialOrd::partial_cmp(l, &r)
                .ok_or(TelemetryError::NotSupported(format!(
                    "max combination not supported for seq telemetry values: {:?} and {:?}",
                    l, r
                )))
                .map(|ordering| match ordering {
                    Ordering::Greater | Ordering::Equal => TV::Seq(l.clone()),
                    Ordering::Less => TV::Seq(r),
                }),
            (TV::Table(_), TV::Table(_)) => Err(TelemetryError::NotSupported(
                "max combining Table telemetry values is not supported".to_string(),
            )),
            (l, r) => Err(TelemetryError::NotSupported(format!(
                "max combining mixed types ({}, {}) is not supported and should have been handled",
                l.as_telemetry_type(),
                r.as_telemetry_type()
            ))),
        }
    }
}

pub struct Min;

impl TelemetryCombinator for Min {
    fn combine(&self, lhs: &TelemetryValue, rhs: &TelemetryValue) -> Result<TelemetryValue, TelemetryError> {
        use super::TelemetryValue as TV;

        let lhs_type = lhs.as_telemetry_type();
        let rhs = rhs.clone().try_cast(lhs_type)?;
        match (lhs, rhs) {
            (TV::Unit, TV::Unit) => Err(TelemetryError::NotSupported(
                "combining Unit telemetry values is not supported".to_string(),
            )),
            (TV::Boolean(l), TV::Boolean(r)) => Ok(TV::Boolean(*l && r)),
            (TV::Integer(l), TV::Integer(r)) => Ok(TV::Integer((*l).min(r))),
            (TV::Float(l), TV::Float(r)) => Ok(TV::Float((*l).min(r))),
            (TV::Text(l), TV::Text(r)) => Ok(TV::Text(l.clone().min(r))),
            (TV::Seq(l), TV::Seq(r)) => PartialOrd::partial_cmp(l, &r)
                .ok_or(TelemetryError::NotSupported(format!(
                    "min combination not supported for seq telemetry values: {:?} and {:?}",
                    l, r
                )))
                .map(|ordering| match ordering {
                    Ordering::Less | Ordering::Equal => TV::Seq(l.clone()),
                    Ordering::Greater => TV::Seq(r),
                }),
            (TV::Table(_), TV::Table(_)) => Err(TelemetryError::NotSupported(
                "min combining Table telemetry values is not supported".to_string(),
            )),
            (l, r) => Err(TelemetryError::NotSupported(format!(
                "min combining mixed types ({}, {}) is not supported and should have been handled",
                l.as_telemetry_type(),
                r.as_telemetry_type()
            ))),
        }
    }
}

pub struct Sum;

impl TelemetryCombinator for Sum {
    fn combine(&self, lhs: &TelemetryValue, rhs: &TelemetryValue) -> Result<TelemetryValue, TelemetryError> {
        use super::TelemetryValue as TV;

        let lhs_type = lhs.as_telemetry_type();
        let rhs = rhs.clone().try_cast(lhs_type)?;
        match (lhs, rhs) {
            (TV::Unit, TV::Unit) => Err(TelemetryError::NotSupported(
                "combining Unit telemetry values is not supported".to_string(),
            )),
            (TV::Boolean(l), TV::Boolean(r)) => Ok(TV::Boolean(*l || r)),
            (TV::Integer(l), TV::Integer(r)) => Ok(TV::Integer(*l + r)),
            (TV::Float(l), TV::Float(r)) => Ok(TV::Float(*l + r)),
            (TV::Text(l), TV::Text(r)) => Ok(TV::Text(l.clone() + &r)),
            (TV::Seq(l), TV::Seq(r)) => {
                let mut sum = l.clone();
                sum.extend(r);
                Ok(TV::Seq(sum))
            },
            (TV::Table(_), TV::Table(_)) => Err(TelemetryError::NotSupported(
                "min combining Table telemetry values is not supported".to_string(),
            )),
            (l, r) => Err(TelemetryError::NotSupported(format!(
                "min combining mixed types ({}, {}) is not supported and should have been handled",
                l.as_telemetry_type(),
                r.as_telemetry_type()
            ))),
        }
    }
}

pub struct Average;

impl TelemetryCombinator for Average {
    fn combine(&self, lhs: &TelemetryValue, rhs: &TelemetryValue) -> Result<TelemetryValue, TelemetryError> {
        use super::TelemetryValue as TV;

        let lhs_type = lhs.as_telemetry_type();
        let rhs = rhs.clone().try_cast(lhs_type)?;
        match (lhs, rhs) {
            (TV::Unit, TV::Unit) => Err(TelemetryError::NotSupported(
                "combining Unit telemetry values is not supported".to_string(),
            )),
            (TV::Boolean(_), TV::Boolean(_)) => Err(TelemetryError::NotSupported(
                "average combining Boolean telemetry values is not supported".to_string(),
            )),
            (TV::Integer(l), TV::Integer(r)) => Ok(TV::Integer((*l + r) / 2)),
            (TV::Float(l), TV::Float(r)) => Ok(TV::Float((*l + r) / 2.)),
            (TV::Text(_), TV::Text(_)) => Err(TelemetryError::NotSupported(
                "average combining text telemetry values is not supported".to_string(),
            )),
            (TV::Seq(_), TV::Seq(_)) => Err(TelemetryError::NotSupported(
                "average combining seq telemetry values is not supported".to_string(),
            )),
            (TV::Table(_), TV::Table(_)) => Err(TelemetryError::NotSupported(
                "average combining Table telemetry values is not supported".to_string(),
            )),
            (l, r) => Err(TelemetryError::NotSupported(format!(
                "average combining mixed types ({}, {}) is not supported and should have been handled",
                l.as_telemetry_type(),
                r.as_telemetry_type()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use claim::*;
    use pretty_assertions::assert_eq;

    use super::TelemetryValue as TV;
    use super::*;
    use crate::elements::TelemetryType;

    #[test]
    fn test_first_combination() {
        let first_unit = assert_ok!(First.combine(&TV::Unit, &TV::Unit));
        assert_eq!(first_unit.as_telemetry_type(), TelemetryType::Unit);
        assert_eq!(
            assert_ok!(First.combine(&TV::Integer(33), &TV::Integer(39))),
            TV::Integer(33)
        );
        assert_eq!(
            assert_ok!(First.combine(&TV::Float(std::f64::consts::PI), &TV::Float(std::f64::consts::LN_2))),
            TV::Float(std::f64::consts::PI)
        );
        assert_eq!(
            assert_ok!(First.combine(
                &TV::Seq(vec![TV::Integer(33), TV::Boolean(false)]),
                &TV::Seq(vec![TV::Float(std::f64::consts::LN_2)])
            )),
            TV::Seq(vec![TV::Integer(33), TV::Boolean(false)])
        );
        assert_eq!(
            assert_ok!(First.combine(
                &TV::Table(
                    maplit::hashmap! {
                        "foo".to_string() => TV::Integer(33),
                        "bar".to_string() => TV::Boolean(false),
                    }
                    .into()
                ),
                &TV::Table(
                    maplit::hashmap! {
                        "zed".to_string() => TV::Float(std::f64::consts::LN_2),
                    }
                    .into()
                )
            )),
            TV::Table(
                maplit::hashmap! {
                    "foo".to_string() => TV::Integer(33),
                    "bar".to_string() => TV::Boolean(false),
                }
                .into()
            )
        );

        assert_eq!(assert_ok!(First.combine(&TV::Integer(33), &TV::Unit)), TV::Integer(33));
    }

    #[test]
    fn test_max_combination() {
        assert_err!(Max.combine(&TV::Unit, &TV::Unit));
        assert_eq!(
            assert_ok!(Max.combine(&TV::Integer(33), &TV::Integer(39))),
            TV::Integer(39)
        );
        assert_eq!(
            assert_ok!(Max.combine(&TV::Float(std::f64::consts::PI), &TV::Float(std::f64::consts::LN_2))),
            TV::Float(std::f64::consts::PI)
        );
        assert_eq!(
            assert_ok!(Max.combine(
                &TV::Seq(vec![TV::Integer(33), TV::Boolean(false)]),
                &TV::Seq(vec![TV::Float(std::f64::consts::LN_2)])
            )),
            TV::Seq(vec![TV::Integer(33), TV::Boolean(false)])
        );
        assert_err!(Max.combine(
            &TV::Table(
                maplit::hashmap! {
                    "foo".to_string() => TV::Integer(33),
                    "bar".to_string() => TV::Boolean(false),
                }
                .into()
            ),
            &TV::Table(
                maplit::hashmap! {
                    "zed".to_string() => TV::Float(std::f64::consts::LN_2),
                }
                .into()
            )
        ));

        assert_err!(Max.combine(&TV::Integer(33), &TV::Unit));
    }

    #[test]
    fn test_min_combination() {
        assert_err!(Min.combine(&TV::Unit, &TV::Unit));
        assert_eq!(
            assert_ok!(Min.combine(&TV::Integer(33), &TV::Integer(39))),
            TV::Integer(33)
        );
        assert_eq!(
            assert_ok!(Min.combine(&TV::Float(std::f64::consts::PI), &TV::Float(std::f64::consts::LN_2))),
            TV::Float(std::f64::consts::LN_2)
        );
        assert_eq!(
            assert_ok!(Min.combine(
                &TV::Seq(vec![TV::Integer(33), TV::Boolean(false)]),
                &TV::Seq(vec![TV::Float(std::f64::consts::LN_2)])
            )),
            TV::Seq(vec![TV::Float(std::f64::consts::LN_2)])
        );
        assert_err!(Min.combine(
            &TV::Table(
                maplit::hashmap! {
                    "foo".to_string() => TV::Integer(33),
                    "bar".to_string() => TV::Boolean(false),
                }
                .into()
            ),
            &TV::Table(
                maplit::hashmap! {
                    "zed".to_string() => TV::Float(std::f64::consts::LN_2),
                }
                .into()
            )
        ));

        assert_eq!(
            assert_ok!(Min.combine(&TV::Integer(33), &TV::Float(std::f64::consts::E))),
            TV::Integer(std::f64::consts::E as i64)
        );
    }

    #[test]
    fn test_sum_combination() {
        assert_err!(Sum.combine(&TV::Unit, &TV::Unit));
        assert_eq!(
            assert_ok!(Sum.combine(&TV::Integer(33), &TV::Integer(39))),
            TV::Integer(72)
        );
        assert_eq!(
            assert_ok!(Sum.combine(&TV::Float(std::f64::consts::PI), &TV::Float(std::f64::consts::LN_2))),
            TV::Float(std::f64::consts::PI + std::f64::consts::LN_2)
        );
        assert_eq!(
            assert_ok!(Sum.combine(
                &TV::Seq(vec![TV::Integer(33), TV::Boolean(false)]),
                &TV::Seq(vec![TV::Float(std::f64::consts::LN_2)])
            )),
            TV::Seq(vec![
                TV::Integer(33),
                TV::Boolean(false),
                TV::Float(std::f64::consts::LN_2)
            ])
        );
        assert_err!(Sum.combine(
            &TV::Table(
                maplit::hashmap! {
                    "foo".to_string() => TV::Integer(33),
                    "bar".to_string() => TV::Boolean(false),
                }
                .into()
            ),
            &TV::Table(
                maplit::hashmap! {
                    "zed".to_string() => TV::Float(std::f64::consts::LN_2),
                }
                .into()
            )
        ));

        assert_eq!(
            assert_ok!(Sum.combine(&TV::Integer(33), &TV::Float(std::f64::consts::E))),
            TV::Integer(33 + std::f64::consts::E as i64)
        );
    }

    #[test]
    fn test_average_combination() {
        assert_err!(Average.combine(&TV::Unit, &TV::Unit));
        assert_eq!(
            assert_ok!(Average.combine(&TV::Integer(34), &TV::Integer(39))),
            TV::Integer(36)
        );
        assert_eq!(
            assert_ok!(Average.combine(&TV::Float(2.134), &TV::Float(5.623))),
            TV::Float(3.8785)
        );
        assert_err!(Average.combine(
            &TV::Seq(vec![TV::Integer(33), TV::Boolean(false)]),
            &TV::Seq(vec![TV::Float(std::f64::consts::LN_2)])
        ));
        assert_err!(Average.combine(
            &TV::Table(
                maplit::hashmap! {
                    "foo".to_string() => TV::Integer(33),
                    "bar".to_string() => TV::Boolean(false),
                }
                .into()
            ),
            &TV::Table(
                maplit::hashmap! {
                    "zed".to_string() => TV::Float(std::f64::consts::LN_2),
                }
                .into()
            )
        ));

        assert_eq!(
            assert_ok!(Average.combine(&TV::Integer(33), &TV::Float(std::f64::consts::E))),
            TV::Integer(17)
        );
    }
}
