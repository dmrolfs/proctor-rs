use std::cmp::Ordering;

use super::{SeqValue, TelemetryType, TelemetryValue};
use super::{TelemetryType as TT, TelemetryValue as TV};
use crate::error::TelemetryError;

pub trait TelemetryCombinator {
    fn combine(&self, items: Vec<TelemetryValue>) -> Result<Option<TelemetryValue>, TelemetryError>;
}

fn type_error_for(expected: TelemetryType, actual: TelemetryValue) -> TelemetryError {
    TelemetryError::TypeError { expected, actual: Some(format!("{:?}", actual)) }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct First;

impl TelemetryCombinator for First {
    fn combine(&self, items: Vec<TelemetryValue>) -> Result<Option<TelemetryValue>, TelemetryError> {
        Ok(items.into_iter().next())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Max;

impl TelemetryCombinator for Max {
    fn combine(&self, items: Vec<TelemetryValue>) -> Result<Option<TelemetryValue>, TelemetryError> {
        self.do_combine(items)
    }
}

impl DoTelemetryCombination for Max {
    fn label(&self) -> String {
        "max".to_string()
    }

    fn combo_bool_fn(&self) -> Box<dyn FnMut(bool, bool) -> Result<bool, TelemetryError>> {
        Box::new(|acc, next| Ok(acc || next))
    }

    fn combo_i64_fn(&self) -> Box<dyn FnMut(i64, i64) -> Result<i64, TelemetryError>> {
        Box::new(|acc, next| Ok(acc.max(next)))
    }

    fn combo_f64_fn(&self) -> Box<dyn FnMut(f64, f64) -> Result<f64, TelemetryError>> {
        Box::new(|acc, next| Ok(acc.max(next)))
    }

    fn combo_string_fn(&self) -> Box<dyn FnMut(String, String) -> Result<String, TelemetryError>> {
        Box::new(|acc, next| Ok(acc.max(next)))
    }

    fn combo_seq_fn(&self) -> Box<dyn FnMut(SeqValue, SeqValue) -> Result<SeqValue, TelemetryError>> {
        let label = self.label();
        Box::new(move |acc, next| {
            PartialOrd::partial_cmp(&acc, &next)
                .ok_or_else(|| {
                    TelemetryError::NotSupported(format!(
                        "{} combination not supported for seq telemetry values, which don't implement PartialOrd: \
                         {:?} and {:?}",
                        label, acc, next
                    ))
                })
                .map(|ordering| match ordering {
                    Ordering::Greater | Ordering::Equal => acc,
                    Ordering::Less => next,
                })
        })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Min;

impl TelemetryCombinator for Min {
    fn combine(&self, items: Vec<TelemetryValue>) -> Result<Option<TelemetryValue>, TelemetryError> {
        self.do_combine(items)
    }
}

impl DoTelemetryCombination for Min {
    fn label(&self) -> String {
        "min".to_string()
    }

    fn combo_bool_fn(&self) -> Box<dyn FnMut(bool, bool) -> Result<bool, TelemetryError>> {
        Box::new(|acc, next| Ok(acc && next))
    }

    fn combo_i64_fn(&self) -> Box<dyn FnMut(i64, i64) -> Result<i64, TelemetryError>> {
        Box::new(|acc, next| Ok(acc.min(next)))
    }

    fn combo_f64_fn(&self) -> Box<dyn FnMut(f64, f64) -> Result<f64, TelemetryError>> {
        Box::new(|acc, next| Ok(acc.min(next)))
    }

    fn combo_string_fn(&self) -> Box<dyn FnMut(String, String) -> Result<String, TelemetryError>> {
        Box::new(|acc, next| Ok(acc.min(next)))
    }

    fn combo_seq_fn(&self) -> Box<dyn FnMut(SeqValue, SeqValue) -> Result<SeqValue, TelemetryError>> {
        let label = self.label();
        Box::new(move |acc, next| {
            PartialOrd::partial_cmp(&acc, &next)
                .ok_or_else(|| {
                    TelemetryError::NotSupported(format!(
                        "{} combination not supported for seq telemetry values, which don't implement PartialOrd: \
                         {:?} and {:?}",
                        label, acc, next
                    ))
                })
                .map(|ordering| match ordering {
                    Ordering::Less | Ordering::Equal => acc,
                    Ordering::Greater => next,
                })
        })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Sum;

impl TelemetryCombinator for Sum {
    fn combine(&self, items: Vec<TelemetryValue>) -> Result<Option<TelemetryValue>, TelemetryError> {
        self.do_combine(items)
    }
}

impl DoTelemetryCombination for Sum {
    fn label(&self) -> String {
        "sum".to_string()
    }

    fn combo_bool_fn(&self) -> Box<dyn FnMut(bool, bool) -> Result<bool, TelemetryError>> {
        Box::new(|acc, next| Ok(acc || next))
    }

    fn combo_i64_fn(&self) -> Box<dyn FnMut(i64, i64) -> Result<i64, TelemetryError>> {
        Box::new(|acc, next| Ok(acc + next))
    }

    fn combo_f64_fn(&self) -> Box<dyn FnMut(f64, f64) -> Result<f64, TelemetryError>> {
        Box::new(|acc, next| Ok(acc + next))
    }

    fn combo_string_fn(&self) -> Box<dyn FnMut(String, String) -> Result<String, TelemetryError>> {
        Box::new(|acc, next| Ok(acc + &next))
    }

    fn combo_seq_fn(&self) -> Box<dyn FnMut(SeqValue, SeqValue) -> Result<SeqValue, TelemetryError>> {
        Box::new(|mut acc, next| {
            acc.extend(next);
            Ok(acc)
        })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Average;

impl TelemetryCombinator for Average {
    fn combine(&self, items: Vec<TelemetryValue>) -> Result<Option<TelemetryValue>, TelemetryError> {
        self.do_combine(items)
    }
}

impl DoTelemetryCombination for Average {
    fn label(&self) -> String {
        "average".to_string()
    }

    fn combo_bool_fn(&self) -> Box<dyn FnMut(bool, bool) -> Result<bool, TelemetryError>> {
        let label = self.label();
        Box::new(move |_, _| {
            Err(TelemetryError::NotSupported(format!(
                "{} combination not supported for bool telemetry values",
                label
            )))
        })
    }

    fn combo_i64_fn(&self) -> Box<dyn FnMut(i64, i64) -> Result<i64, TelemetryError>> {
        let mut count = 0;
        let mut sum = 0;
        Box::new(move |acc, next| {
            let span = tracing::debug_span!("combo_i64", enter_count=%count, enter_sum=%sum);
            let _ = span.enter();

            if count == 0 {
                sum = acc;
                count += 1;
            }

            sum += next;
            count += 1;

            tracing::debug!(%count, %sum, "combined step");
            Ok(sum / count)
        })
    }

    fn combo_f64_fn(&self) -> Box<dyn FnMut(f64, f64) -> Result<f64, TelemetryError>> {
        let mut count = 0;
        let mut sum = 0.;
        Box::new(move |acc, next| {
            if count == 0 {
                sum = acc;
                count += 1;
            }

            sum += next;
            count += 1;

            Ok(sum / count as f64)
        })
    }

    fn combo_string_fn(&self) -> Box<dyn FnMut(String, String) -> Result<String, TelemetryError>> {
        let label = self.label();
        Box::new(move |_, _| {
            Err(TelemetryError::NotSupported(format!(
                "{} combination not supported for string telemetry values",
                label
            )))
        })
    }

    fn combo_seq_fn(&self) -> Box<dyn FnMut(SeqValue, SeqValue) -> Result<SeqValue, TelemetryError>> {
        let label = self.label();
        Box::new(move |_, _| {
            Err(TelemetryError::NotSupported(format!(
                "{} combination not supported for seq telemetry values",
                label
            )))
        })
    }
}

trait DoTelemetryCombination {
    fn label(&self) -> String;

    fn combo_bool_fn(&self) -> Box<dyn FnMut(bool, bool) -> Result<bool, TelemetryError>>;
    fn combo_i64_fn(&self) -> Box<dyn FnMut(i64, i64) -> Result<i64, TelemetryError>>;
    fn combo_f64_fn(&self) -> Box<dyn FnMut(f64, f64) -> Result<f64, TelemetryError>>;
    fn combo_string_fn(&self) -> Box<dyn FnMut(String, String) -> Result<String, TelemetryError>>;
    fn combo_seq_fn(&self) -> Box<dyn FnMut(SeqValue, SeqValue) -> Result<SeqValue, TelemetryError>>;

    fn do_combine<I>(&self, items: I) -> Result<Option<TelemetryValue>, TelemetryError>
    where
        I: IntoIterator<Item = TelemetryValue>,
    {
        let mut items = items.into_iter().peekable();
        if items.peek().is_none() {
            return Ok(None);
        }
        let head = items.next().unwrap();

        if items.peek().is_none() {
            return Ok(Some(head));
        }

        let head_type = head.as_telemetry_type();
        let tail = items
            .map(|i| i.try_cast(head_type))
            .collect::<Result<Vec<TelemetryValue>, TelemetryError>>()?
            .into_iter();

        match head {
            TV::Unit => Ok(Some(TV::Unit)),
            TV::Boolean(h) => self.do_combine_bool(h, tail),
            TV::Integer(h) => self.do_combine_i64(h, tail),
            TV::Float(h) => self.do_combine_f64(h, tail),
            TV::Text(h) => self.do_combine_string(h, tail),
            TV::Seq(h) => self.do_combine_seq(h, tail),
            TV::Table(_) => Err(TelemetryError::NotSupported(format!(
                "{} combining Table telemetry values is not supported",
                self.label()
            ))),
        }
    }

    fn do_combine_bool(
        &self, head: bool, tail: impl Iterator<Item = TelemetryValue>,
    ) -> Result<Option<TelemetryValue>, TelemetryError> {
        let mut combo_fn = self.combo_bool_fn();
        tail.into_iter()
            .fold(Ok(head), |acc, next_t| {
                if let TV::Boolean(next) = next_t {
                    acc.and_then(|a| combo_fn(a, next))
                } else {
                    Err(type_error_for(TT::Boolean, next_t))
                }
            })
            .map(|acc| Some(TV::Boolean(acc)))
    }

    fn do_combine_i64(
        &self, head: i64, tail: impl Iterator<Item = TelemetryValue>,
    ) -> Result<Option<TelemetryValue>, TelemetryError> {
        let mut combo_fn = self.combo_i64_fn();
        tail.into_iter()
            .fold(Ok(head), |acc, next_t| {
                if let TV::Integer(next) = next_t {
                    acc.and_then(|a| combo_fn(a, next))
                } else {
                    Err(type_error_for(TT::Integer, next_t))
                }
            })
            .map(|acc| Some(TV::Integer(acc)))
    }

    fn do_combine_f64(
        &self, head: f64, tail: impl Iterator<Item = TelemetryValue>,
    ) -> Result<Option<TelemetryValue>, TelemetryError> {
        let mut combo_fn = self.combo_f64_fn();
        tail.into_iter()
            .fold(Ok(head), |acc, next_t| {
                if let TV::Float(next) = next_t {
                    acc.and_then(|a| combo_fn(a, next))
                } else {
                    Err(type_error_for(TT::Float, next_t))
                }
            })
            .map(|acc| Some(TV::Float(acc)))
    }

    fn do_combine_string(
        &self, head: String, tail: impl Iterator<Item = TelemetryValue>,
    ) -> Result<Option<TelemetryValue>, TelemetryError> {
        let mut combo_fn = self.combo_string_fn();
        tail.into_iter()
            .fold(Ok(head), |acc, next_t| {
                if let TV::Text(next) = next_t {
                    acc.and_then(|a| combo_fn(a, next))
                } else {
                    Err(type_error_for(TT::Text, next_t))
                }
            })
            .map(|acc| Some(TV::Text(acc)))
    }

    fn do_combine_seq(
        &self, head: SeqValue, tail: impl Iterator<Item = TelemetryValue>,
    ) -> Result<Option<TelemetryValue>, TelemetryError> {
        let mut combo_fn = self.combo_seq_fn();
        tail.into_iter()
            .fold(Ok(head), |acc, next_t| {
                if let TV::Seq(next) = next_t {
                    acc.and_then(|a| combo_fn(a, next))
                } else {
                    Err(type_error_for(TT::Seq, next_t))
                }
            })
            .map(|acc| Some(TV::Seq(acc)))
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
        let first_unit = assert_some!(assert_ok!(First.combine(vec![TV::Unit, TV::Unit])));
        assert_eq!(first_unit.as_telemetry_type(), TelemetryType::Unit);
        assert_eq!(
            assert_some!(assert_ok!(First.combine(vec![TV::Integer(33), TV::Integer(39)]))),
            TV::Integer(33)
        );
        assert_eq!(
            assert_some!(assert_ok!(First.combine(vec![
                TV::Float(std::f64::consts::PI),
                TV::Float(std::f64::consts::LN_2)
            ]))),
            TV::Float(std::f64::consts::PI)
        );
        assert_eq!(
            assert_some!(assert_ok!(First.combine(vec![
                TV::Seq(vec![TV::Integer(33), TV::Boolean(false)]),
                TV::Seq(vec![TV::Float(std::f64::consts::LN_2)]),
            ]))),
            TV::Seq(vec![TV::Integer(33), TV::Boolean(false)])
        );
        assert_eq!(
            assert_some!(assert_ok!(First.combine(vec![
                TV::Table(
                    maplit::hashmap! {
                        "foo".to_string() => TV::Integer(33),
                        "bar".to_string() => TV::Boolean(false),
                    }
                    .into()
                ),
                TV::Table(
                    maplit::hashmap! {
                        "zed".to_string() => TV::Float(std::f64::consts::LN_2),
                    }
                    .into()
                ),
            ]))),
            TV::Table(
                maplit::hashmap! {
                    "foo".to_string() => TV::Integer(33),
                    "bar".to_string() => TV::Boolean(false),
                }
                .into()
            )
        );

        assert_eq!(
            assert_some!(assert_ok!(First.combine(vec![TV::Integer(33), TV::Unit]))),
            TV::Integer(33)
        );
    }

    #[test]
    fn test_max_combination() {
        assert_eq!(
            assert_some!(assert_ok!(Max.combine(vec![TV::Unit, TV::Unit]))),
            TV::Unit
        );
        assert_eq!(
            assert_some!(assert_ok!(Max.combine(vec![TV::Integer(33), TV::Integer(39)]))),
            TV::Integer(39)
        );
        assert_eq!(
            assert_some!(assert_ok!(Max.combine(vec![
                TV::Float(std::f64::consts::PI),
                TV::Float(std::f64::consts::LN_2)
            ]))),
            TV::Float(std::f64::consts::PI)
        );
        assert_eq!(
            assert_some!(assert_ok!(Max.combine(vec![
                TV::Seq(vec![TV::Integer(33), TV::Boolean(false)]),
                TV::Seq(vec![TV::Float(std::f64::consts::LN_2)]),
            ]))),
            TV::Seq(vec![TV::Integer(33), TV::Boolean(false)])
        );
        assert_err!(Max.combine(vec![
            TV::Table(
                maplit::hashmap! {
                    "foo".to_string() => TV::Integer(33),
                    "bar".to_string() => TV::Boolean(false),
                }
                .into()
            ),
            TV::Table(
                maplit::hashmap! {
                    "zed".to_string() => TV::Float(std::f64::consts::LN_2),
                }
                .into()
            ),
        ]));

        assert_err!(Max.combine(vec![TV::Integer(33), TV::Unit]));
    }

    #[test]
    fn test_min_combination() {
        assert_eq!(
            assert_some!(assert_ok!(Min.combine(vec![TV::Unit, TV::Unit]))),
            TV::Unit
        );
        assert_eq!(
            assert_some!(assert_ok!(Min.combine(vec![TV::Integer(33), TV::Integer(39)]))),
            TV::Integer(33)
        );
        assert_eq!(
            assert_some!(assert_ok!(Min.combine(vec![
                TV::Float(std::f64::consts::PI),
                TV::Float(std::f64::consts::LN_2)
            ]))),
            TV::Float(std::f64::consts::LN_2)
        );
        assert_eq!(
            assert_some!(assert_ok!(Min.combine(vec![
                TV::Seq(vec![TV::Integer(33), TV::Boolean(false)]),
                TV::Seq(vec![TV::Float(std::f64::consts::LN_2)]),
            ]))),
            TV::Seq(vec![TV::Float(std::f64::consts::LN_2)])
        );
        assert_err!(Min.combine(vec![
            TV::Table(
                maplit::hashmap! {
                    "foo".to_string() => TV::Integer(33),
                    "bar".to_string() => TV::Boolean(false),
                }
                .into()
            ),
            TV::Table(
                maplit::hashmap! {
                    "zed".to_string() => TV::Float(std::f64::consts::LN_2),
                }
                .into()
            ),
        ]));

        assert_eq!(
            assert_some!(assert_ok!(
                Min.combine(vec![TV::Integer(33), TV::Float(std::f64::consts::E)])
            )),
            TV::Integer(std::f64::consts::E as i64)
        );
    }

    #[test]
    fn test_sum_combination() {
        assert_eq!(
            assert_some!(assert_ok!(Sum.combine(vec![TV::Unit, TV::Unit]))),
            TV::Unit
        );
        assert_eq!(
            assert_some!(assert_ok!(Sum.combine(vec![TV::Integer(33), TV::Integer(39)]))),
            TV::Integer(72)
        );
        assert_eq!(
            assert_some!(assert_ok!(Sum.combine(vec![
                TV::Float(std::f64::consts::PI),
                TV::Float(std::f64::consts::LN_2)
            ]))),
            TV::Float(std::f64::consts::PI + std::f64::consts::LN_2)
        );
        assert_eq!(
            assert_some!(assert_ok!(Sum.combine(vec![
                TV::Seq(vec![TV::Integer(33), TV::Boolean(false)]),
                TV::Seq(vec![TV::Float(std::f64::consts::LN_2)]),
            ]))),
            TV::Seq(vec![
                TV::Integer(33),
                TV::Boolean(false),
                TV::Float(std::f64::consts::LN_2)
            ])
        );
        assert_err!(Sum.combine(vec![
            TV::Table(
                maplit::hashmap! {
                    "foo".to_string() => TV::Integer(33),
                    "bar".to_string() => TV::Boolean(false),
                }
                .into()
            ),
            TV::Table(
                maplit::hashmap! {
                    "zed".to_string() => TV::Float(std::f64::consts::LN_2),
                }
                .into()
            ),
        ]));

        assert_eq!(
            assert_some!(assert_ok!(
                Sum.combine(vec![TV::Integer(33), TV::Float(std::f64::consts::E)])
            )),
            TV::Integer(33 + std::f64::consts::E as i64)
        );
    }

    #[test]
    fn test_average_combination() {
        once_cell::sync::Lazy::force(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_average_combination");
        let _main_span_guard = main_span.enter();

        assert_eq!(
            assert_some!(assert_ok!(Average.combine(vec![TV::Unit, TV::Unit]))),
            TV::Unit
        );
        assert_eq!(
            assert_some!(assert_ok!(Average.combine(vec![TV::Integer(34), TV::Integer(39)]))),
            TV::Integer(36)
        );
        assert_eq!(
            assert_some!(assert_ok!(Average.combine(vec![TV::Float(2.134), TV::Float(5.623)]))),
            TV::Float(3.8785)
        );
        assert_err!(Average.combine(vec![
            TV::Seq(vec![TV::Integer(33), TV::Boolean(false)]),
            TV::Seq(vec![TV::Float(std::f64::consts::LN_2)]),
        ]));
        assert_err!(Average.combine(vec![
            TV::Table(
                maplit::hashmap! {
                    "foo".to_string() => TV::Integer(33),
                    "bar".to_string() => TV::Boolean(false),
                }
                .into()
            ),
            TV::Table(
                maplit::hashmap! {
                    "zed".to_string() => TV::Float(std::f64::consts::LN_2),
                }
                .into()
            ),
        ]));

        assert_eq!(
            assert_some!(assert_ok!(
                Average.combine(vec![TV::Integer(33), TV::Float(std::f64::consts::E)])
            )),
            TV::Integer(17)
        );
    }
}
