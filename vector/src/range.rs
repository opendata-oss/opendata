use crate::model::AttributeValue;
use std::ops::Bound::{Excluded, Included, Unbounded};

/// A nonempty-field, typed interval over string, integer or finite float metadata.
/// At least one bound is required. Equal exclusive bounds describe an empty interval.
#[derive(Debug, Clone, PartialEq)]
pub struct Range {
    field: String,
    lower: std::ops::Bound<AttributeValue>,
    upper: std::ops::Bound<AttributeValue>,
}

impl Range {
    pub fn new(
        field: impl Into<String>,
        lower: std::ops::Bound<AttributeValue>,
        upper: std::ops::Bound<AttributeValue>,
    ) -> crate::Result<Self> {
        let field = field.into();
        fn bounded(bound: &std::ops::Bound<AttributeValue>) -> Option<&AttributeValue> {
            match bound {
                Included(value) | Excluded(value) => Some(value),
                Unbounded => None,
            }
        }
        let ordered = |value: &AttributeValue| {
            matches!(value, AttributeValue::String(_) | AttributeValue::Int64(_))
                || matches!(value, AttributeValue::Float64(value) if value.is_finite())
        };
        let first = bounded(&lower);
        let last = bounded(&upper);
        if field.is_empty()
            || first.is_none() && last.is_none()
            || first.into_iter().chain(last).any(|value| !ordered(value))
            || matches!((first, last), (Some(a), Some(b)) if compare(a, b).is_none_or(|order| order.is_gt()))
        {
            return Err(crate::Error::InvalidInput(
                "metadata range requires matching ordered bounds".into(),
            ));
        }
        Ok(Self {
            field,
            lower,
            upper,
        })
    }

    pub fn field(&self) -> &str {
        &self.field
    }
    pub fn lower(&self) -> &std::ops::Bound<AttributeValue> {
        &self.lower
    }
    pub fn upper(&self) -> &std::ops::Bound<AttributeValue> {
        &self.upper
    }

    pub fn matches(&self, value: &AttributeValue) -> bool {
        let lower = match &self.lower {
            Included(bound) => compare(value, bound).is_some_and(|order| !order.is_lt()),
            Excluded(bound) => compare(value, bound).is_some_and(|order| order.is_gt()),
            Unbounded => true,
        };
        let upper = match &self.upper {
            Included(bound) => compare(value, bound).is_some_and(|order| !order.is_gt()),
            Excluded(bound) => compare(value, bound).is_some_and(|order| order.is_lt()),
            Unbounded => true,
        };
        lower && upper
    }
}

fn compare(a: &AttributeValue, b: &AttributeValue) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (AttributeValue::String(a), AttributeValue::String(b)) => Some(a.cmp(b)),
        (AttributeValue::Int64(a), AttributeValue::Int64(b)) => Some(a.cmp(b)),
        (AttributeValue::Float64(a), AttributeValue::Float64(b))
            if a.is_finite() && b.is_finite() =>
        {
            a.partial_cmp(b)
        }
        _ => None,
    }
}
