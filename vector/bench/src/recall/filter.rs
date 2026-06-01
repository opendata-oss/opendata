//! Per-query filter construction for the recall benchmark.
//!
//! A `filter_spec` file (JSON or YAML) describes the filter shape using the
//! same [`vector::server::JsonFilter`] types the HTTP API accepts. Every
//! query applies the same shape; the literal values are resolved per query
//! row via the `@column` convention:
//!
//! - a JSON string `"@col"` binds to the query row's `col` value;
//! - a JSON string without a leading `@` is a literal string;
//! - a JSON number is a literal `Int64` (if integral) or `Float64`;
//! - a JSON bool is a literal `Bool`.
//!
//! The same rules apply to each entry of an `in` filter's `values`.

use std::collections::{HashMap, HashSet};

use anyhow::{Context, bail};
use serde_json::Value;
use vector::server::JsonFilter;
use vector::{AttributeValue, Filter};

/// Load and parse a filter spec from a JSON or YAML file. (`serde_yaml`
/// parses both, since YAML is a superset of JSON.)
pub(crate) fn load_filter_spec(path: &str) -> anyhow::Result<JsonFilter> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read filter_spec {}", path))?;
    serde_yaml::from_str(&contents).with_context(|| format!("failed to parse filter_spec {}", path))
}

/// Collect the set of query columns referenced via `@column` anywhere in the
/// filter spec, so the caller knows which columns to read from the query file.
pub(crate) fn referenced_columns(spec: &JsonFilter) -> HashSet<String> {
    let mut cols = HashSet::new();
    collect_columns(spec, &mut cols);
    cols
}

fn collect_columns(spec: &JsonFilter, out: &mut HashSet<String>) {
    fn check(v: &Value, out: &mut HashSet<String>) {
        if let Value::String(s) = v
            && let Some(col) = s.strip_prefix('@')
        {
            out.insert(col.to_string());
        }
    }
    if let Some(c) = &spec.eq {
        check(&c.value, out);
    }
    if let Some(c) = &spec.neq {
        check(&c.value, out);
    }
    if let Some(c) = &spec.r#in {
        for v in &c.values {
            check(v, out);
        }
    }
    if let Some(fs) = &spec.and {
        for f in fs {
            collect_columns(f, out);
        }
    }
    if let Some(fs) = &spec.or {
        for f in fs {
            collect_columns(f, out);
        }
    }
}

/// Build a concrete [`Filter`] for one query row. `row` maps each referenced
/// column name to that row's value.
pub(crate) fn build_filter(
    spec: &JsonFilter,
    row: &HashMap<String, AttributeValue>,
) -> anyhow::Result<Filter> {
    let mut variants = 0;
    variants += usize::from(spec.eq.is_some());
    variants += usize::from(spec.neq.is_some());
    variants += usize::from(spec.r#in.is_some());
    variants += usize::from(spec.and.is_some());
    variants += usize::from(spec.or.is_some());
    if variants != 1 {
        bail!("each filter spec node must contain exactly one operator");
    }

    if let Some(c) = &spec.eq {
        return Ok(Filter::eq(c.field.clone(), resolve_value(&c.value, row)?));
    }
    if let Some(c) = &spec.neq {
        return Ok(Filter::neq(c.field.clone(), resolve_value(&c.value, row)?));
    }
    if let Some(c) = &spec.r#in {
        let values = c
            .values
            .iter()
            .map(|v| resolve_value(v, row))
            .collect::<anyhow::Result<Vec<_>>>()?;
        return Ok(Filter::in_set(c.field.clone(), values));
    }
    if let Some(fs) = &spec.and {
        let parsed = fs
            .iter()
            .map(|f| build_filter(f, row))
            .collect::<anyhow::Result<Vec<_>>>()?;
        return Ok(Filter::and(parsed));
    }
    if let Some(fs) = &spec.or {
        let parsed = fs
            .iter()
            .map(|f| build_filter(f, row))
            .collect::<anyhow::Result<Vec<_>>>()?;
        return Ok(Filter::or(parsed));
    }
    unreachable!("variant count checked above")
}

/// Resolve a single JSON filter value to an [`AttributeValue`], applying the
/// `@column` binding convention.
fn resolve_value(
    value: &Value,
    row: &HashMap<String, AttributeValue>,
) -> anyhow::Result<AttributeValue> {
    match value {
        Value::String(s) => {
            if let Some(col) = s.strip_prefix('@') {
                row.get(col).cloned().with_context(|| {
                    format!(
                        "filter references column '@{}' but it was not loaded \
                         (or is null) for this query row",
                        col
                    )
                })
            } else {
                Ok(AttributeValue::String(s.clone()))
            }
        }
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(AttributeValue::Int64(i))
            } else if let Some(u) = n.as_u64() {
                Ok(AttributeValue::Int64(u as i64))
            } else if let Some(f) = n.as_f64() {
                Ok(AttributeValue::Float64(f))
            } else {
                bail!("unrepresentable numeric filter value: {}", n)
            }
        }
        Value::Bool(b) => Ok(AttributeValue::Bool(*b)),
        other => bail!(
            "unsupported filter value {:?}; expected string, number, or bool",
            other
        ),
    }
}
