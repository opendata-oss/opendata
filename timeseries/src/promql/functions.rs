use std::collections::HashMap;

use super::evaluator::{EvalResult, EvalSample, EvalSamples};
use crate::model::Sample;

/// Kahan summation increment with Neumaier improvement (1974)
///
/// Performs compensated summation to minimize floating-point rounding errors.
/// The Neumaier variant handles the case where the next term is larger than
/// the running sum, which the original Kahan algorithm (1965) did not address.
///
/// Returns (new_sum, new_compensation)
#[inline(never)]
// Important: do NOT inline.
// Compiler reordering of floating-point operations can cause precision loss.
// This was observed in Prometheus (issue #16714) and we lock the behavior
// to maintain IEEE-754 semantics exactly.
fn kahan_inc(inc: f64, sum: f64, c: f64) -> (f64, f64) {
    let t = sum + inc;

    let new_c = if t.is_infinite() {
        0.0
    } else if sum.abs() >= inc.abs() {
        // Neumaier improvement: swap roles when next term is larger
        c + ((sum - t) + inc)
    } else {
        c + ((inc - t) + sum)
    };

    (t, new_c)
}

/// Generic aggregator for range vector functions.
///
/// Invariant:
/// - Each input series is reduced to a single output sample at eval_timestamp_ms.
/// - Empty series are skipped (matching Prometheus behavior).
/// - Aggregation function `f` must implement PromQL float semantics exactly.
fn aggr_over_time<F>(samples: Vec<EvalSamples>, eval_timestamp_ms: i64, f: F) -> Vec<EvalSample>
where
    F: Fn(&[Sample]) -> f64,
{
    let mut result = Vec::with_capacity(samples.len());

    for series in samples {
        if series.values.is_empty() {
            continue;
        }

        let value = f(&series.values);

        result.push(EvalSample {
            timestamp_ms: eval_timestamp_ms,
            value,
            labels: series.labels,
        });
    }

    result
}

/// Average calculation matching Prometheus semantics.
///
/// Strategy:
/// 1. Use Kahan summation for numerical stability.
/// 2. If intermediate sum overflows to ±Inf, switch to incremental mean
///    to avoid poisoning the entire result.
///
/// This mirrors Prometheus' hybrid strategy and prevents overflow-induced
/// divergence while maintaining IEEE-754 parity.
fn avg_kahan(values: &[Sample]) -> f64 {
    if values.len() == 1 {
        return values[0].value;
    }

    let mut sum = values[0].value;
    let mut c = 0.0;
    let mut mean = 0.0;
    let mut incremental = false;

    for (i, sample) in values.iter().enumerate().skip(1) {
        let count = (i + 1) as f64;

        if !incremental {
            let (new_sum, new_c) = kahan_inc(sample.value, sum, c);
            if !new_sum.is_infinite() {
                sum = new_sum;
                c = new_c;
                continue;
            }

            incremental = true;
            mean = sum / (count - 1.0);
            c /= count - 1.0;
        }

        let q = (count - 1.0) / count;
        (mean, c) = kahan_inc(sample.value / count, q * mean, q * c);
    }

    if incremental {
        mean + c
    } else {
        let count = values.len() as f64;
        sum / count + c / count
    }
}

/// Trait for PromQL functions that operate on instant vectors
pub(crate) trait PromQLFunction {
    /// Apply the function to the input samples.
    /// `eval_timestamp_ms` is the evaluation timestamp in milliseconds since UNIX epoch.
    fn apply(
        &self,
        samples: Vec<EvalSample>,
        eval_timestamp_ms: i64,
    ) -> EvalResult<Vec<EvalSample>>;
}

/// Trait for PromQL functions that operate on range vectors (matrix selectors)
pub(crate) trait RangeFunction {
    /// Apply the function to range vector samples.
    /// `eval_timestamp_ms` is the evaluation timestamp in milliseconds since UNIX epoch.
    fn apply(
        &self,
        samples: Vec<EvalSamples>,
        eval_timestamp_ms: i64,
    ) -> EvalResult<Vec<EvalSample>>;
}

/// Function that applies a unary operation to each sample
struct UnaryFunction {
    op: fn(f64) -> f64,
}

impl PromQLFunction for UnaryFunction {
    fn apply(
        &self,
        mut samples: Vec<EvalSample>,
        _eval_timestamp_ms: i64,
    ) -> EvalResult<Vec<EvalSample>> {
        for sample in &mut samples {
            sample.value = (self.op)(sample.value);
        }
        Ok(samples)
    }
}

/// Function registry that maps function names to their implementations
pub(crate) struct FunctionRegistry {
    functions: HashMap<String, Box<dyn PromQLFunction>>,
    range_functions: HashMap<String, Box<dyn RangeFunction>>,
}

impl FunctionRegistry {
    pub(crate) fn new() -> Self {
        let mut functions: HashMap<String, Box<dyn PromQLFunction>> = HashMap::new();
        let mut range_functions: HashMap<String, Box<dyn RangeFunction>> = HashMap::new();

        // Mathematical functions
        functions.insert("abs".to_string(), Box::new(UnaryFunction { op: f64::abs }));
        functions.insert(
            "acos".to_string(),
            Box::new(UnaryFunction { op: f64::acos }),
        );
        functions.insert(
            "acosh".to_string(),
            Box::new(UnaryFunction { op: f64::acosh }),
        );
        functions.insert(
            "asin".to_string(),
            Box::new(UnaryFunction { op: f64::asin }),
        );
        functions.insert(
            "asinh".to_string(),
            Box::new(UnaryFunction { op: f64::asinh }),
        );
        functions.insert(
            "atan".to_string(),
            Box::new(UnaryFunction { op: f64::atan }),
        );
        functions.insert(
            "atanh".to_string(),
            Box::new(UnaryFunction { op: f64::atanh }),
        );
        functions.insert(
            "ceil".to_string(),
            Box::new(UnaryFunction { op: f64::ceil }),
        );
        functions.insert("cos".to_string(), Box::new(UnaryFunction { op: f64::cos }));
        functions.insert(
            "cosh".to_string(),
            Box::new(UnaryFunction { op: f64::cosh }),
        );
        functions.insert(
            "deg".to_string(),
            Box::new(UnaryFunction {
                op: f64::to_degrees,
            }),
        );
        functions.insert("exp".to_string(), Box::new(UnaryFunction { op: f64::exp }));
        functions.insert(
            "floor".to_string(),
            Box::new(UnaryFunction { op: f64::floor }),
        );
        functions.insert("ln".to_string(), Box::new(UnaryFunction { op: f64::ln }));
        functions.insert(
            "log10".to_string(),
            Box::new(UnaryFunction { op: f64::log10 }),
        );
        functions.insert(
            "log2".to_string(),
            Box::new(UnaryFunction { op: f64::log2 }),
        );
        functions.insert(
            "rad".to_string(),
            Box::new(UnaryFunction {
                op: f64::to_radians,
            }),
        );
        functions.insert(
            "round".to_string(),
            Box::new(UnaryFunction { op: f64::round }),
        );
        functions.insert("sin".to_string(), Box::new(UnaryFunction { op: f64::sin }));
        functions.insert(
            "sinh".to_string(),
            Box::new(UnaryFunction { op: f64::sinh }),
        );
        functions.insert(
            "sqrt".to_string(),
            Box::new(UnaryFunction { op: f64::sqrt }),
        );
        functions.insert("tan".to_string(), Box::new(UnaryFunction { op: f64::tan }));
        functions.insert(
            "tanh".to_string(),
            Box::new(UnaryFunction { op: f64::tanh }),
        );

        // Special functions
        functions.insert("absent".to_string(), Box::new(AbsentFunction));
        functions.insert("scalar".to_string(), Box::new(ScalarFunction));

        // Range vector functions
        range_functions.insert("rate".to_string(), Box::new(RateFunction));
        range_functions.insert("sum_over_time".to_string(), Box::new(SumOverTimeFunction));
        range_functions.insert("avg_over_time".to_string(), Box::new(AvgOverTimeFunction));
        range_functions.insert("min_over_time".to_string(), Box::new(MinOverTimeFunction));
        range_functions.insert("max_over_time".to_string(), Box::new(MaxOverTimeFunction));
        range_functions.insert(
            "count_over_time".to_string(),
            Box::new(CountOverTimeFunction),
        );
        range_functions.insert(
            "stddev_over_time".to_string(),
            Box::new(StddevOverTimeFunction),
        );
        range_functions.insert(
            "stdvar_over_time".to_string(),
            Box::new(StdvarOverTimeFunction),
        );

        Self {
            functions,
            range_functions,
        }
    }

    pub(crate) fn get(&self, name: &str) -> Option<&dyn PromQLFunction> {
        self.functions.get(name).map(|f| f.as_ref())
    }

    pub(crate) fn get_range_function(&self, name: &str) -> Option<&dyn RangeFunction> {
        self.range_functions.get(name).map(|f| f.as_ref())
    }
}

/// Absent function: returns 1.0 if input is empty, empty vector otherwise
struct AbsentFunction;

impl PromQLFunction for AbsentFunction {
    fn apply(
        &self,
        samples: Vec<EvalSample>,
        eval_timestamp_ms: i64,
    ) -> EvalResult<Vec<EvalSample>> {
        if samples.is_empty() {
            // Return a single sample with value 1.0 at the evaluation timestamp
            Ok(vec![EvalSample {
                timestamp_ms: eval_timestamp_ms,
                value: 1.0,
                labels: HashMap::new(),
            }])
        } else {
            // Return empty vector when input has samples
            Ok(vec![])
        }
    }
}

/// Scalar function: converts single-element vector to scalar (returns as-is or empty)
struct ScalarFunction;

impl PromQLFunction for ScalarFunction {
    fn apply(
        &self,
        samples: Vec<EvalSample>,
        _eval_timestamp_ms: i64,
    ) -> EvalResult<Vec<EvalSample>> {
        if samples.len() == 1 {
            // Return the single sample (scalar converts single-element vector to scalar)
            Ok(samples)
        } else {
            // Return empty vector if input doesn't have exactly one element
            Ok(vec![])
        }
    }
}

/// Rate function: calculates per-second rate of change for range vectors
struct RateFunction;

impl RangeFunction for RateFunction {
    fn apply(
        &self,
        samples: Vec<EvalSamples>,
        eval_timestamp_ms: i64,
    ) -> EvalResult<Vec<EvalSample>> {
        // TODO(rohan): handle counter resets
        // TODO(rohan): implement extrapolation
        let mut result = Vec::with_capacity(samples.len());

        for sample_series in samples {
            if sample_series.values.len() < 2 {
                continue;
            }

            let first = &sample_series.values[0];
            let last = &sample_series.values[sample_series.values.len() - 1];

            let time_diff_seconds = (last.timestamp_ms - first.timestamp_ms) as f64 / 1000.0;

            if time_diff_seconds <= 0.0 {
                continue;
            }

            let value_diff = last.value - first.value;

            let rate = value_diff / time_diff_seconds;

            let rate = if rate < 0.0 { 0.0 } else { rate };

            result.push(EvalSample {
                timestamp_ms: eval_timestamp_ms,
                value: rate,
                labels: sample_series.labels,
            });
        }

        Ok(result)
    }
}

/// Sum over time function: sums all sample values in the range
/// Uses Kahan summation for numerical stability
/// TODO: Add histogram support when histogram types are implemented
struct SumOverTimeFunction;

impl RangeFunction for SumOverTimeFunction {
    fn apply(
        &self,
        samples: Vec<EvalSamples>,
        eval_timestamp_ms: i64,
    ) -> EvalResult<Vec<EvalSample>> {
        Ok(aggr_over_time(samples, eval_timestamp_ms, |values| {
            let mut sum = 0.0;
            let mut c = 0.0;
            for sample in values {
                (sum, c) = kahan_inc(sample.value, sum, c);
            }
            // If sum is infinite, return it directly without compensation
            if sum.is_infinite() { sum } else { sum + c }
        }))
    }
}

/// Average over time function: averages all sample values in the range
/// Uses hybrid approach: direct mean with Kahan summation, switching to incremental mean on overflow
/// TODO: Add histogram support when histogram types are implemented
struct AvgOverTimeFunction;

impl RangeFunction for AvgOverTimeFunction {
    fn apply(
        &self,
        samples: Vec<EvalSamples>,
        eval_timestamp_ms: i64,
    ) -> EvalResult<Vec<EvalSample>> {
        Ok(aggr_over_time(samples, eval_timestamp_ms, avg_kahan))
    }
}

// NOTE ON NaN HANDLING:
//
// Prometheus does NOT use simple f64::min/max semantics.
// It uses explicit comparisons to ensure:
//   - Real numbers replace NaN
//   - All-NaN input returns NaN
//
// We mirror that behavior exactly for semantic parity.

/// Min over time.
///
/// IMPORTANT:
/// We intentionally do NOT use `f64::min` or a fold with +inf.
///
/// Prometheus semantics:
/// - If the first value is NaN and later values are real numbers,
///   NaN is replaced by the first real number.
/// - If all values are NaN, result must remain NaN.
///
/// A naive fold starting from +inf would incorrectly return +inf
/// for all-NaN input. This manual loop preserves exact PromQL behavior.
struct MinOverTimeFunction;

impl RangeFunction for MinOverTimeFunction {
    fn apply(
        &self,
        samples: Vec<EvalSamples>,
        eval_timestamp_ms: i64,
    ) -> EvalResult<Vec<EvalSample>> {
        Ok(aggr_over_time(samples, eval_timestamp_ms, |values| {
            let mut min_val = values[0].value;
            for sample in values.iter().skip(1) {
                let cur = sample.value;
                if cur < min_val || min_val.is_nan() {
                    min_val = cur;
                }
            }
            min_val
        }))
    }
}

/// Max over time.
///
/// IMPORTANT:
/// We intentionally do NOT use `f64::max` or a fold with -inf.
///
/// Prometheus semantics:
/// - NaN is replaced by any subsequent real value.
/// - If all values are NaN, result must remain NaN.
///
/// A naive fold starting from -inf would incorrectly return -inf
/// for all-NaN input. This manual loop guarantees semantic parity
/// with Prometheus.
struct MaxOverTimeFunction;

impl RangeFunction for MaxOverTimeFunction {
    fn apply(
        &self,
        samples: Vec<EvalSamples>,
        eval_timestamp_ms: i64,
    ) -> EvalResult<Vec<EvalSample>> {
        Ok(aggr_over_time(samples, eval_timestamp_ms, |values| {
            let mut max_val = values[0].value;
            for sample in values.iter().skip(1) {
                let cur = sample.value;
                if cur > max_val || max_val.is_nan() {
                    max_val = cur;
                }
            }
            max_val
        }))
    }
}

/// Count over time function: counts number of samples in the range
/// TODO: Add histogram support - Prometheus counts both floats and histograms
struct CountOverTimeFunction;

impl RangeFunction for CountOverTimeFunction {
    fn apply(
        &self,
        samples: Vec<EvalSamples>,
        eval_timestamp_ms: i64,
    ) -> EvalResult<Vec<EvalSample>> {
        Ok(aggr_over_time(samples, eval_timestamp_ms, |values| {
            values.len() as f64
        }))
    }
}

/// Variance calculation using Welford's online algorithm (1962)
/// with compensated summation for improved numerical stability.
///
/// Algorithm:
///   For each value x:
///     count += 1
///     delta  = x - mean
///     mean  += delta / count
///     delta2 = x - mean
///     M2    += delta * delta2
///   variance = M2 / count   (population variance)
///
/// Enhancement:
///   Kahan compensated summation is applied to the incremental
///   updates of both the running mean and M2 accumulators,
///   reducing floating-point rounding error in long sequences.
///
/// Semantics:
///   - Computes population variance (divides by n)
///   - Matches Prometheus population variance semantics
///
/// NaN handling:
///   - Empty input returns NaN
///   - Single value returns 0.0
///   - NaN values propagate through the calculation
///
/// References:
///   - https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
///   - Prometheus: `promql/functions.go::varianceOverTime`
fn variance_kahan(values: &[Sample]) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }

    let mut count = 0.0;
    let mut mean = 0.0;
    let mut c_mean = 0.0;
    let mut m2 = 0.0;
    let mut c_m2 = 0.0;

    for sample in values {
        count += 1.0;
        let delta = sample.value - (mean + c_mean);
        (mean, c_mean) = kahan_inc(delta / count, mean, c_mean);
        let new_delta = sample.value - (mean + c_mean);
        (m2, c_m2) = kahan_inc(delta * new_delta, m2, c_m2);
    }

    (m2 + c_m2) / count
}

/// Standard deviation over time function: population stddev of all sample values
/// Only operates on float samples; histogram samples are ignored
struct StddevOverTimeFunction;

impl RangeFunction for StddevOverTimeFunction {
    fn apply(
        &self,
        samples: Vec<EvalSamples>,
        eval_timestamp_ms: i64,
    ) -> EvalResult<Vec<EvalSample>> {
        Ok(aggr_over_time(samples, eval_timestamp_ms, |values| {
            variance_kahan(values).sqrt()
        }))
    }
}

/// Standard variance over time function: population variance of all sample values
/// Only operates on float samples; histogram samples are ignored
struct StdvarOverTimeFunction;

impl RangeFunction for StdvarOverTimeFunction {
    fn apply(
        &self,
        samples: Vec<EvalSamples>,
        eval_timestamp_ms: i64,
    ) -> EvalResult<Vec<EvalSample>> {
        Ok(aggr_over_time(samples, eval_timestamp_ms, variance_kahan))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Sample;
    use rstest::rstest;
    use std::collections::HashMap;

    // ========================================================================
    // Test helpers
    // ========================================================================

    /// Converts f64 values to Sample structs with sequential timestamps
    fn test_samples(values: &[f64]) -> Vec<Sample> {
        values
            .iter()
            .enumerate()
            .map(|(i, &v)| Sample {
                timestamp_ms: i as i64,
                value: v,
            })
            .collect()
    }

    /// Relative error allowed for sample values (matches Prometheus defaultEpsilon)
    const DEFAULT_EPSILON: f64 = 0.000001;

    /// Compare two floats with tolerance.
    ///
    /// Handles StaleNaN, NaN, exact equality, near-zero, and relative tolerance.
    fn almost_equal(a: f64, b: f64, epsilon: f64) -> bool {
        use crate::model::is_stale_nan;
        const MIN_NORMAL: f64 = f64::MIN_POSITIVE;

        if is_stale_nan(a) || is_stale_nan(b) {
            return is_stale_nan(a) && is_stale_nan(b);
        }

        if a.is_nan() && b.is_nan() {
            return true;
        }

        if a == b {
            return true;
        }

        let abs_sum = a.abs() + b.abs();
        let diff = (a - b).abs();

        if a == 0.0 || b == 0.0 || abs_sum < MIN_NORMAL {
            return diff < epsilon * MIN_NORMAL;
        }

        diff / abs_sum.min(f64::MAX) < epsilon
    }

    // ========================================================================
    // Tests for variance_kahan
    // ========================================================================

    fn create_sample(value: f64) -> EvalSample {
        EvalSample {
            timestamp_ms: 1000,
            value,
            labels: HashMap::new(),
        }
    }

    #[test]
    fn should_apply_abs_function() {
        let registry = FunctionRegistry::new();
        let func = registry.get("abs").unwrap();

        let samples = vec![create_sample(-5.0), create_sample(3.0)];
        let result = func.apply(samples, 1000).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].value, 5.0);
        assert_eq!(result[1].value, 3.0);
    }

    #[test]
    fn should_apply_absent_function() {
        let registry = FunctionRegistry::new();
        let func = registry.get("absent").unwrap();

        let eval_timestamp_ms = 5000i64;

        // Empty input should return one sample with value 1.0 at eval timestamp
        let result = func.apply(vec![], eval_timestamp_ms).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 1.0);
        assert_eq!(result[0].timestamp_ms, eval_timestamp_ms);

        // Non-empty input should return empty
        let result = func
            .apply(vec![create_sample(42.0)], eval_timestamp_ms)
            .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn should_apply_scalar_function() {
        let registry = FunctionRegistry::new();
        let func = registry.get("scalar").unwrap();

        // Single element should be returned
        let result = func.apply(vec![create_sample(42.0)], 1000).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 42.0);

        // Zero or multiple elements should return empty
        let result = func.apply(vec![], 1000).unwrap();
        assert!(result.is_empty());

        let result = func
            .apply(vec![create_sample(1.0), create_sample(2.0)], 1000)
            .unwrap();
        assert!(result.is_empty());
    }

    fn create_eval_samples(
        values: Vec<(i64, f64)>,
        labels: HashMap<String, String>,
    ) -> EvalSamples {
        let values = values
            .into_iter()
            .map(|(t, v)| Sample::new(t, v))
            .collect::<Vec<_>>();
        EvalSamples { values, labels }
    }

    #[test]
    fn should_apply_rate_function() {
        let registry = FunctionRegistry::new();
        let func = registry.get_range_function("rate").unwrap();

        let mut labels = HashMap::new();
        labels.insert("job".to_string(), "test".to_string());

        // Create sample series with increasing counter values
        let samples = vec![create_eval_samples(
            vec![
                (1000, 100.0), // t=1s, value=100
                (2000, 110.0), // t=2s, value=110
                (3000, 125.0), // t=3s, value=125
            ],
            labels.clone(),
        )];

        let result = func.apply(samples, 3000).unwrap();

        assert_eq!(result.len(), 1);
        // Rate = (125 - 100) / (3000 - 1000) * 1000 = 25 / 2 = 12.5 per second
        assert_eq!(result[0].value, 12.5);
        assert_eq!(result[0].timestamp_ms, 3000);
        assert_eq!(result[0].labels, labels);
    }

    #[test]
    fn should_apply_sum_over_time_function() {
        // given
        let registry = FunctionRegistry::new();
        let func = registry.get_range_function("sum_over_time").unwrap();

        let samples = vec![create_eval_samples(
            vec![(1000, 1.0), (2000, 2.0), (3000, 3.0)],
            HashMap::new(),
        )];

        // when
        let result = func.apply(samples, 3000).unwrap();

        // then
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 6.0); // 1 + 2 + 3
    }

    #[test]
    fn should_apply_avg_over_time_function() {
        // given
        let registry = FunctionRegistry::new();
        let func = registry.get_range_function("avg_over_time").unwrap();

        let samples = vec![create_eval_samples(
            vec![(1000, 10.0), (2000, 20.0), (3000, 30.0)],
            HashMap::new(),
        )];

        // when
        let result = func.apply(samples, 3000).unwrap();

        // then
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 20.0); // (10 + 20 + 30) / 3
    }

    #[test]
    fn should_apply_min_over_time_function() {
        // given
        let registry = FunctionRegistry::new();
        let func = registry.get_range_function("min_over_time").unwrap();

        let samples = vec![create_eval_samples(
            vec![(1000, 10.0), (2000, 5.0), (3000, 30.0)],
            HashMap::new(),
        )];

        // when
        let result = func.apply(samples, 3000).unwrap();

        // then
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 5.0);
    }

    #[test]
    fn should_apply_max_over_time_function() {
        // given
        let registry = FunctionRegistry::new();
        let func = registry.get_range_function("max_over_time").unwrap();

        let samples = vec![create_eval_samples(
            vec![(1000, 10.0), (2000, 50.0), (3000, 30.0)],
            HashMap::new(),
        )];

        // when
        let result = func.apply(samples, 3000).unwrap();

        // then
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 50.0);
    }

    #[test]
    fn should_apply_count_over_time_function() {
        // given
        let registry = FunctionRegistry::new();
        let func = registry.get_range_function("count_over_time").unwrap();

        let samples = vec![create_eval_samples(
            vec![(1000, 10.0), (2000, 20.0), (3000, 30.0), (4000, 40.0)],
            HashMap::new(),
        )];

        // when
        let result = func.apply(samples, 4000).unwrap();

        // then
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 4.0);
    }

    #[test]
    fn should_apply_stddev_over_time_function() {
        // given
        let registry = FunctionRegistry::new();
        let func = registry.get_range_function("stddev_over_time").unwrap();

        // Values: [10, 20, 30, 40]
        // Mean: 25
        // Variance: ((10-25)^2 + (20-25)^2 + (30-25)^2 + (40-25)^2) / 4 = (225 + 25 + 25 + 225) / 4 = 125
        // Stddev: sqrt(125) ≈ 11.180339887498949
        let samples = vec![create_eval_samples(
            vec![(1000, 10.0), (2000, 20.0), (3000, 30.0), (4000, 40.0)],
            HashMap::new(),
        )];

        // when
        let result = func.apply(samples, 4000).unwrap();

        // then
        assert_eq!(result.len(), 1);
        assert!((result[0].value - 11.180339887498949).abs() < 1e-10);
    }

    #[test]
    fn should_apply_stdvar_over_time_function() {
        // given
        let registry = FunctionRegistry::new();
        let func = registry.get_range_function("stdvar_over_time").unwrap();

        // Values: [10, 20, 30, 40]
        // Mean: 25
        // Variance: 125
        let samples = vec![create_eval_samples(
            vec![(1000, 10.0), (2000, 20.0), (3000, 30.0), (4000, 40.0)],
            HashMap::new(),
        )];

        // when
        let result = func.apply(samples, 4000).unwrap();

        // then
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 125.0);
    }

    #[test]
    fn should_return_zero_for_stddev_with_single_value() {
        // given
        let registry = FunctionRegistry::new();
        let func = registry.get_range_function("stddev_over_time").unwrap();

        let samples = vec![create_eval_samples(vec![(1000, 42.0)], HashMap::new())];

        // when
        let result = func.apply(samples, 1000).unwrap();

        // then
        assert_eq!(result.len(), 1);
        // Single value has variance 0, stddev 0
        assert_eq!(result[0].value, 0.0);
    }

    #[test]
    fn should_skip_empty_series_for_stddev() {
        // given
        let registry = FunctionRegistry::new();
        let func = registry.get_range_function("stddev_over_time").unwrap();

        let samples = vec![create_eval_samples(vec![], HashMap::new())];

        // when
        let result = func.apply(samples, 1000).unwrap();

        // then
        // Empty series are skipped (Prometheus behavior)
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn should_skip_empty_series_for_stdvar() {
        // given
        let registry = FunctionRegistry::new();
        let func = registry.get_range_function("stdvar_over_time").unwrap();

        let samples = vec![create_eval_samples(vec![], HashMap::new())];

        // when
        let result = func.apply(samples, 1000).unwrap();

        // then
        // Empty series are skipped (Prometheus behavior)
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn variance_kahan_empty_returns_nan() {
        assert!(variance_kahan(&[]).is_nan());
    }

    #[test]
    fn variance_kahan_single_value_returns_zero() {
        let result = variance_kahan(&test_samples(&[42.0]));
        assert!(almost_equal(result, 0.0, 1e-6));
    }

    #[rstest]
    #[case(&[10.0, 20.0, 30.0, 40.0], 125.0)]
    #[case(&[5.0, 5.0, 5.0, 5.0], 0.0)]
    #[case(&[1.0, 2.0], 0.25)]
    #[case(&[1.0, 2.0, 3.0, 4.0, 5.0], 2.0)]
    fn variance_kahan_fixed_vectors(#[case] values: &[f64], #[case] expected: f64) {
        let result = variance_kahan(&test_samples(values));
        assert!(
            almost_equal(result, expected, 1e-6),
            "Expected {}, got {}",
            expected,
            result
        );
    }

    #[test]
    fn variance_kahan_numerical_stability_stress() {
        // Large base + small deltas: base=1e10, values [base+0, base+1, base+2, base+3]
        // Expected from delta-space variance ([0,1,2,3]) = 1.25
        let base = 1e10;
        let samples = test_samples(&[base, base + 1.0, base + 2.0, base + 3.0]);
        let result = variance_kahan(&samples);
        assert!(
            almost_equal(result, 1.25, 1e-6),
            "Expected 1.25, got {}",
            result
        );
    }

    #[test]
    fn variance_kahan_vs_two_pass_oracle() {
        // Test against independent two-pass algorithm (inlined to prevent misuse)
        let samples = test_samples(&[10.0, 20.0, 30.0, 40.0]);

        let welford_result = variance_kahan(&samples);

        // Two-pass oracle (inlined)
        let mean = samples.iter().map(|s| s.value).sum::<f64>() / samples.len() as f64;
        let two_pass_result = samples
            .iter()
            .map(|s| (s.value - mean).powi(2))
            .sum::<f64>()
            / samples.len() as f64;

        assert!(
            almost_equal(welford_result, two_pass_result, 1e-6),
            "Welford: {}, Two-pass: {}",
            welford_result,
            two_pass_result
        );
    }

    #[test]
    fn variance_kahan_nan_propagation() {
        assert!(variance_kahan(&test_samples(&[1.0, f64::NAN, 3.0])).is_nan());
    }

    #[test]
    fn should_handle_constant_values_in_stddev() {
        // given
        let registry = FunctionRegistry::new();
        let func = registry.get_range_function("stddev_over_time").unwrap();

        let samples = vec![create_eval_samples(
            vec![(1000, 5.0), (2000, 5.0), (3000, 5.0), (4000, 5.0)],
            HashMap::new(),
        )];

        // when
        let result = func.apply(samples, 4000).unwrap();

        // then
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 0.0); // All values same = zero variance
    }

    #[test]
    fn should_handle_large_magnitude_small_variance_in_stddev() {
        // given
        let registry = FunctionRegistry::new();
        let func = registry.get_range_function("stddev_over_time").unwrap();

        // Large magnitude values (1e10) with small variance
        // At 1e16, floating point precision limits prevent accurate small variance calculation
        // Values: [1e10, 1e10 + 1, 1e10 + 2, 1e10 + 3]
        // Mean: 1e10 + 1.5
        // Variance: ((0-1.5)^2 + (1-1.5)^2 + (2-1.5)^2 + (3-1.5)^2) / 4 = 1.25
        // Stddev: sqrt(1.25) ≈ 1.118033988749895
        let base = 1e10;
        let samples = vec![create_eval_samples(
            vec![
                (1000, base),
                (2000, base + 1.0),
                (3000, base + 2.0),
                (4000, base + 3.0),
            ],
            HashMap::new(),
        )];

        // when
        let result = func.apply(samples, 4000).unwrap();

        // then
        assert_eq!(result.len(), 1);
        // Kahan summation should maintain reasonable precision
        let expected_stddev = 1.118033988749895;
        let rel_error = ((result[0].value - expected_stddev) / expected_stddev).abs();
        assert!(
            rel_error < 1e-6,
            "stddev should be reasonably accurate for large magnitude with small variance: computed={}, expected={}, rel_error={}",
            result[0].value,
            expected_stddev,
            rel_error
        );
    }

    #[test]
    fn should_propagate_nan_in_stddev() {
        // given
        let registry = FunctionRegistry::new();
        let func = registry.get_range_function("stddev_over_time").unwrap();

        let samples = vec![create_eval_samples(
            vec![(1000, 10.0), (2000, f64::NAN), (3000, 30.0)],
            HashMap::new(),
        )];

        // when
        let result = func.apply(samples, 3000).unwrap();

        // then
        assert_eq!(result.len(), 1);
        assert!(
            result[0].value.is_nan(),
            "NaN should propagate through stddev calculation"
        );
    }

    #[test]
    fn should_propagate_nan_in_stdvar() {
        // given
        let registry = FunctionRegistry::new();
        let func = registry.get_range_function("stdvar_over_time").unwrap();

        let samples = vec![create_eval_samples(
            vec![(1000, 10.0), (2000, f64::NAN), (3000, 30.0)],
            HashMap::new(),
        )];

        // when
        let result = func.apply(samples, 3000).unwrap();

        // then
        assert_eq!(result.len(), 1);
        assert!(
            result[0].value.is_nan(),
            "NaN should propagate through stdvar calculation"
        );
    }

    #[test]
    fn should_handle_counter_reset_in_rate() {
        let registry = FunctionRegistry::new();
        let func = registry.get_range_function("rate").unwrap();

        let labels = HashMap::new();

        // Create sample series with counter reset (value goes down)
        let samples = vec![create_eval_samples(
            vec![
                (1000, 100.0), // t=1s, value=100
                (2000, 50.0),  // t=2s, value=50 (counter reset)
            ],
            labels,
        )];

        let result = func.apply(samples, 2000).unwrap();

        assert_eq!(result.len(), 1);
        // Rate should be 0 for negative differences (counter resets)
        assert_eq!(result[0].value, 0.0);
    }

    #[test]
    fn should_skip_series_with_insufficient_samples() {
        let registry = FunctionRegistry::new();
        let func = registry.get_range_function("rate").unwrap();

        // Create sample series with only one point
        let samples = vec![create_eval_samples(vec![(1000, 100.0)], HashMap::new())];

        let result = func.apply(samples, 1000).unwrap();

        // Should return empty result for insufficient samples
        assert!(result.is_empty());
    }

    #[test]
    fn should_handle_catastrophic_cancellation_in_sum() {
        // Test Kahan summation handles catastrophic cancellation
        let values = vec![
            Sample {
                timestamp_ms: 0,
                value: 1e16,
            },
            Sample {
                timestamp_ms: 1,
                value: 1.0,
            },
            Sample {
                timestamp_ms: 2,
                value: -1e16,
            },
        ];

        let naive: f64 = values.iter().map(|s| s.value).sum();

        let mut sum = 0.0;
        let mut c = 0.0;
        for sample in &values {
            (sum, c) = kahan_inc(sample.value, sum, c);
        }
        let kahan = sum + c;

        // Naive sum loses precision due to catastrophic cancellation
        // Kahan summation preserves the 1.0
        assert!((kahan - 1.0).abs() < 1e-9, "kahan={}, expected=1.0", kahan);
        assert!(
            (naive - 1.0).abs() > 1e-9,
            "naive={}, should lose precision",
            naive
        );
    }

    #[test]
    fn should_match_prometheus_kahan_bits() {
        // Bitwise exact match with Prometheus Go implementation.
        //
        // Generated using the following Go harness:
        //
        // package main
        //
        // import (
        //     "fmt"
        //     "math"
        //     "github.com/prometheus/prometheus/util/kahansum"
        // )
        //
        // func main() {
        //     values := []float64{1e16, 1.0, -1e16}
        //
        //     sum, c := 0.0, 0.0
        //     for _, v := range values {
        //         sum, c = kahansum.Inc(v, sum, c)
        //     }
        //
        //     result := sum + c
        //     fmt.Println(math.Float64bits(result))
        // }
        //
        // Output:
        // 4607182418800017408
        //
        // This locks Rust behavior to Prometheus' exact IEEE-754 bit pattern.
        // Any future compiler or refactor drift will be caught immediately.
        let values = vec![
            Sample {
                timestamp_ms: 0,
                value: 1e16,
            },
            Sample {
                timestamp_ms: 1,
                value: 1.0,
            },
            Sample {
                timestamp_ms: 2,
                value: -1e16,
            },
        ];

        let mut sum = 0.0;
        let mut c = 0.0;
        for sample in &values {
            (sum, c) = kahan_inc(sample.value, sum, c);
        }
        let result = sum + c;

        // Expected bits generated from Go harness
        let expected_bits: u64 = 4607182418800017408;

        assert_eq!(
            result.to_bits(),
            expected_bits,
            "result={}, bits={}, expected_bits={}",
            result,
            result.to_bits(),
            expected_bits
        );
    }

    #[rstest]
    #[case(vec![1e16, 1.0, -1e16], 1.0, "catastrophic cancellation")]
    #[case(vec![1e10, 1.0, 1.0, 1.0, 1.0, 1.0, -1e10], 5.0, "small values lost in large sum")]
    #[case(vec![1e8, 1.0, -1e8, 1.0, 1e8, 1.0, -1e8], 3.0, "alternating magnitudes")]
    #[case(vec![1.0, 1e100, 1.0, -1e100], 2.0, "Neumaier improvement case")]
    #[case(vec![0.1; 10], 1.0, "repeated small values")]
    #[case(vec![1e10, 1e5, 1e0, 1e-5, 1e-10], 1e10 + 1e5 + 1.0 + 1e-5 + 1e-10, "decreasing magnitude")]
    #[case(vec![1e-10, 1e-5, 1e0, 1e5, 1e10], 1e10 + 1e5 + 1.0 + 1e-5 + 1e-10, "increasing magnitude")]
    #[case(vec![1e16, -1e16, 1e16, -1e16, 1.0], 1.0, "near-zero with large intermediates")]
    #[case(vec![1e-100; 1000], 1e-97, "very small repeated values")]
    #[case(vec![1.0, -2.0, 3.0, -4.0, 5.0], 3.0, "mixed signs")]
    fn should_handle_kahan_edge_cases(
        #[case] values: Vec<f64>,
        #[case] expected: f64,
        #[case] description: &str,
    ) {
        let mut sum = 0.0;
        let mut c = 0.0;
        for &val in &values {
            (sum, c) = kahan_inc(val, sum, c);
        }
        let result = sum + c;

        // Use relative error for large values, absolute error for small values
        let tolerance = if expected.abs() > 1.0 {
            expected.abs() * 1e-10
        } else {
            1e-10
        };

        assert!(
            (result - expected).abs() <= tolerance,
            "Failed case '{}': expected {}, got {}, error {}",
            description,
            expected,
            result,
            (result - expected).abs()
        );
    }

    #[test]
    fn should_handle_nan_in_max_over_time() {
        // Test that NaN is replaced by subsequent values (Prometheus behavior)
        let registry = FunctionRegistry::new();
        let func = registry.get_range_function("max_over_time").unwrap();

        let samples = vec![create_eval_samples(
            vec![(1000, f64::NAN), (2000, 5.0)],
            HashMap::new(),
        )];

        let result = func.apply(samples, 2000).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 5.0, "NaN should be replaced by 5.0");
    }

    #[test]
    fn should_handle_nan_in_min_over_time() {
        // Test that NaN is replaced by subsequent values (Prometheus behavior)
        let registry = FunctionRegistry::new();
        let func = registry.get_range_function("min_over_time").unwrap();

        let samples = vec![create_eval_samples(
            vec![(1000, f64::NAN), (2000, 5.0)],
            HashMap::new(),
        )];

        let result = func.apply(samples, 2000).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 5.0, "NaN should be replaced by 5.0");
    }

    #[test]
    fn should_match_prometheus_all_nan_max() {
        // Test that all-NaN returns NaN (not -inf from fold)
        let registry = FunctionRegistry::new();
        let func = registry.get_range_function("max_over_time").unwrap();

        let samples = vec![create_eval_samples(
            vec![(1000, f64::NAN), (2000, f64::NAN)],
            HashMap::new(),
        )];

        let result = func.apply(samples, 2000).unwrap();

        assert_eq!(result.len(), 1);
        assert!(
            result[0].value.is_nan(),
            "All-NaN should return NaN, got {}",
            result[0].value
        );
    }

    #[test]
    fn should_match_prometheus_all_nan_min() {
        // Test that all-NaN returns NaN (not +inf from fold)
        let registry = FunctionRegistry::new();
        let func = registry.get_range_function("min_over_time").unwrap();

        let samples = vec![create_eval_samples(
            vec![(1000, f64::NAN), (2000, f64::NAN)],
            HashMap::new(),
        )];

        let result = func.apply(samples, 2000).unwrap();

        assert_eq!(result.len(), 1);
        assert!(
            result[0].value.is_nan(),
            "All-NaN should return NaN, got {}",
            result[0].value
        );
    }

    // Property-based tests using proptest
    mod proptests {
        use super::*;
        use dashu_float::{FBig, round::mode::HalfEven};
        use proptest::prelude::*;

        /// Generate finite f64 values (no NaN, no infinity)
        fn finite_f64() -> impl Strategy<Value = f64> {
            prop::num::f64::NORMAL
        }

        /// Compute high-precision sum using arbitrary precision arithmetic (oracle)
        fn oracle_sum_high_precision(values: &[f64]) -> f64 {
            let mut sum = FBig::<HalfEven>::ZERO.with_precision(256).value();
            for &val in values {
                sum += FBig::try_from(val).unwrap();
            }
            sum.to_f64().value()
        }

        /// Compute sum of absolute values for error bound
        fn sum_abs(values: &[f64]) -> f64 {
            let mut sum = FBig::<HalfEven>::ZERO.with_precision(256).value();
            for &val in values {
                sum += FBig::try_from(val.abs()).unwrap();
            }
            sum.to_f64().value()
        }

        proptest! {
            /// Test that sum_over_time satisfies Kahan error bound:
            /// |computed_sum - true_sum| ≤ 2ε · Σ|xᵢ| + O(nε²)
            ///
            /// We use arbitrary precision arithmetic as the oracle for true_sum.
            /// See: https://en.wikipedia.org/wiki/Kahan_summation_algorithm#Accuracy
            #[test]
            fn sum_over_time_satisfies_kahan_error_bound(
                values in prop::collection::vec(finite_f64(), 1..100)
            ) {
                let registry = FunctionRegistry::new();
                let func = registry.get_range_function("sum_over_time").unwrap();

                let samples = vec![create_eval_samples(
                    values.iter().enumerate().map(|(i, &v)| ((i as i64) * 1000, v)).collect(),
                    HashMap::new(),
                )];

                let result = func.apply(samples, 0).unwrap();
                let computed_sum = result[0].value;
                let true_sum = oracle_sum_high_precision(&values);

                // Skip if overflow occurred (error bound doesn't apply)
                if computed_sum.is_infinite() || true_sum.is_infinite() {
                    return Ok(());
                }

                let error = (computed_sum - true_sum).abs();
                let sum_of_abs = sum_abs(&values);
                let n = values.len() as f64;

                // Kahan error bound: |computed - true| ≤ 2ε · Σ|xᵢ| + O(nε²)
                // We use a slightly relaxed bound to account for the O(nε²) term
                let epsilon = f64::EPSILON;
                let error_bound = 2.0 * epsilon * sum_of_abs + n * epsilon * epsilon * sum_of_abs;

                prop_assert!(
                    error <= error_bound,
                    "Kahan error bound violated: error={}, bound={}, computed={}, true={}, n={}, Σ|xᵢ|={}",
                    error, error_bound, computed_sum, true_sum, n, sum_of_abs
                );
            }

            /// Test that avg_over_time satisfies error bound (derived from sum)
            #[test]
            fn avg_over_time_satisfies_error_bound(
                values in prop::collection::vec(finite_f64(), 1..100)
            ) {
                let registry = FunctionRegistry::new();
                let func = registry.get_range_function("avg_over_time").unwrap();

                let samples = vec![create_eval_samples(
                    values.iter().enumerate().map(|(i, &v)| ((i as i64) * 1000, v)).collect(),
                    HashMap::new(),
                )];

                let result = func.apply(samples, 0).unwrap();
                let computed_avg = result[0].value;
                let true_sum = oracle_sum_high_precision(&values);
                let true_avg = true_sum / (values.len() as f64);

                // Skip if overflow occurred
                if computed_avg.is_infinite() || true_avg.is_infinite() {
                    return Ok(());
                }

                let error = (computed_avg - true_avg).abs();
                let sum_of_abs = sum_abs(&values);
                let n = values.len() as f64;

                // Error bound for average: divide sum error bound by n
                let epsilon = f64::EPSILON;
                let error_bound = (2.0 * epsilon * sum_of_abs + n * epsilon * epsilon * sum_of_abs) / n;

                prop_assert!(
                    error <= error_bound,
                    "avg_over_time error bound violated: error={}, bound={}, computed={}, true={}, n={}",
                    error, error_bound, computed_avg, true_avg, n
                );
            }

            /// Test that min_over_time returns the actual minimum
            #[test]
            fn min_over_time_returns_minimum(
                values in prop::collection::vec(finite_f64(), 1..100)
            ) {
                let registry = FunctionRegistry::new();
                let func = registry.get_range_function("min_over_time").unwrap();

                let samples = vec![create_eval_samples(
                    values.iter().enumerate().map(|(i, &v)| ((i as i64) * 1000, v)).collect(),
                    HashMap::new(),
                )];

                let result = func.apply(samples, 0).unwrap();
                let computed_min = result[0].value;
                let expected_min = values.iter().copied().fold(f64::INFINITY, f64::min);

                assert_eq!(computed_min, expected_min, "min_over_time should return exact minimum");
            }

            /// Test that max_over_time returns the actual maximum
            #[test]
            fn max_over_time_returns_maximum(
                values in prop::collection::vec(finite_f64(), 1..100)
            ) {
                let registry = FunctionRegistry::new();
                let func = registry.get_range_function("max_over_time").unwrap();

                let samples = vec![create_eval_samples(
                    values.iter().enumerate().map(|(i, &v)| ((i as i64) * 1000, v)).collect(),
                    HashMap::new(),
                )];

                let result = func.apply(samples, 0).unwrap();
                let computed_max = result[0].value;
                let expected_max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);

                assert_eq!(computed_max, expected_max, "max_over_time should return exact maximum");
            }

            /// Test that count_over_time returns the correct count
            #[test]
            fn count_over_time_returns_count(
                values in prop::collection::vec(finite_f64(), 1..100)
            ) {
                let registry = FunctionRegistry::new();
                let func = registry.get_range_function("count_over_time").unwrap();

                let samples = vec![create_eval_samples(
                    values.iter().enumerate().map(|(i, &v)| ((i as i64) * 1000, v)).collect(),
                    HashMap::new(),
                )];

                let result = func.apply(samples, 0).unwrap();
                let computed_count = result[0].value;
                let expected_count = values.len() as f64;

                assert_eq!(computed_count, expected_count, "count_over_time should return exact count");
            }

            /// Test that stddev_over_time computes correct standard deviation
            #[test]
            fn stddev_over_time_computes_correctly(
                values in prop::collection::vec(finite_f64(), 2..100)
            ) {
                let registry = FunctionRegistry::new();
                let func = registry.get_range_function("stddev_over_time").unwrap();

                let eval_samples = vec![create_eval_samples(
                    values.iter().enumerate().map(|(i, &v)| ((i as i64) * 1000, v)).collect(),
                    HashMap::new(),
                )];

                let result = func.apply(eval_samples, 0).unwrap();
                let computed_stddev = result[0].value;

                // Independent two-pass algorithm (stable baseline)
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
                let expected_stddev = variance.sqrt();

                // Skip if overflow occurred
                if computed_stddev.is_infinite() || expected_stddev.is_infinite() {
                    return Ok(());
                }

                // Allow small relative error due to numerical differences
                let rel_error = ((computed_stddev - expected_stddev) / expected_stddev.max(1e-10)).abs();
                prop_assert!(
                    rel_error < 1e-10 || (computed_stddev - expected_stddev).abs() < 1e-10,
                    "stddev_over_time error too large: computed={}, expected={}, rel_error={}",
                    computed_stddev, expected_stddev, rel_error
                );
            }

            /// Test that stdvar_over_time computes correct variance
            #[test]
            fn stdvar_over_time_computes_correctly(
                values in prop::collection::vec(finite_f64(), 2..100)
            ) {
                let registry = FunctionRegistry::new();
                let func = registry.get_range_function("stdvar_over_time").unwrap();

                let eval_samples = vec![create_eval_samples(
                    values.iter().enumerate().map(|(i, &v)| ((i as i64) * 1000, v)).collect(),
                    HashMap::new(),
                )];

                let result = func.apply(eval_samples, 0).unwrap();
                let computed_variance = result[0].value;

                // Independent two-pass algorithm (stable baseline)
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let expected_variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;

                // Skip if overflow occurred
                if computed_variance.is_infinite() || expected_variance.is_infinite() {
                    return Ok(());
                }

                // Allow small relative error due to numerical differences
                let rel_error = ((computed_variance - expected_variance) / expected_variance.max(1e-10)).abs();
                prop_assert!(
                    rel_error < 1e-10 || (computed_variance - expected_variance).abs() < 1e-10,
                    "stdvar_over_time error too large: computed={}, expected={}, rel_error={}",
                    computed_variance, expected_variance, rel_error
                );
            }

            /// Test stddev with extremely close values (catastrophic cancellation scenario)
            /// Values are base ± small_delta where base >> small_delta
            #[test]
            fn stddev_handles_extremely_close_values(
                base in 1e10_f64..1e14_f64,  // Limit base to avoid extreme precision loss
                small_deltas in prop::collection::vec(-10.0_f64..10.0_f64, 3..20)  // At least 3 values
            ) {
                let registry = FunctionRegistry::new();
                let func = registry.get_range_function("stddev_over_time").unwrap();

                // Create values: base + delta for each delta
                let values: Vec<f64> = small_deltas.iter().map(|&d| base + d).collect();

                let samples = vec![create_eval_samples(
                    values.iter().enumerate().map(|(i, &v)| ((i as i64) * 1000, v)).collect(),
                    HashMap::new(),
                )];

                let result = func.apply(samples, 0).unwrap();
                let computed_stddev = result[0].value;

                // Compute expected stddev from the deltas (more numerically stable)
                let delta_mean = small_deltas.iter().sum::<f64>() / small_deltas.len() as f64;
                let delta_variance = small_deltas.iter().map(|d| (d - delta_mean).powi(2)).sum::<f64>() / small_deltas.len() as f64;
                let expected_stddev = delta_variance.sqrt();

                // Skip if overflow occurred or variance is too small
                if computed_stddev.is_infinite() || expected_stddev.is_infinite() || expected_stddev < 1e-10 {
                    return Ok(());
                }

                // For catastrophic cancellation scenarios, Welford's algorithm should maintain
                // reasonable accuracy. We expect relative error < 1% for well-conditioned cases.
                // The condition number is roughly base/stddev, so we scale tolerance accordingly.
                let condition_number = base / expected_stddev.max(1.0);
                let tolerance = if condition_number > 1e12 {
                    0.1  // Very ill-conditioned: 10% tolerance
                } else if condition_number > 1e10 {
                    0.01  // Ill-conditioned: 1% tolerance
                } else {
                    0.001  // Well-conditioned: 0.1% tolerance
                };

                let rel_error = ((computed_stddev - expected_stddev) / expected_stddev).abs();
                prop_assert!(
                    rel_error < tolerance,
                    "stddev_over_time failed for extremely close values: base={}, computed={}, expected={}, rel_error={}, tolerance={}, condition_number={}",
                    base, computed_stddev, expected_stddev, rel_error, tolerance, condition_number
                );
            }
        }
    }
}
