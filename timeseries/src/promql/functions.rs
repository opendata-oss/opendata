use std::collections::HashMap;

use super::evaluator::{EvalResult, EvalSample, EvalSamples};

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Sample;
    use std::collections::HashMap;

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
}
