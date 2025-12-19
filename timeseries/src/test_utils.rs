#[cfg(test)]
pub(crate) mod assertions {
    const EPSILON: f64 = 1e-10;

    /// Helper to check if two floats are approximately equal
    pub(crate) fn approx_eq(a: f64, b: f64) -> bool {
        (a - b).abs() < EPSILON || (a.is_nan() && b.is_nan())
    }

    /// Assert that two floats are approximately equal, with a helpful error message
    #[track_caller]
    pub(crate) fn assert_approx_eq(actual: f64, expected: f64) {
        assert!(
            approx_eq(actual, expected),
            "expected {}, got {}",
            expected,
            actual
        );
    }
}
