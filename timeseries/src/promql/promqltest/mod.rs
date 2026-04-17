mod assert;
mod dsl;
mod evaluator;
mod loader;
pub mod runner;

#[cfg(feature = "promql-v2")]
mod v2_harness;

#[cfg(test)]
mod tests {
    use super::runner::run_test;

    include!(concat!(env!("OUT_DIR"), "/promql_tests_generated.rs"));
}
