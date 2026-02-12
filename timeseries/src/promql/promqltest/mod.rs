pub mod dsl;
pub mod runner;

#[cfg(test)]
mod tests {
    use super::runner::run_test;

    include!(concat!(env!("OUT_DIR"), "/promql_tests_generated.rs"));
}
