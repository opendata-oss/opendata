use criterion::Criterion;

pub const DEFAULT_SAMPLE_SIZE: usize = 10;

pub fn default_criterion() -> Criterion {
    Criterion::default().sample_size(DEFAULT_SAMPLE_SIZE)
}
