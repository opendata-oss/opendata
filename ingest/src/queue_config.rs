#[derive(Clone)]
pub struct ConsumerConfig {
    pub manifest_path: String,
    pub heartbeat_timeout_ms: i64,
    pub done_cleanup_threshold: usize,
}
