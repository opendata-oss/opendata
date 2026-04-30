use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn millis(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_millis() as i64
}
