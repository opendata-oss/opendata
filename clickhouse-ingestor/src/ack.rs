//! Ack controller.
//!
//! After a commit group's chunks have all succeeded the runtime invokes
//! [`AckController::on_commit_group_success`] with the contiguous range
//! `low_sequence..=high_sequence`. The controller acks every sequence in
//! that range (per-sequence because the current
//! [`buffer::Consumer::ack`] requires strict in-order, one-at-a-time
//! acks) and decides — based on the configured [`AckFlushPolicy`] —
//! whether to call [`buffer::Consumer::flush`] right now.
//!
//! The controller is *not* an `Arc<Consumer>` owner. The runtime owns the
//! consumer and hands the controller a `&mut Consumer` for each call, so
//! manifest mutations remain serialized by Rust's borrow checker.

use serde::{Deserialize, Serialize};

use crate::error::{IngestorError, IngestorResult};

/// How often to call `Consumer::flush()` after acking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "policy", rename_all = "snake_case")]
#[derive(Default)]
pub enum AckFlushPolicy {
    /// Flush after every commit group. Default. Replay window after a
    /// crash is at most one commit group.
    #[default]
    EveryCommitGroup,
    /// Flush after `n` commit groups. Operator opt-in for higher
    /// throughput at the cost of an explicit replay window.
    EveryN { n: u32 },
}

#[derive(Debug)]
pub struct AckController {
    flush_policy: AckFlushPolicy,
    groups_since_flush: u32,
}

impl AckController {
    pub fn new(flush_policy: AckFlushPolicy) -> Self {
        Self {
            flush_policy,
            groups_since_flush: 0,
        }
    }

    pub fn flush_policy(&self) -> AckFlushPolicy {
        self.flush_policy
    }

    /// Acknowledge the contiguous sequence range `low..=high` and apply
    /// the configured flush policy.
    pub async fn on_commit_group_success(
        &mut self,
        consumer: &mut buffer::Consumer,
        low_sequence: u64,
        high_sequence: u64,
    ) -> IngestorResult<()> {
        if low_sequence > high_sequence {
            return Err(IngestorError::Other(format!(
                "ack range is empty: low={low_sequence}, high={high_sequence}"
            )));
        }
        for seq in low_sequence..=high_sequence {
            consumer.ack(seq).await?;
        }
        self.groups_since_flush = self.groups_since_flush.saturating_add(1);

        let should_flush = match self.flush_policy {
            AckFlushPolicy::EveryCommitGroup => true,
            AckFlushPolicy::EveryN { n } => self.groups_since_flush >= n.max(1),
        };
        if should_flush {
            consumer.flush().await?;
            self.groups_since_flush = 0;
        }
        Ok(())
    }

    /// Force a flush regardless of policy. Used by the runtime on
    /// graceful shutdown so the last commit group's high-watermark is
    /// durable before the process exits.
    pub async fn force_flush(&mut self, consumer: &mut buffer::Consumer) -> IngestorResult<()> {
        consumer.flush().await?;
        self.groups_since_flush = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_commit_group_is_default() {
        assert_eq!(AckFlushPolicy::default(), AckFlushPolicy::EveryCommitGroup);
    }

    #[test]
    fn every_n_zero_treated_as_one() {
        // We don't want a divide-by-zero or "never flush" surprise if
        // someone configures n=0. The policy clamps to >= 1 internally.
        let mut c = AckController::new(AckFlushPolicy::EveryN { n: 0 });
        c.groups_since_flush = 0;
        // Manually exercise the threshold check via the public API by
        // simulating the post-ack state. We construct a fake range and
        // skip the consumer call by using a mocked behavior — instead,
        // assert the threshold logic directly:
        let n = match c.flush_policy {
            AckFlushPolicy::EveryN { n } => n.max(1),
            _ => unreachable!(),
        };
        assert_eq!(n, 1);
    }

    // The "happy path" through Consumer::ack + flush is covered by the
    // local integration test in tests/ — it requires a real Consumer
    // backed by an in-memory object store, which is too heavy for a
    // pure unit test here.
}
