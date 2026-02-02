//! Common types for the write coordinator.

/// A monotonically increasing identifier assigned to each write.
///
/// Epochs are used to track write ordering and determine which writes
/// have been flushed or made durable.
pub type Epoch = u64;

/// The durability level to wait for before a write operation returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Durability {
    /// The write has been applied to the in-memory delta.
    Applied,
    /// The write has been flushed to the storage layer.
    Flushed,
    /// The write has been durably persisted.
    Durable,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_order_durability_levels_correctly() {
        assert!(Durability::Applied < Durability::Flushed);
        assert!(Durability::Flushed < Durability::Durable);
        assert!(Durability::Applied < Durability::Durable);
    }

    #[test]
    fn should_compare_durability_levels_for_equality() {
        assert_eq!(Durability::Applied, Durability::Applied);
        assert_eq!(Durability::Flushed, Durability::Flushed);
        assert_eq!(Durability::Durable, Durability::Durable);

        assert_ne!(Durability::Applied, Durability::Flushed);
        assert_ne!(Durability::Flushed, Durability::Durable);
    }
}
