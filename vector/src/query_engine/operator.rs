//! The `Operator` execution interface.
//!
//! Query execution is modelled as a chain of operators. Each operator produces a
//! typed result and may pull from a child operator. For example, a BM25 search runs as a
//! `project -> lookup -> bm25` chain that is driven by calling
//! [`Operator::execute`] on the root.

/// A node in the query execution chain, producing a value of type `T`.
#[async_trait::async_trait]
pub(crate) trait Operator<T> {
    async fn execute(&self) -> crate::Result<T>;
}

/// A boxed child operator.
///
/// `Send + Sync` so the enclosing query future stays `Send`: `VectorDbRead` is
/// an `#[async_trait]` trait, whose methods return boxed `Send` futures.
pub(crate) type BoxedOperator<T> = Box<dyn Operator<T> + Send + Sync>;
