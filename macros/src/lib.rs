//! Procedural macros for OpenData

mod test;
mod trace;

use proc_macro::TokenStream;

/// Attribute macro for test functions that need storage
///
/// Automatically creates SlateDB-backed in-memory storage, passes it to the test,
/// and ensures proper cleanup via `close()`.
///
/// # Basic usage
///
/// ```ignore
/// #[opendata_macros::storage_test]
/// async fn my_test(storage: Arc<dyn Storage>) {
///     // test body
/// }
/// ```
///
/// # With merge operator
///
/// ```ignore
/// #[opendata_macros::storage_test(merge_operator = VectorDbMergeOperator::new(3))]
/// async fn my_test(storage: Arc<dyn Storage>) {
///     // test body
/// }
/// ```
#[proc_macro_attribute]
pub fn storage_test(args: TokenStream, input: TokenStream) -> TokenStream {
    test::storage::test_impl(args.into(), input.into()).into()
}

/// Attribute macro for instrumenting functions to participate in OpenData
/// query tracing.
///
/// Expands to `#[tracing::instrument(...)]` with a marker field
/// (`_otrace = tracing::field::Empty`) injected into `fields(...)`. The
/// Chrome trace layer in `common::tracing` uses the presence of that field
/// to discriminate trace spans from ordinary logs — ordinary `info!` /
/// `info_span!` callsites never land in the trace file.
///
/// Accepts all of `#[tracing::instrument]`'s arguments unchanged.
///
/// # Example
///
/// ```ignore
/// use opendata_macros::trace_instrument;
///
/// #[trace_instrument(skip_all, fields(limit = query.limit))]
/// async fn search(query: &Query) -> Result<Vec<Hit>> { /* ... */ }
/// ```
#[proc_macro_attribute]
pub fn trace_instrument(args: TokenStream, input: TokenStream) -> TokenStream {
    trace::trace_instrument_impl(args.into(), input.into()).into()
}
