//! Crate-wide utility helpers.
use bytemuck::NoUninit;
use bytes::Bytes;

/// Owner wrapper for a `Vec<T>` exposed as its raw byte representation, so
/// we can hand ownership to `Bytes::from_owner` while preserving the natural
/// alignment of the underlying allocation.
struct AlignedBuf<T: NoUninit>(Vec<T>);

impl<T: NoUninit> AsRef<[u8]> for AlignedBuf<T> {
    fn as_ref(&self) -> &[u8] {
        bytemuck::cast_slice(&self.0)
    }
}

/// Turn a `Vec<T>` into a `Bytes` whose pointer is guaranteed to have at least
/// `align_of::<T>()` alignment (the Vec's natural allocator alignment).
pub(crate) fn aligned_bytes_from_vec<T: NoUninit + Send + 'static>(vec: Vec<T>) -> Bytes {
    Bytes::from_owner(AlignedBuf(vec))
}
