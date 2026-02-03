#![allow(unused)]

mod error;
mod traits;

pub use error::{WriteError, WriteResult};
pub use traits::{Delta, Durability, FlushEvent, Flusher};
