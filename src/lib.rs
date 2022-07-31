#![doc = include_str!("lib.md")]
#![forbid(unsafe_code)]

pub mod error;
pub mod file;
pub mod read;
#[cfg(feature = "compression")]
#[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
pub mod read_async;
pub mod schema;
