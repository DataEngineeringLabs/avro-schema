#![doc = include_str!("lib.md")]
#![forbid(unsafe_code)]

mod de;
mod schema;
mod se;
pub use schema::*;
