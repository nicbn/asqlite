//! # asqlite - Async SQLite for Rust
//!
//! This library provides an API for accessing SQLite databases using
//! async Rust.
//!
//! It wraps `libsqlite3`, dispatching operations in a background thread.
//!
//! # Usage
//!
//! Add to your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! asqlite = { version = "1.0.0", features = [ "bundled" ] }
//! ```
//!
//! Unless you are writing a library, you probably want to enable the `bundled`
//! feature, which automatically compiles SQLite.
//! See [Cargo features](#cargo-features) for more.
//!
//! Start by creating a [`Connection`] via [`Connection::builder`].
//!
//! # Example
//!
//! ```
#![doc = include_str!("../examples/apple.rs")]
//! ```
//!
//! # Cancel safety
//!
//! All operations of this library are cancel-safe and can be used with,
//! for example, `tokio::select!`.
//!
//! # Cargo features
//!
//! * `bundled` (disabled by default): automatically compiles and statically
//!   links an up to date version of SQLite to the library. This is a very
//!   good choice for most applications.

#![allow(clippy::type_complexity)]
#![warn(missing_docs, unreachable_pub)]
#![deny(unsafe_op_in_unsafe_fn)]

#[macro_use]
mod macros;
mod blob;
mod connection;
pub mod convert;
mod error;
mod internal;
mod statement;

pub use self::{
    blob::Blob,
    connection::{Connection, ConnectionBuilder, InterruptHandle},
    error::{Error, ErrorKind},
    statement::Statement,
};

/// Alias for `Result<T, Error>`.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A zero filled binary blob.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ZeroBlob(
    /// Size of the blob.
    pub u64,
);

/// Open mode for blobs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum BlobOpenMode {
    /// Read-only.
    ReadOnly = 0,

    /// Read and write.
    ReadWrite = 1,
}
