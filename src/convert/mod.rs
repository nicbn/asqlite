//! Types and traits for converting from and to SQLite data types.
//!
//! # Converting from SQLite to Rust
//!
//! The trait [`FromSql`] provides a way to convert types received from the
//! database into Rust types. Default implementations are provided, which are
//! detailed in the [`FromSql`] documentation. This trait can be implemented
//! for custom types.
//!
//! The trait [`FromRow`] is used for converting an entire row. In general
//! it is not necessary to implement this trait for more types, as it is
//! implemented for tuples of [`FromSql`] types up to a certain size.
//!
//! # Converting from Rust to SQLite
//!
//! The trait [`IntoSql`] provides a way to convert Rust types into query
//! parameters. Default implementations are provided, which are detailed in
//! the [`IntoSql`] documentation. This trait can be implemented
//! for custom types.
//!
//! The struct [`ParamList`] is a parameter list created using the
//! [`params!`] macro.
//!
//! [`SqlRef`]: crate::SqlRef
//! [`params!`]: crate::params!

mod from_row;
mod from_sql;
mod into_sql;
mod param_list;
mod statement;

use crate::ZeroBlob;

pub use self::{from_row::*, from_sql::*, into_sql::*, param_list::*, statement::*};

/// A SQLite value.
#[derive(Clone, Debug, Default)]
pub enum Sql {
    /// An integer.
    Int(i64),
    /// A float.
    Float(f64),
    /// A string.
    Text(String),
    /// A binary blob.
    ///
    /// Strings that are not UTF-8 are converted to blobs.
    Blob(Vec<u8>),
    /// A zeroed binary blob.
    ///
    /// This is never returned by SQL queries.
    ZeroBlob(ZeroBlob),
    /// A null.
    #[default]
    Null,
}

impl Sql {
    /// Borrow [`SqlRef`].
    #[inline]
    pub fn borrow(&self) -> SqlRef {
        match self {
            Self::Int(v) => SqlRef::Int(*v),
            Self::Float(v) => SqlRef::Float(*v),
            Self::Text(v) => SqlRef::Text(v),
            Self::Blob(v) => SqlRef::Blob(v),
            Self::ZeroBlob(v) => SqlRef::ZeroBlob(*v),
            Self::Null => SqlRef::Null,
        }
    }
}

/// A borrowed SQLite value.
#[derive(Clone, Copy, Debug, Default)]
pub enum SqlRef<'a> {
    /// An integer.
    Int(i64),
    /// A float.
    Float(f64),
    /// A string.
    Text(&'a str),
    /// A binary blob.
    ///
    /// Strings that are not UTF-8 are converted to blobs.
    Blob(&'a [u8]),
    /// A zeroed binary blob.
    ///
    /// This is never returned by SQL queries.
    ZeroBlob(ZeroBlob),
    /// A null.
    #[default]
    Null,
}

impl From<SqlRef<'_>> for Sql {
    fn from(value: SqlRef<'_>) -> Self {
        match value {
            SqlRef::Int(x) => Self::Int(x),
            SqlRef::Float(x) => Self::Float(x),
            SqlRef::Text(x) => Self::Text(x.to_string()),
            SqlRef::Blob(x) => Self::Blob(x.to_owned()),
            SqlRef::ZeroBlob(x) => Self::ZeroBlob(x),
            SqlRef::Null => Self::Null,
        }
    }
}

impl<'a> From<&'a Sql> for SqlRef<'a> {
    fn from(value: &'a Sql) -> Self {
        value.borrow()
    }
}
