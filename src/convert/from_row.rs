use super::{FromSql, SqlRef};
use crate::{convert::Sql, Error, ErrorKind, Result};
use core::str;
use std::{ffi::c_int, fmt, slice};

/// Convert SQLite rows into Rust tuples.
///
/// This trait can be implemented for types other than tuples, if desired.
pub trait FromRow: Sized {
    /// Converts into a Rust tuple, by reading from a [`RowReader`].
    fn from_row(row: &mut RowReader) -> Result<Self>;
}

impl FromRow for Vec<Sql> {
    fn from_row(row: &mut RowReader) -> Result<Self> {
        let size = row.len();
        (0..size).map(|_| Ok(row.read()?.into())).collect()
    }
}

impl<const N: usize> FromRow for [Sql; N] {
    fn from_row(row: &mut RowReader) -> Result<Self> {
        let mut res = [const { Sql::Null }; N];
        for v in res.iter_mut() {
            *v = row.read()?.into();
        }
        Ok(res)
    }
}

macro_rules! impl_tuple {
    ( $( $x:ident ),* ) => {
        impl<$($x),*> FromRow for ($($x,)*)
        where
            $( $x: FromSql, )*
        {
            fn from_row(row: &mut RowReader) -> Result<Self> {
                Ok((
                    $( $x::from_sql(row.read()?)?, )*
                ))
            }
        }
    };
}

impl<T> FromRow for T
where
    T: FromSql,
{
    #[inline]
    fn from_row(row: &mut RowReader) -> Result<Self> {
        T::from_sql(row.read()?)
    }
}

impl_tuple!(T1);
impl_tuple!(T1, T2);
impl_tuple!(T1, T2, T3);
impl_tuple!(T1, T2, T3, T4);
impl_tuple!(T1, T2, T3, T4, T5);
impl_tuple!(T1, T2, T3, T4, T5, T6);
impl_tuple!(T1, T2, T3, T4, T5, T6, T7);
impl_tuple!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
impl_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
impl_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
impl_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
impl_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);

/// Type for reading rows.
pub struct RowReader<'a> {
    statement: &'a crate::internal::Statement,
    next: c_int,
}

impl fmt::Debug for RowReader<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowReader").finish_non_exhaustive()
    }
}

impl<'a> RowReader<'a> {
    pub(crate) fn new(statement: &'a crate::internal::Statement) -> Self {
        Self { statement, next: 0 }
    }

    /// Read a row.
    pub fn read(&mut self) -> Result<SqlRef> {
        let index = self.next;
        self.next += 1;

        match unsafe { libsqlite3_sys::sqlite3_column_type(self.statement.handle(), index) } {
            libsqlite3_sys::SQLITE_INTEGER => {
                let value =
                    unsafe { libsqlite3_sys::sqlite3_column_int64(self.statement.handle(), index) };
                Ok(SqlRef::Int(value))
            }
            libsqlite3_sys::SQLITE_FLOAT => {
                let value = unsafe {
                    libsqlite3_sys::sqlite3_column_double(self.statement.handle(), index)
                };
                Ok(SqlRef::Float(value))
            }
            libsqlite3_sys::SQLITE_TEXT => {
                let ptr =
                    unsafe { libsqlite3_sys::sqlite3_column_text(self.statement.handle(), index) };

                if ptr.is_null() {
                    return Ok(SqlRef::Text(""));
                }

                let size =
                    unsafe { libsqlite3_sys::sqlite3_column_bytes(self.statement.handle(), index) };
                let size = size.try_into().map_err(|_| ErrorKind::OutOfRange)?;

                let slice = unsafe { slice::from_raw_parts(ptr, size) };

                // If the text is not actually UTF-8, pretend it is a blob
                if let Ok(x) = str::from_utf8(slice) {
                    Ok(SqlRef::Text(x))
                } else {
                    Ok(SqlRef::Blob(slice))
                }
            }
            libsqlite3_sys::SQLITE_BLOB => {
                let ptr =
                    unsafe { libsqlite3_sys::sqlite3_column_text(self.statement.handle(), index) };

                if ptr.is_null() {
                    return Ok(SqlRef::Blob(&[]));
                }

                let size =
                    unsafe { libsqlite3_sys::sqlite3_column_bytes(self.statement.handle(), index) };
                let size = size.try_into().map_err(|_| ErrorKind::OutOfRange)?;

                Ok(SqlRef::Blob(unsafe { slice::from_raw_parts(ptr, size) }))
            }
            libsqlite3_sys::SQLITE_NULL => Ok(SqlRef::Null),
            _ => Err(Error::new(
                ErrorKind::DatatypeMismatch,
                "invalid data type".to_string(),
            )),
        }
    }

    /// Amount of rows.
    pub fn len(&self) -> usize {
        unsafe {
            libsqlite3_sys::sqlite3_column_count(self.statement.handle())
                .try_into()
                .unwrap_or(0)
        }
    }

    /// Whether is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
