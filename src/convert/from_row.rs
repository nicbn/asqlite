use super::{FromSql, SqlRef};
use crate::{convert::Sql, Result};
use std::fmt;

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
#[repr(transparent)]
pub struct RowReader<'a> {
    iterator: dyn ExactSizeIterator<Item = Result<SqlRef<'a>>>,
}

impl fmt::Debug for RowReader<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowReader").finish_non_exhaustive()
    }
}

impl<'a> RowReader<'a> {
    pub(crate) fn new<'b>(
        iterator: &'b mut dyn ExactSizeIterator<Item = Result<SqlRef<'a>>>,
    ) -> &'b mut Self {
        unsafe {
            &mut *(iterator as *mut dyn ExactSizeIterator<Item = Result<SqlRef<'a>>> as *mut Self)
        }
    }

    /// Read a column.
    pub fn read(&mut self) -> Result<SqlRef> {
        self.iterator.next().unwrap_or(Ok(SqlRef::Null))
    }

    /// Amount of rows.
    pub fn len(&self) -> usize {
        self.iterator.len()
    }

    /// Whether is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
