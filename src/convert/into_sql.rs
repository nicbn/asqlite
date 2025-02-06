use crate::{convert::Sql, ErrorKind, Result, ZeroBlob};
use std::{rc::Rc, sync::Arc};

/// Convert Rust types to SQLite parameters.
///
/// In cases where verifications may fail, an error can be returned.
///
/// # Default implementations
///
/// By default, the following implementations are provided:
///
/// | Rust type(s)                              | SQL type      | Infallible |
/// |-------------------------------------------|---------------|------------|
/// | `bool, i8, i16, i32, i64, u8, u16, u32`   | `INTEGER`     | Yes        |
/// | `i128, isize, u64, u128, usize`           | `INTEGER`     | No         |
/// | `f64, f32`                                | `REAL`        | Yes        |
/// | `[u8], Vec<u8>`                           | `BLOB`        | Yes        |
/// | [`ZeroBlob`]                              | `BLOB`        | Yes        |
/// | `str, String`                             | `TEXT`        | Yes        |
/// | [`()`](primitive@unit)                    | `NULL`        | Yes        |
///
/// Aditionally, [`Option<T>`] maps to either the SQL type of `T`, or `NULL`
/// if the `Option` evaluates to `None`.
///
/// The implementations for `[u8]` and `str` automatically include
/// `Arc<[u8]>`, `Box<[u8]>`, `Arc<str>` and `Box<str>`.
pub trait IntoSql {
    /// Converts into a SQL value.
    fn into_sql(self) -> Result<Sql>;
}

macro_rules! impl_transparent {
    ($($x: ty),*) => {
        $(
            impl<T> IntoSql for $x
            where
                for<'a> &'a T: IntoSql,
            {
                #[inline]
                fn into_sql(self) -> Result<Sql> {
                    (&*self).into_sql()
                }
            }
        )*
    };
}

impl_transparent!(&'_ mut T, Box<T>, Arc<T>, Rc<T>);

impl<const N: usize> IntoSql for [u8; N] {
    #[inline]
    fn into_sql(self) -> Result<Sql> {
        Ok(Sql::Blob(self.into()))
    }
}

impl IntoSql for &[u8] {
    #[inline]
    fn into_sql(self) -> Result<Sql> {
        Ok(Sql::Blob(self.into()))
    }
}

impl IntoSql for Vec<u8> {
    #[inline]
    fn into_sql(self) -> Result<Sql> {
        Ok(Sql::Blob(self))
    }
}

impl IntoSql for &'_ Vec<u8> {
    #[inline]
    fn into_sql(self) -> Result<Sql> {
        self.as_slice().into_sql()
    }
}

impl IntoSql for &str {
    #[inline]
    fn into_sql(self) -> Result<Sql> {
        Ok(Sql::Text(self.into()))
    }
}

impl IntoSql for String {
    #[inline]
    fn into_sql(self) -> Result<Sql> {
        Ok(Sql::Text(self))
    }
}

impl IntoSql for &'_ String {
    #[inline]
    fn into_sql(self) -> Result<Sql> {
        self.as_str().into_sql()
    }
}

macro_rules! impl_int_lossless {
    ($($x: ty),*) => {
        $(
            impl IntoSql for $x {
                #[inline]
                fn into_sql(self) -> Result<Sql> {
                    Ok(Sql::Int(i64::from(self)))
                }
            }

            impl IntoSql for &'_ $x {
                #[inline]
                fn into_sql(self) -> Result<Sql> {
                    (*self).into_sql()
                }
            }
        )*
    };
}

impl_int_lossless!(bool, i8, i16, i32, i64, u8, u16, u32);

macro_rules! impl_int {
    ($($x: ty),*) => {
        $(
            impl IntoSql for $x {
                #[inline]
                fn into_sql(self) -> Result<Sql> {
                    Ok(Sql::Int(i64::try_from(self).map_err(|_| {
                        ErrorKind::DatatypeMismatch
                    })?))
                }
            }

            impl IntoSql for &'_ $x {
                #[inline]
                fn into_sql(self) -> Result<Sql> {
                    (*self).into_sql()
                }
            }
        )*
    };
}

impl_int!(i128, isize, u64, u128, usize);

impl IntoSql for f64 {
    #[inline]
    fn into_sql(self) -> Result<Sql> {
        Ok(Sql::Float(self))
    }
}

impl IntoSql for f32 {
    #[inline]
    fn into_sql(self) -> Result<Sql> {
        Ok(Sql::Float(f64::from(self)))
    }
}

impl IntoSql for &'_ f64 {
    #[inline]
    fn into_sql(self) -> Result<Sql> {
        (*self).into_sql()
    }
}

impl IntoSql for &'_ f32 {
    #[inline]
    fn into_sql(self) -> Result<Sql> {
        (*self).into_sql()
    }
}

impl<T> IntoSql for Option<T>
where
    T: IntoSql,
{
    #[inline]
    fn into_sql(self) -> Result<Sql> {
        match self {
            Some(inner) => inner.into_sql(),
            None => Ok(Sql::Null),
        }
    }
}

impl<'a, T> IntoSql for &'a Option<T>
where
    &'a T: IntoSql,
{
    #[inline]
    fn into_sql(self) -> Result<Sql> {
        match self {
            Some(inner) => inner.into_sql(),
            None => Ok(Sql::Null),
        }
    }
}

impl IntoSql for () {
    #[inline]
    fn into_sql(self) -> Result<Sql> {
        Ok(Sql::Null)
    }
}

impl IntoSql for &'_ () {
    #[inline]
    fn into_sql(self) -> Result<Sql> {
        Ok(Sql::Null)
    }
}

impl IntoSql for ZeroBlob {
    #[inline]
    fn into_sql(self) -> Result<Sql> {
        Ok(Sql::ZeroBlob(self))
    }
}

impl IntoSql for &'_ ZeroBlob {
    #[inline]
    fn into_sql(self) -> Result<Sql> {
        (*self).into_sql()
    }
}
