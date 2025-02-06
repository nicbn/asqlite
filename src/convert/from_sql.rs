use crate::{convert::SqlRef, ErrorKind, Result};
use std::{str, iter, sync::Arc};

/// Convert SQLite values into Rust types.
///
/// In cases where verifications may fail, an error can be returned.
///
/// # Default implementations
///
/// The default implementation automatically converts data types when
/// necessary. Because of this, any SQLite type can be converted to any
/// Rust type; however, the conversion may fail at runtime due to
/// verifications.
///
/// If `T` can be used as [`FromSql`], so can [`Option<T>`]. In this case,
/// `None` is returned if the type is `NULL`.
/// 
/// The following are the types where no automatic conversion happens, with
/// the exception of expected integer and floating point casts.
/// 
/// | SQL type      | Rust type(s)                              | Infallible |
/// |---------------|-------------------------------------------|------------|
/// | `INTEGER`     | `bool, i64, i128`                         | Yes        |
/// | `INTEGER`     | `i8, i16, i32, isize, u8, u16, u32, u64, u128, usize` | No |
/// | `REAL`        | `f64, f32`                                | Yes        |
/// | `BLOB`        | `Vec<u8>`                                 | Yes        |
/// | `TEXT`\*      | `String`                                  | Yes        |
/// 
/// \* If the text is invalid UTF-8, the library treats as if it was a blob.
/// 
/// # Automatic conversions
///
/// For `INTEGER` SQLite type:
///
/// * Converting to `bool` returns `false` only if the value is zero.
/// * Converting to `i64` or `i128` is infallible. Converting to
///   other numeric types (`i8`, `i16`, `i32`, `isize`, `u8`, `u16`, `u32`,
///   `u64`, `u128`, `usize`) may fail because of bounds checks.
/// * Converting to `f64` or `f32` is infallible.
/// * Converting to `String` produces the decimal textual representation of
///   the integer. It is infallible.
/// * Converting to `Vec<u8>` produces the UTF-8 bytes of the decimal textual
///   representation of the integer. It is infallible.
///
/// For `REAL` SQLite type:
///
/// * Converting to `bool` returns `false` only if the value is zero.
/// * Converting to integers may fail because of bounds checks and NaN.
/// * Converting to `f64` or `f32` is infallible.
/// * Converting to `String` produces a textual representation of the
///   floating point number. It is infallible.
/// * Converting to `Vec<u8>` produces the UTF-8 bytes of the textual
///   representation of the sfloating point number. It is infallible.
///
/// For `TEXT` SQLite type:
///
/// * Converting to `bool` returns `false` if the value is empty or a textual
///   representation of the number zero.
/// * Converting to integers attempts to parse the number, which may fail.
/// * Converting to `f64` or `f32` attempts to parse the number, which
///   may fail.
/// * Converting to `String` validates the UTF-8, which may fail.
/// * Converting to `Vec<u8>` returns the bytes. It is infallible.
///
/// For `BLOB` SQLite type:
///
/// * Converting to `bool` return `false` if the value is empty or a UTF-8
///   textual representation of the number zero.
/// * Converting to integers treats the bytes as a string, validating
///   the UTF-8 and then parsing the number, which may fail.
/// * Converting to `f64` or `f32` treats the bytes as a string, validating
///   the UTF-8 and then parsing the number, which may fail.
/// * Converting to `String` trats the bytes as a string, validating the UTF-8,
///   which may fail.
/// * Converting to `Vec<u8>` returns the bytes. It is infallible.
///
/// For `NULL` SQLite type:
///
/// * If converting to an [`Option<T>`], returns `None`, infallibly.
/// * Converting to `bool` return false.
/// * Converting to numbers returns zero.
/// * Converting to `String` or `Vec<u8>` returns empty.
///
/// When `Vec<u8>` is mentioned, the same applies for `Box<[u8]>` and
/// `Arc<[u8]>`.
///
/// When `String` is mentioned, the same applies for `Box<str>` and
/// `Arc<str>`.
pub trait FromSql: Sized {
    /// Converts into a Rust type.
    fn from_sql(sql: SqlRef) -> Result<Self>;
}

impl<T> FromSql for Option<T>
where
    T: FromSql,
{
    #[inline]
    fn from_sql(sql: SqlRef) -> Result<Self> {
        match sql {
            SqlRef::Null => Ok(None),
            _ => T::from_sql(sql).map(Some),
        }
    }
}

impl FromSql for bool {
    fn from_sql(sql: SqlRef) -> Result<Self> {
        match sql {
            SqlRef::Int(x) => Ok(x != 0),
            SqlRef::Float(x) => Ok(x != 0.0),
            SqlRef::Text(x) => Ok(!x.is_empty() && x.parse() != Ok(0)),
            SqlRef::Blob(x) => {
                if x.is_empty() {
                    Ok(false)
                } else if let Ok(x) = str::from_utf8(x) {
                    Ok(x.parse() != Ok(0))
                } else {
                    Ok(true)
                }
            }
            SqlRef::ZeroBlob(_) | SqlRef::Null => Ok(false),
        }
    }
}

macro_rules! float_to_int {
    ($INT:ty, $x:expr) => {{
        const MAX: f64 = <$INT>::MAX as f64;
        const MIN: f64 = <$INT>::MIN as f64;
        let x = $x;
        if x.is_finite() && (MIN..=MAX).contains(&x) {
            Some(x as $INT)
        } else {
            None
        }
    }};
}

macro_rules! impl_int_lossless {
    ($($x: ty),*) => {
        $(
            impl FromSql for $x {
                fn from_sql(sql: SqlRef) -> Result<Self> {
                    match sql {
                        SqlRef::Int(x) => Ok(x.into()),
                        SqlRef::Float(x) => float_to_int!($x, x)
                            .ok_or_else(|| ErrorKind::DatatypeMismatch.into()),
                        SqlRef::Text(x) => x
                            .parse()
                            .map_err(|_| ErrorKind::DatatypeMismatch.into()),
                        SqlRef::Blob(x) => String::from_utf8_lossy(x)
                            .parse()
                            .map_err(|_| ErrorKind::DatatypeMismatch.into()),
                        SqlRef::ZeroBlob(_) | SqlRef::Null => Ok(0),
                    }
                }
            }
        )*
    };
}

impl_int_lossless!(i64, i128);

macro_rules! impl_int {
    ($($x: ty),*) => {
        $(
            impl FromSql for $x {
                fn from_sql(sql: SqlRef) -> Result<Self> {
                    match sql {
                        SqlRef::Int(x) => x
                            .try_into()
                            .map_err(|_| ErrorKind::DatatypeMismatch.into()),
                        SqlRef::Float(x) => float_to_int!($x, x)
                            .ok_or_else(|| ErrorKind::DatatypeMismatch.into()),
                        SqlRef::Text(x) => x
                            .parse()
                            .map_err(|_| ErrorKind::DatatypeMismatch.into()),
                        SqlRef::Blob(x) => String::from_utf8_lossy(x)
                            .parse()
                            .map_err(|_| ErrorKind::DatatypeMismatch.into()),
                        SqlRef::ZeroBlob(_) | SqlRef::Null => Ok(0),
                    }
                }
            }
        )*
    };
}

impl_int!(i8, i16, i32, isize, u8, u16, u32, u64, u128, usize);

impl FromSql for f64 {
    fn from_sql(sql: SqlRef) -> Result<Self> {
        match sql {
            SqlRef::Int(x) => Ok(x as f64),
            SqlRef::Float(x) => Ok(x),
            SqlRef::Text(x) => x.parse().map_err(|_| ErrorKind::DatatypeMismatch.into()),
            SqlRef::Blob(x) => str::from_utf8(x)
                .map_err(|_| ErrorKind::DatatypeMismatch)?
                .parse()
                .map_err(|_| ErrorKind::DatatypeMismatch.into()),
            SqlRef::ZeroBlob(_) | SqlRef::Null => Ok(0.0),
        }
    }
}

impl FromSql for f32 {
    fn from_sql(sql: SqlRef) -> Result<Self> {
        match sql {
            SqlRef::Int(x) => Ok(x as f32),
            SqlRef::Float(x) => Ok(x as f32),
            SqlRef::Text(x) => x.parse().map_err(|_| ErrorKind::DatatypeMismatch.into()),
            SqlRef::Blob(x) => String::from_utf8_lossy(x)
                .parse()
                .map_err(|_| ErrorKind::DatatypeMismatch.into()),
            SqlRef::ZeroBlob(_) | SqlRef::Null => Ok(0.0),
        }
    }
}

macro_rules! impl_blob {
    ($x:ty) => {
        impl FromSql for $x {
            fn from_sql(sql: SqlRef) -> Result<Self> {
                match sql {
                    SqlRef::Int(x) => Ok(x.to_string().into_bytes().into()),
                    SqlRef::Float(x) => Ok(x.to_string().into_bytes().into()),
                    SqlRef::Text(x) => Ok(x.as_bytes().into()),
                    SqlRef::Blob(x) => Ok(x.into()),
                    SqlRef::ZeroBlob(_) | SqlRef::Null => Ok(iter::empty().collect()),
                }
            }
        }
    };
}

impl_blob!(Vec<u8>);
impl_blob!(Box<[u8]>);
impl_blob!(Arc<[u8]>);

macro_rules! impl_text {
    ($x:ty) => {
        impl FromSql for $x {
            fn from_sql(sql: SqlRef) -> Result<Self> {
                match sql {
                    SqlRef::Int(x) => Ok(x.to_string().into()),
                    SqlRef::Float(x) => Ok(x.to_string().into()),
                    SqlRef::Text(x) => Ok(x.into()),
                    SqlRef::Blob(x) => Ok(str::from_utf8(x)
                        .map_err(|_| ErrorKind::DatatypeMismatch)?
                        .into()),
                    SqlRef::ZeroBlob(_) | SqlRef::Null => Ok("".into()),
                }
            }
        }
    };
}

impl_text!(String);
impl_text!(Box<str>);
impl_text!(Arc<str>);
