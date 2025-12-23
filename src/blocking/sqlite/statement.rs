use crate::{
    blocking::{sqlite::ConnectionHandle, Row},
    convert::{Param, ParamList, SqlRef},
    Error, ErrorKind, Result, ZeroBlob,
};
use core::str;
use std::{ffi::c_int, ptr::NonNull, slice, sync::Arc};

pub(super) struct Statement {
    handle: NonNull<libsqlite3_sys::sqlite3_stmt>,

    error: Option<Error>,

    connection: Arc<ConnectionHandle>,
    params: Option<Box<[Param]>>,
}

impl Statement {
    pub(super) unsafe fn from(
        handle: NonNull<libsqlite3_sys::sqlite3_stmt>,
        connection: Arc<ConnectionHandle>,
    ) -> Self {
        Self {
            handle,

            error: None,

            connection,
            params: None,
        }
    }

    fn try_bind(&mut self, params: ParamList) -> Result<()> {
        self.try_reset_and_unbind()?;
        self.params = Some(params.items?);
        let mut position = 1;
        let mut param_binder = ParamBinder::new(self);
        for param in self.params.as_ref().unwrap().iter() {
            match &param.name {
                Some(name) => unsafe { param_binder.bind_named(name, param.param.borrow())? },
                None => unsafe { param_binder.bind(position, param.param.borrow())? },
            }

            position += 1;
        }
        Ok(())
    }

    pub(crate) fn bind(&mut self, params: ParamList) {
        if let Err(e) = self.try_bind(params) {
            self.error = Some(e);
        }
    }

    fn try_reset_and_unbind(&mut self) -> Result<()> {
        self.reset()?;

        if self.params.is_some() {
            match unsafe { libsqlite3_sys::sqlite3_clear_bindings(self.handle.as_ptr()) } {
                libsqlite3_sys::SQLITE_OK => {}
                _ => return Err(self.connection.last_error()),
            }
            self.params = None;
        }

        Ok(())
    }

    pub(crate) fn reset_and_unbind(&mut self) {
        if let Err(e) = self.try_reset_and_unbind() {
            self.error = Some(e);
        }
    }

    /// Resets.
    fn reset(&mut self) -> Result<()> {
        match unsafe { libsqlite3_sys::sqlite3_reset(self.handle.as_ptr()) } {
            libsqlite3_sys::SQLITE_OK => Ok(()),
            _ => Err(self.connection.last_error()),
        }
    }

    /// Steps.
    pub(crate) fn step(&mut self) -> Result<Step> {
        if let Some(e) = self.error.take() {
            return Err(e);
        };
        match unsafe { libsqlite3_sys::sqlite3_step(self.handle.as_ptr()) } {
            libsqlite3_sys::SQLITE_ROW => Ok(Step::Row),
            libsqlite3_sys::SQLITE_DONE => Ok(Step::Done),
            _ => Err(self.connection.last_error()),
        }
    }

    pub(crate) fn handle(&self) -> *mut libsqlite3_sys::sqlite3_stmt {
        self.handle.as_ptr()
    }
}

impl Row for Statement {
    fn row_len(&self) -> usize {
        usize::try_from(unsafe { libsqlite3_sys::sqlite3_column_count(self.handle()) }).unwrap_or(0)
    }

    fn get(&self, idx: usize) -> Option<Result<SqlRef<'_>>> {
        if idx >= self.row_len() {
            return None;
        }

        let idx = c_int::try_from(idx).ok()?;

        match unsafe { libsqlite3_sys::sqlite3_column_type(self.handle(), idx) } {
            libsqlite3_sys::SQLITE_INTEGER => {
                let value =
                    unsafe { libsqlite3_sys::sqlite3_column_int64(self.handle(), idx) };
                Some(Ok(SqlRef::Int(value)))
            }
            libsqlite3_sys::SQLITE_FLOAT => {
                let value = unsafe {
                    libsqlite3_sys::sqlite3_column_double(self.handle(), idx)
                };
                Some(Ok(SqlRef::Float(value)))
            }
            libsqlite3_sys::SQLITE_TEXT => {
                let ptr =
                    unsafe { libsqlite3_sys::sqlite3_column_text(self.handle(), idx) };

                if ptr.is_null() {
                    return Some(Ok(SqlRef::Text("")));
                }

                let size =
                    unsafe { libsqlite3_sys::sqlite3_column_bytes(self.handle(), idx) };
                let Ok(size) = size.try_into() else {
                    return Some(Err(Error::from(ErrorKind::OutOfRange)));
                };

                let slice = unsafe { slice::from_raw_parts(ptr, size) };

                // If the text is not actually UTF-8, pretend it is a blob
                if let Ok(x) = str::from_utf8(slice) {
                    Some(Ok(SqlRef::Text(x)))
                } else {
                    Some(Ok(SqlRef::Blob(slice)))
                }
            }
            libsqlite3_sys::SQLITE_BLOB => {
                let ptr =
                    unsafe { libsqlite3_sys::sqlite3_column_text(self.handle(), idx) };

                if ptr.is_null() {
                    return Some(Ok(SqlRef::Blob(&[])));
                }

                let size =
                    unsafe { libsqlite3_sys::sqlite3_column_bytes(self.handle(), idx) };
                let Ok(size) = size.try_into() else {
                    return Some(Err(Error::from(ErrorKind::OutOfRange)));
                };

                Some(Ok(SqlRef::Blob(unsafe {
                    slice::from_raw_parts(ptr, size)
                })))
            }
            libsqlite3_sys::SQLITE_NULL => Some(Ok(SqlRef::Null)),
            _ => Some(Err(Error::new(
                ErrorKind::DatatypeMismatch,
                "invalid data type".to_string(),
            ))),
        }
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        let r = unsafe { libsqlite3_sys::sqlite3_finalize(self.handle.as_ptr()) };

        debug_assert_eq!(r, 0);
    }
}

/// Type for binding parameters.
struct ParamBinder<'a> {
    statement: &'a Statement,
    parameter_name_buf: String,
}

impl<'a> ParamBinder<'a> {
    fn new(statement: &'a Statement) -> Self {
        Self {
            statement,
            parameter_name_buf: String::new(),
        }
    }

    unsafe fn bind_blob(&mut self, position: c_int, value: &[u8]) -> Result<()> {
        match unsafe {
            libsqlite3_sys::sqlite3_bind_blob(
                self.statement.handle.as_ptr(),
                position,
                value.as_ptr() as _,
                value.len().try_into().map_err(|_| ErrorKind::OutOfRange)?,
                libsqlite3_sys::SQLITE_STATIC(),
            )
        } {
            libsqlite3_sys::SQLITE_OK => Ok(()),
            _ => Err(self.statement.connection.last_error()),
        }
    }

    unsafe fn bind_text(&mut self, position: c_int, value: &str) -> Result<()> {
        match unsafe {
            libsqlite3_sys::sqlite3_bind_text(
                self.statement.handle.as_ptr(),
                position,
                value.as_ptr() as _,
                value.len().try_into().map_err(|_| ErrorKind::OutOfRange)?,
                libsqlite3_sys::SQLITE_STATIC(),
            )
        } {
            libsqlite3_sys::SQLITE_OK => Ok(()),
            _ => Err(self.statement.connection.last_error()),
        }
    }

    fn bind_int(&mut self, position: c_int, value: i64) -> Result<()> {
        match unsafe {
            libsqlite3_sys::sqlite3_bind_int64(self.statement.handle.as_ptr(), position, value)
        } {
            libsqlite3_sys::SQLITE_OK => Ok(()),
            _ => Err(self.statement.connection.last_error()),
        }
    }

    fn bind_float(&mut self, position: c_int, value: f64) -> Result<()> {
        match unsafe {
            libsqlite3_sys::sqlite3_bind_double(self.statement.handle.as_ptr(), position, value)
        } {
            libsqlite3_sys::SQLITE_OK => Ok(()),
            _ => Err(self.statement.connection.last_error()),
        }
    }

    fn bind_null(&mut self, position: c_int) -> Result<()> {
        match unsafe { libsqlite3_sys::sqlite3_bind_null(self.statement.handle.as_ptr(), position) }
        {
            libsqlite3_sys::SQLITE_OK => Ok(()),
            _ => Err(self.statement.connection.last_error()),
        }
    }

    fn bind_zero_blob(&mut self, position: c_int, value: ZeroBlob) -> Result<()> {
        match unsafe {
            libsqlite3_sys::sqlite3_bind_zeroblob(
                self.statement.handle.as_ptr(),
                position,
                value.0.try_into().map_err(|_| ErrorKind::OutOfRange)?,
            )
        } {
            libsqlite3_sys::SQLITE_OK => Ok(()),
            _ => Err(self.statement.connection.last_error()),
        }
    }

    /// Bind a named parameter.
    ///
    /// Advances the position.
    unsafe fn bind_named(&mut self, name: &str, sql: SqlRef) -> Result<()> {
        self.parameter_name_buf.clear();

        self.parameter_name_buf.reserve(64.min(name.len() + 2));

        if !name.starts_with([':', '@', '$']) {
            self.parameter_name_buf.push(':');
        }

        self.parameter_name_buf.push_str(name);
        self.parameter_name_buf.push('\0');

        let position = unsafe {
            libsqlite3_sys::sqlite3_bind_parameter_index(
                self.statement.handle.as_ptr(),
                self.parameter_name_buf.as_ptr() as _,
            )
        };
        if position == 0 {
            return Err(Error::new(
                ErrorKind::Misuse,
                format!("parameter '{}' not found", name),
            ));
        }

        unsafe { self.bind(position, sql)? };

        Ok(())
    }

    /// Bind a positional parameter.
    ///
    /// Advances the position.
    unsafe fn bind(&mut self, position: c_int, sql: SqlRef) -> Result<()> {
        match sql {
            SqlRef::Int(x) => self.bind_int(position, x)?,
            SqlRef::Float(x) => self.bind_float(position, x)?,
            SqlRef::Text(x) => unsafe { self.bind_text(position, x)? },
            SqlRef::Blob(x) => unsafe { self.bind_blob(position, x)? },
            SqlRef::ZeroBlob(x) => self.bind_zero_blob(position, x)?,
            SqlRef::Null => self.bind_null(position)?,
        }

        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Step {
    Done,
    Row,
}
