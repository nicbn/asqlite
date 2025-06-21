use super::Connection;
use crate::{
    convert::{Param, ParamList, SqlRef},
    Error, ErrorKind, Result, ZeroBlob,
};
use std::{ffi::c_int, ptr::NonNull};

pub(crate) struct Statement {
    handle: NonNull<libsqlite3_sys::sqlite3_stmt>,
    error: Option<Error>,

    connection: Connection,
    params: Option<Box<[Param]>>,
}

impl Statement {
    pub(super) unsafe fn from(
        handle: NonNull<libsqlite3_sys::sqlite3_stmt>,
        connection: Connection,
    ) -> Self {
        Self {
            handle,
            error: None,

            connection,
            params: None,
        }
    }

    /// Binds parameters.
    pub(crate) fn bind(&mut self, params: ParamList) {
        if self.error.is_none() {
            if let Err(e) = self.try_bind(params) {
                self.error = Some(e);
            }
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

    /// Resets and unbinds parameters.
    pub(crate) fn reset_and_unbind(&mut self) {
        self.error = None;
        if let Err(e) = self.try_reset_and_unbind() {
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

    /// Resets.
    pub(crate) fn reset(&mut self) -> Result<()> {
        match unsafe { libsqlite3_sys::sqlite3_reset(self.handle.as_ptr()) } {
            libsqlite3_sys::SQLITE_OK => Ok(()),
            _ => Err(self.connection.last_error()),
        }
    }

    /// Steps.
    pub(crate) fn step(&mut self) -> Result<Step> {
        if let Some(e) = self.error.take() {
            return Err(e);
        }
        match unsafe { libsqlite3_sys::sqlite3_step(self.handle.as_ptr()) } {
            libsqlite3_sys::SQLITE_ROW => Ok(Step::Row),
            libsqlite3_sys::SQLITE_DONE => Ok(Step::Done {
                modified: self.connection.total_changes(),
                insert: self.connection.last_insert_rowid(),
            }),
            _ => Err(self.connection.last_error()),
        }
    }

    pub(crate) fn handle(&self) -> *mut libsqlite3_sys::sqlite3_stmt {
        self.handle.as_ptr()
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
    Done { modified: u32, insert: i64 },
    Row,
}
