use crate::{
    blocking::{
        self,
        sqlite::{Blob, Statement, Step},
        BlobIndex, Row,
    },
    convert::ParamList,
    utils::vec_arena::VecArena,
    Error, ErrorKind, Result,
};
use std::{
    cell::UnsafeCell,
    cmp::Ordering,
    ffi::{c_int, c_void, CStr},
    panic::RefUnwindSafe,
    ptr::{self, NonNull},
    slice,
    sync::Arc,
};

pub(super) struct ConnectionHandle {
    inner: NonNull<libsqlite3_sys::sqlite3>,

    busy_handler: UnsafeCell<Option<Box<dyn FnMut(u32) -> bool + Send>>>,
}

impl RefUnwindSafe for ConnectionHandle {}

impl ConnectionHandle {
    pub(super) fn last_error(&self) -> Error {
        let error = unsafe { libsqlite3_sys::sqlite3_errcode(self.get()) };
        let error_message = unsafe { libsqlite3_sys::sqlite3_errmsg(self.get()) };

        let error_message = if !error_message.is_null() {
            Some(unsafe { CStr::from_ptr(error_message).to_string_lossy().into() })
        } else {
            None
        };
        Error::from_code(error, error_message)
    }

    fn get(&self) -> *mut libsqlite3_sys::sqlite3 {
        self.inner.as_ptr()
    }
}

impl Drop for ConnectionHandle {
    fn drop(&mut self) {
        let r = unsafe { libsqlite3_sys::sqlite3_close(self.get()) };

        debug_assert_eq!(r, 0);
    }
}

#[repr(transparent)]
pub(crate) struct InterruptHandle(ConnectionHandle);

unsafe impl Send for InterruptHandle {}

unsafe impl Sync for InterruptHandle {}

impl blocking::InterruptHandle for InterruptHandle {
    fn interrupt(&self) {
        unsafe { libsqlite3_sys::sqlite3_interrupt(self.0.get()) };
    }
}

pub(crate) struct Connection {
    handle: Arc<ConnectionHandle>,

    blobs: VecArena<Blob>,

    statements: VecArena<Statement>,
}

impl blocking::Connection for Connection {
    fn interrupt_handle(&self) -> Arc<dyn blocking::InterruptHandle> {
        unsafe { Arc::from_raw(Arc::into_raw(self.handle.clone()) as *const InterruptHandle) }
    }

    fn prepare(&mut self, sql: &str) -> Result<Option<(blocking::StatementIndex, usize)>> {
        let mut handle = ptr::null_mut();
        let mut sql_tail = ptr::null();
        match unsafe {
            libsqlite3_sys::sqlite3_prepare_v2(
                self.handle.get(),
                sql.as_ptr() as _,
                sql.len().try_into().map_err(|_| ErrorKind::OutOfRange)?,
                &mut handle,
                &mut sql_tail,
            )
        } {
            libsqlite3_sys::SQLITE_OK => {}
            _ => return Err(self.last_error()),
        }

        let consumed = unsafe { sql_tail.offset_from(sql.as_ptr() as _) };
        let Some(statement) = NonNull::new(handle) else {
            return Ok(None);
        };
        let statement = unsafe { Statement::from(statement, self.handle.clone()) };

        let index = self.statements.push(statement);
        Ok(Some((blocking::StatementIndex(index), consumed as usize)))
    }

    fn bind(&mut self, statement: blocking::StatementIndex, param_list: ParamList) {
        if let Some(statement) = self.statements.get_mut(statement.0) {
            statement.bind(param_list);
        }
    }

    fn step(&mut self, statement: blocking::StatementIndex) -> Result<Option<&mut dyn Row>> {
        let statement = self
            .statements
            .get_mut(statement.0)
            .ok_or_else(|| Error::from(ErrorKind::Misuse))?;
        match statement.step()? {
            Step::Row => Ok(Some(statement)),
            Step::Done => Ok(None),
        }
    }

    fn execute(&mut self, statement: blocking::StatementIndex) -> Result<()> {
        let statement = self
            .statements
            .get_mut(statement.0)
            .ok_or_else(|| Error::from(ErrorKind::Misuse))?;
        while statement.step()? == Step::Row {}
        Ok(())
    }

    fn reset(&mut self, statement: blocking::StatementIndex) {
        if let Some(statement) = self.statements.get_mut(statement.0) {
            statement.reset_and_unbind();
        }
    }

    fn finalize(&mut self, statement: blocking::StatementIndex) {
        self.statements.remove(statement.0);
    }

    fn flush_cache(&mut self) -> Result<()> {
        match unsafe { libsqlite3_sys::sqlite3_db_cacheflush(self.handle.get()) } {
            libsqlite3_sys::SQLITE_OK => Ok(()),
            _ => Err(self.last_error()),
        }
    }

    fn reset_busy_handler(&mut self) -> Result<()> {
        match unsafe {
            libsqlite3_sys::sqlite3_busy_handler(self.handle.get(), None, ptr::null_mut())
        } {
            libsqlite3_sys::SQLITE_OK => {
                unsafe { *self.handle.busy_handler.get() = None };

                Ok(())
            }
            _ => Err(self.last_error()),
        }
    }

    unsafe fn set_busy_handler(&mut self, f: Box<dyn FnMut(u32) -> bool + Send>) -> Result<()> {
        unsafe extern "C" fn api_callback(userdata: *mut c_void, called: c_int) -> c_int {
            unsafe {
                let conn = &*(userdata as *const ConnectionHandle);
                let f = (*conn.busy_handler.get()).as_mut().unwrap();
                f(called as u32).into()
            }
        }

        match unsafe {
            libsqlite3_sys::sqlite3_busy_handler(
                self.handle.get(),
                Some(api_callback),
                Arc::as_ptr(&self.handle) as _,
            )
        } {
            libsqlite3_sys::SQLITE_OK => {
                unsafe { *self.handle.busy_handler.get() = Some(f) };

                Ok(())
            }
            _ => Err(self.last_error()),
        }
    }

    fn set_busy_timeout(&mut self, duration: std::time::Duration) -> Result<()> {
        match unsafe {
            libsqlite3_sys::sqlite3_busy_timeout(
                self.handle.get(),
                duration.as_millis().try_into().unwrap_or(c_int::MAX),
            )
        } {
            libsqlite3_sys::SQLITE_OK => {
                unsafe { *self.handle.busy_handler.get() = None };

                Ok(())
            }
            _ => Err(self.last_error()),
        }
    }

    fn create_collation(
        &mut self,
        name: &CStr,
        f: Box<dyn FnMut(&str, &str) -> std::cmp::Ordering>,
    ) -> Result<()> {
        unsafe extern "C" fn api_callback(
            userdata: *mut c_void,
            length_left: c_int,
            left: *const c_void,
            length_right: c_int,
            right: *const c_void,
        ) -> c_int {
            let f = userdata as *mut Box<dyn FnMut(&str, &str) -> Ordering>;
            unsafe {
                ((*f)(
                    &String::from_utf8_lossy(slice::from_raw_parts(
                        left as *const u8,
                        length_left as usize,
                    )),
                    &String::from_utf8_lossy(slice::from_raw_parts(
                        right as *const u8,
                        length_right as usize,
                    )),
                ) as i8)
                    .into()
            }
        }

        unsafe extern "C" fn api_callback_drop(userdata: *mut c_void) {
            let f = userdata as *mut Box<dyn FnMut(&str, &str) -> Ordering>;
            drop(unsafe { Box::from_raw(f) });
        }

        let f = Box::new(UnsafeCell::new(f));

        let ptr = f.get();

        match unsafe {
            libsqlite3_sys::sqlite3_create_collation_v2(
                self.handle.get(),
                name.as_ptr(),
                libsqlite3_sys::SQLITE_UTF8,
                ptr.cast(),
                Some(api_callback),
                Some(api_callback_drop),
            )
        } {
            libsqlite3_sys::SQLITE_OK => {
                Box::leak(f);

                Ok(())
            }
            _ => Err(self.last_error()),
        }
    }

    fn open_blob(
        &mut self,
        database: &CStr,
        table: &CStr,
        column: &CStr,
        row: i64,
        flags: i32,
    ) -> Result<blocking::BlobIndex> {
        let mut result = ptr::null_mut();
        match unsafe {
            libsqlite3_sys::sqlite3_blob_open(
                self.handle.get(),
                database.as_ptr(),
                table.as_ptr(),
                column.as_ptr(),
                row,
                flags,
                &mut result,
            )
        } {
            libsqlite3_sys::SQLITE_OK => {}
            _ => return Err(self.last_error()),
        }
        let idx = self
            .blobs
            .push(unsafe { Blob::from(NonNull::new(result).unwrap(), self.handle.clone()) });
        Ok(BlobIndex(idx))
    }

    fn reopen_blob(&mut self, index: blocking::BlobIndex, row: i64) -> Result<()> {
        self.blobs
            .get_mut(index.0)
            .ok_or(ErrorKind::Misuse)?
            .reopen(row)
    }

    fn blob_size(&mut self, index: blocking::BlobIndex) -> Result<u64> {
        Ok(self.blobs.get_mut(index.0).ok_or(ErrorKind::Misuse)?.size())
    }

    fn read_blob(
        &mut self,
        index: blocking::BlobIndex,
        chunk: &mut [u8],
        position: u64,
    ) -> Result<()> {
        self.blobs
            .get_mut(index.0)
            .ok_or(ErrorKind::Misuse)?
            .read(chunk, position)
    }

    fn write_blob(
        &mut self,
        index: blocking::BlobIndex,
        chunk: &[u8],
        position: u64,
    ) -> Result<()> {
        self.blobs
            .get_mut(index.0)
            .ok_or(ErrorKind::Misuse)?
            .write(chunk, position)
    }

    fn close_blob(&mut self, index: blocking::BlobIndex) {
        self.blobs.remove(index.0);
    }

    fn last_insert_row_id(&mut self) -> i64 {
        unsafe { libsqlite3_sys::sqlite3_last_insert_rowid(self.handle.get()) }
    }

    fn total_changes(&mut self) -> u32 {
        unsafe {
            libsqlite3_sys::sqlite3_total_changes(self.handle.get())
                .try_into()
                .unwrap_or(0)
        }
    }
}

impl Connection {
    pub(super) fn open(path: &CStr, flags: i32) -> Result<Self> {
        let mut handle = ptr::null_mut();
        let result = unsafe {
            libsqlite3_sys::sqlite3_open_v2(path.as_ptr(), &mut handle, flags, ptr::null())
        };
        let handle = NonNull::new(handle).ok_or(ErrorKind::OutOfMemory)?;

        let connection = Self {
            #[allow(clippy::arc_with_non_send_sync)]
            handle: Arc::new(ConnectionHandle {
                inner: handle,

                busy_handler: UnsafeCell::new(None),
            }),

            blobs: VecArena::new(),

            statements: VecArena::new(),
        };

        if result != libsqlite3_sys::SQLITE_OK {
            let error = connection.handle.last_error();
            return Err(error);
        }

        Ok(connection)
    }

    fn last_error(&self) -> Error {
        self.handle.last_error()
    }
}
