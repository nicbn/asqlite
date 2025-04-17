use super::{blob::Blob, Statement};
use crate::{Error, ErrorKind, Result};
use std::{
    cell::UnsafeCell,
    cmp::Ordering,
    ffi::{c_int, c_void, CStr, CString},
    marker::PhantomData,
    panic::RefUnwindSafe,
    ptr::{self, NonNull},
    slice,
    sync::Arc,
    time::Duration,
};

pub(crate) struct ConnectionHandle {
    inner: NonNull<libsqlite3_sys::sqlite3>,

    busy_handler: UnsafeCell<Option<Box<dyn FnMut(u32) -> bool + Send>>>,
}

unsafe impl Send for ConnectionHandle {}

unsafe impl Sync for ConnectionHandle {}

impl RefUnwindSafe for ConnectionHandle {}

impl ConnectionHandle {
    pub(crate) fn get(&self) -> *mut libsqlite3_sys::sqlite3 {
        self.inner.as_ptr()
    }

    pub(crate) fn interrupt(&self) {
        unsafe { libsqlite3_sys::sqlite3_interrupt(self.get()) };
    }
}

impl Drop for ConnectionHandle {
    fn drop(&mut self) {
        let r = unsafe { libsqlite3_sys::sqlite3_close(self.get()) };

        debug_assert_eq!(r, 0);
    }
}

#[derive(Clone)]
pub(crate) struct Connection {
    handle: Arc<ConnectionHandle>,
    _no_send: PhantomData<*mut libsqlite3_sys::sqlite3>,
}

impl Connection {
    pub(crate) fn open(path: CString, flags: i32) -> Result<Self> {
        let mut handle = ptr::null_mut();
        let result = unsafe {
            libsqlite3_sys::sqlite3_open_v2(path.as_ptr(), &mut handle, flags, ptr::null())
        };
        let handle = NonNull::new(handle).ok_or(ErrorKind::OutOfMemory)?;

        let connection = Self {
            handle: Arc::new(ConnectionHandle {
                inner: handle,

                busy_handler: UnsafeCell::new(None),
            }),
            _no_send: PhantomData,
        };

        if result != libsqlite3_sys::SQLITE_OK {
            let error = connection.last_error();
            return Err(error);
        }

        Ok(connection)
    }

    pub(crate) fn handle(&self) -> &Arc<ConnectionHandle> {
        &self.handle
    }

    pub(crate) fn last_error(&self) -> Error {
        let error = unsafe { libsqlite3_sys::sqlite3_errcode(self.handle.get()) };
        let error_message = unsafe { libsqlite3_sys::sqlite3_errmsg(self.handle.get()) };

        let error_message = if !error_message.is_null() {
            Some(unsafe { CStr::from_ptr(error_message).to_string_lossy().into() })
        } else {
            None
        };
        Error::from_code(error, error_message)
    }

    pub(crate) fn prepare(&self, sql: &str) -> Result<Option<(Statement, usize)>> {
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
        let statement = unsafe { Statement::from(statement, self.clone()) };

        Ok(Some((statement, consumed as usize)))
    }

    pub(crate) fn total_changes(&self) -> u32 {
        unsafe { libsqlite3_sys::sqlite3_total_changes(self.handle.get()) as u32 }
    }

    pub(crate) fn last_insert_rowid(&self) -> i64 {
        unsafe { libsqlite3_sys::sqlite3_last_insert_rowid(self.handle.get()) }
    }

    pub(crate) fn flush_cache(&self) -> Result<()> {
        match unsafe { libsqlite3_sys::sqlite3_db_cacheflush(self.handle.get()) } {
            libsqlite3_sys::SQLITE_OK => Ok(()),
            _ => Err(self.last_error()),
        }
    }

    pub(crate) fn reset_busy_handler(&self) -> Result<()> {
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

    /// Safety: non reentrant
    pub(crate) unsafe fn set_busy_handler(
        &self,
        f: Box<dyn FnMut(u32) -> bool + Send>,
    ) -> Result<()> {
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

    pub(crate) fn set_busy_timeout(&self, duration: Duration) -> Result<()> {
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

    pub(crate) fn create_collation(
        &self,
        name: CString,
        f: Box<dyn FnMut(&str, &str) -> Ordering>,
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

    pub(crate) fn open_blob(
        &self,
        database: CString,
        table: CString,
        column: CString,
        row: i64,
        flags: i32,
    ) -> Result<Blob> {
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
        Ok(unsafe { Blob::from(NonNull::new(result).unwrap(), self.clone()) })
    }
}
