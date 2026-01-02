use crate::{blocking::sqlite::ConnectionHandle, ErrorKind, Result};
use std::{ffi::c_void, ptr::NonNull, sync::Arc};

pub(crate) struct Blob {
    handle: NonNull<libsqlite3_sys::sqlite3_blob>,
    connection: Arc<ConnectionHandle>,
}

impl Blob {
    pub(super) unsafe fn from(
        handle: NonNull<libsqlite3_sys::sqlite3_blob>,
        connection: Arc<ConnectionHandle>,
    ) -> Self {
        Self { handle, connection }
    }

    pub(crate) fn reopen(&self, row: i64) -> Result<()> {
        match unsafe { libsqlite3_sys::sqlite3_blob_reopen(self.handle.as_ptr(), row) } {
            libsqlite3_sys::SQLITE_OK => Ok(()),
            _ => Err(self.connection.last_error()),
        }
    }

    pub(crate) fn size(&self) -> u64 {
        unsafe { libsqlite3_sys::sqlite3_blob_bytes(self.handle.as_ptr()) as i64 as u64 }
    }

    pub(crate) fn read(&self, chunk: &mut [u8], position: u64) -> Result<()> {
        match unsafe {
            libsqlite3_sys::sqlite3_blob_read(
                self.handle.as_ptr(),
                chunk.as_mut_ptr() as *mut c_void,
                chunk.len().try_into().unwrap_or(i32::MAX),
                position.try_into().map_err(|_| ErrorKind::OutOfRange)?,
            )
        } {
            libsqlite3_sys::SQLITE_OK => {}
            _ => return Err(self.connection.last_error()),
        }
        Ok(())
    }

    pub(crate) fn write(&self, chunk: &[u8], position: u64) -> Result<()> {
        match unsafe {
            libsqlite3_sys::sqlite3_blob_write(
                self.handle.as_ptr(),
                chunk.as_ptr() as *const c_void,
                chunk.len().try_into().map_err(|_| ErrorKind::OutOfRange)?,
                position.try_into().map_err(|_| ErrorKind::OutOfRange)?,
            )
        } {
            libsqlite3_sys::SQLITE_OK => Ok(()),
            _ => Err(self.connection.last_error()),
        }
    }
}

impl Drop for Blob {
    fn drop(&mut self) {
        let r = unsafe { libsqlite3_sys::sqlite3_blob_close(self.handle.as_ptr()) };

        debug_assert_eq!(r, 0);
    }
}
