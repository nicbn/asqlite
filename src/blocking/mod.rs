//! Blocking, single-thread bindings to SQLite.

use crate::{
    convert::{ParamList, SqlRef},
    Result,
};
use std::{cmp::Ordering, ffi::CStr, panic::RefUnwindSafe, sync::Arc, time::Duration};

pub(crate) mod sqlite;

#[derive(Clone, Copy, Debug)]
pub(crate) struct BlobIndex(pub(crate) usize);

#[derive(Clone, Copy, Debug)]
pub(crate) struct StatementIndex(pub(crate) usize);

pub(crate) trait ConnectionFactory: Send {
    type Connection: Connection;

    fn open(&self, path: &CStr, flags: i32) -> Result<Self::Connection>;
}

pub(crate) trait Connection {
    fn interrupt_handle(&self) -> Arc<dyn InterruptHandle>;

    fn prepare(&mut self, sql: &str) -> Result<Option<(StatementIndex, usize)>>;

    fn bind(&mut self, statement: StatementIndex, param_list: ParamList);

    fn step(
        &mut self,
        statement: StatementIndex,
        callback: &mut dyn FnMut(Option<&mut dyn ExactSizeIterator<Item = Result<SqlRef>>>),
    );

    fn execute(&mut self, statement: StatementIndex) -> Result<()>;

    fn reset(&mut self, statement: StatementIndex);

    fn finalize(&mut self, statement: StatementIndex);

    fn flush_cache(&mut self) -> Result<()>;

    fn reset_busy_handler(&mut self) -> Result<()>;

    unsafe fn set_busy_handler(&mut self, f: Box<dyn FnMut(u32) -> bool + Send>) -> Result<()>;

    fn set_busy_timeout(&mut self, duration: Duration) -> Result<()>;

    fn create_collation(
        &mut self,
        name: &CStr,
        f: Box<dyn FnMut(&str, &str) -> Ordering>,
    ) -> Result<()>;

    fn open_blob(
        &mut self,
        database: &CStr,
        table: &CStr,
        column: &CStr,
        row: i64,
        flags: i32,
    ) -> Result<BlobIndex>;

    fn reopen_blob(&mut self, index: BlobIndex, row: i64) -> Result<()>;

    fn blob_size(&mut self, index: BlobIndex) -> Result<u64>;

    fn read_blob(
        &mut self,
        index: BlobIndex,
        chunk: &mut Vec<u8>,
        position: u64,
        bytes: usize,
    ) -> Result<()>;

    fn write_blob(&mut self, index: BlobIndex, chunk: &[u8], position: u64) -> Result<()>;

    fn close_blob(&mut self, index: BlobIndex);

    fn last_insert_row_id(&mut self) -> i64;

    fn total_changes(&mut self) -> u32;
}

pub(crate) trait InterruptHandle: Send + Sync + RefUnwindSafe {
    fn interrupt(&self);
}
