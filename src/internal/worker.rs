use super::{blob::Blob, vec_arena::VecArena, Connection, ConnectionHandle, Statement, Step};
use crate::{
    convert::{ParamList, RowReader},
    Error, ErrorKind, Result,
};
use std::{
    cmp::Ordering,
    ffi::CString,
    sync::{mpsc, Arc, Weak},
    thread,
    time::Duration,
};

macro_rules! debug_eprintln {
    ($($x:tt)*) => {
        if cfg!(debug_assertions) {
            eprintln!($($x)*);
        }
    };
}

struct Database {
    connection: Option<Connection>,
    blobs: VecArena<Blob>,
    statements: VecArena<Statement>,
}

#[derive(Clone)]
pub(crate) struct Sender {
    tx: mpsc::Sender<RequestMessage>,
}

impl Sender {
    pub(crate) fn send(&self, request: RequestMessage) -> Result<()> {
        self.tx
            .send(request)
            .map_err(|_| Error::background_task_failed())
    }
}

pub(crate) enum RequestMessage {
    Open {
        path: CString,
        flags: i32,
        tx: oneshot::Sender<Result<Weak<ConnectionHandle>>>,
    },

    Close {
        tx: Option<oneshot::Sender<()>>,
    },

    Prepare {
        sql: Arc<str>,
        index_from_start: usize,
        tx: oneshot::Sender<Result<Option<(StatementIndex, usize)>>>,
    },

    StatementBind {
        index: StatementIndex,
        param_list: ParamList,
    },

    StatementNext {
        index: StatementIndex,
        callback: Box<dyn Send + FnOnce(Result<(RowReader, Step)>)>,
    },

    StatementExecute {
        index: StatementIndex,
        tx: oneshot::Sender<Result<(u32, i64)>>,
    },

    StatementReset {
        index: StatementIndex,
    },

    Finish {
        index: StatementIndex,
    },

    FlushDiskCache {
        tx: oneshot::Sender<Result<()>>,
    },

    ResetBusyHandler,

    SetBusyHandler {
        handler: Box<dyn FnMut(u32) -> bool + Send>,
    },

    SetBusyTimeout {
        duration: Duration,
    },

    CreateCollation {
        name: CString,
        callback: Box<dyn FnMut(&str, &str) -> Ordering + Send>,
        tx: oneshot::Sender<Result<()>>,
    },

    OpenBlob {
        database: CString,
        table: CString,
        column: CString,
        row: i64,
        flags: i32,
        tx: oneshot::Sender<Result<(u64, BlobIndex)>>,
    },

    Read {
        index: BlobIndex,
        len: usize,
        chunk: Vec<u8>,
        offset: u64,
        tx: oneshot::Sender<(Vec<u8>, Result<()>)>,
    },

    Write {
        index: BlobIndex,
        chunk: Vec<u8>,
        offset: u64,
        tx: oneshot::Sender<(Vec<u8>, Result<()>)>,
    },

    Reopen {
        index: BlobIndex,
        row: i64,
        tx: oneshot::Sender<Result<()>>,
    },

    CloseBlob {
        index: BlobIndex,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct StatementIndex(usize);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct BlobIndex(usize);

pub(crate) fn worker() -> Sender {
    let (tx_req, rx_req) = mpsc::channel::<RequestMessage>();

    thread::spawn(move || {
        let mut database: Database = Database {
            connection: None,
            blobs: VecArena::new(),
            statements: VecArena::new(),
        };

        while let Ok(x) = rx_req.recv() {
            match x {
                RequestMessage::Open { path, flags, tx } => {
                    let _ = tx.send(database.open(path, flags));
                }

                RequestMessage::Close { tx } => {
                    database.close();
                    if let Some(tx) = tx {
                        let _ = tx.send(());
                    }

                    return;
                }

                RequestMessage::Prepare {
                    sql,
                    index_from_start,
                    tx,
                } => {
                    let _ = tx.send(database.prepare(sql, index_from_start));
                }

                RequestMessage::StatementBind { index, param_list } => {
                    database.statement_bind(index, param_list);
                }

                RequestMessage::StatementNext { index, callback } => {
                    callback(database.statement_next(index));
                }

                RequestMessage::StatementExecute { index, tx } => {
                    let _ = tx.send(database.statement_execute(index));
                }

                RequestMessage::StatementReset { index } => {
                    database.statement_reset(index);
                }

                RequestMessage::Finish { index } => {
                    database.finish(index);
                }

                RequestMessage::FlushDiskCache { tx } => {
                    let _ = tx.send(database.flush_disk_cache());
                }

                RequestMessage::ResetBusyHandler => {
                    if let Err(e) = database.reset_busy_handler() {
                        debug_eprintln!("reset busy handler failed: {}", e);
                    }
                }

                RequestMessage::SetBusyHandler { handler } => {
                    if let Err(e) = database.set_busy_handler(handler) {
                        debug_eprintln!("set busy handler failed: {}", e);
                    }
                }

                RequestMessage::SetBusyTimeout { duration } => {
                    if let Err(e) = database.set_busy_timeout(duration) {
                        debug_eprintln!("set busy timeout failed: {}", e);
                    }
                }

                RequestMessage::CreateCollation { name, callback, tx } => {
                    let _ = tx.send(database.create_collation(name, callback));
                }

                RequestMessage::OpenBlob {
                    database: v,
                    table,
                    column,
                    row,
                    flags,
                    tx,
                } => {
                    let _ = tx.send(database.open_blob(v, table, column, row, flags));
                }

                RequestMessage::Read {
                    index,
                    len,
                    mut chunk,
                    offset,
                    tx,
                } => {
                    let v = database.read(index, len, &mut chunk, offset);
                    let _ = tx.send((chunk, v));
                }

                RequestMessage::Write {
                    index,
                    chunk: bytes,
                    offset,
                    tx,
                } => {
                    let v = database.write(index, &bytes, offset);
                    let _ = tx.send((bytes, v));
                }

                RequestMessage::Reopen { index, row, tx } => {
                    let _ = tx.send(database.reopen(index, row));
                }

                RequestMessage::CloseBlob { index } => {
                    database.close_blob(index);
                }
            }
        }
    });
    Sender { tx: tx_req }
}

impl Database {
    fn open(&mut self, path: CString, flags: i32) -> Result<Weak<ConnectionHandle>> {
        let conn = crate::internal::Connection::open(path, flags)?;
        let handle = Arc::downgrade(conn.handle());
        self.connection = Some(conn);
        Ok(handle)
    }

    fn close(&mut self) {
        self.connection = None;
        self.blobs = VecArena::new();
        self.statements = VecArena::new();
    }

    fn prepare(
        &mut self,
        sql: Arc<str>,
        index_from_start: usize,
    ) -> Result<Option<(StatementIndex, usize)>> {
        let Some((statement, rest)) = self
            .connection
            .as_mut()
            .ok_or(ErrorKind::ConnectionClosed)?
            .prepare(&sql[index_from_start..])?
        else {
            return Ok(None);
        };
        Ok(Some((
            StatementIndex(self.statements.push(statement)),
            index_from_start + rest,
        )))
    }

    fn statement_bind(&mut self, index: StatementIndex, param_list: ParamList) {
        if let Some(statement) = self.statements.get_mut(index.0) {
            statement.bind(param_list)
        }
    }

    fn statement_execute(&mut self, index: StatementIndex) -> Result<(u32, i64)> {
        let statement = self
            .statements
            .get_mut(index.0)
            .ok_or(ErrorKind::ConnectionClosed)?;
        loop {
            if let Step::Done { modified, insert } = statement.step()? {
                return Ok((modified, insert));
            }
        }
    }

    fn statement_next(&mut self, index: StatementIndex) -> Result<(RowReader, Step)> {
        let statement = self
            .statements
            .get_mut(index.0)
            .ok_or(ErrorKind::ConnectionClosed)?;
        let next = statement.step()?;
        Ok((RowReader::new(statement), next))
    }

    fn statement_reset(&mut self, index: StatementIndex) {
        if let Some(statement) = self.statements.get_mut(index.0) {
            statement.reset_and_unbind();
        }
    }

    fn finish(&mut self, index: StatementIndex) {
        self.statements.remove(index.0);
    }

    fn flush_disk_cache(&mut self) -> Result<()> {
        let conn = self
            .connection
            .as_mut()
            .ok_or(ErrorKind::ConnectionClosed)?;
        conn.flush_cache()
    }

    fn reset_busy_handler(&mut self) -> Result<()> {
        let conn = self
            .connection
            .as_mut()
            .ok_or(ErrorKind::ConnectionClosed)?;
        conn.reset_busy_handler()
    }

    fn set_busy_handler(&mut self, f: Box<dyn FnMut(u32) -> bool + Send>) -> Result<()> {
        let conn = self
            .connection
            .as_mut()
            .ok_or(ErrorKind::ConnectionClosed)?;

        // SAFETY: non reentrant
        unsafe { conn.set_busy_handler(f) }
    }

    fn set_busy_timeout(&mut self, duration: Duration) -> Result<()> {
        let conn = self
            .connection
            .as_mut()
            .ok_or(ErrorKind::ConnectionClosed)?;
        conn.set_busy_timeout(duration)
    }

    fn create_collation(
        &mut self,
        name: CString,
        f: Box<dyn FnMut(&str, &str) -> Ordering + Send>,
    ) -> Result<()> {
        let conn = self
            .connection
            .as_mut()
            .ok_or(ErrorKind::ConnectionClosed)?;
        conn.create_collation(name, f)
    }

    fn open_blob(
        &mut self,
        database: CString,
        table: CString,
        column: CString,
        row: i64,
        flags: i32,
    ) -> Result<(u64, BlobIndex)> {
        let conn = self
            .connection
            .as_mut()
            .ok_or(ErrorKind::ConnectionClosed)?;
        let blob = conn.open_blob(database, table, column, row, flags)?;
        let len = blob.size();
        let index = BlobIndex(self.blobs.push(blob));
        Ok((len, index))
    }

    fn read(
        &mut self,
        index: BlobIndex,
        len: usize,
        chunk: &mut Vec<u8>,
        offset: u64,
    ) -> Result<()> {
        self.blobs
            .get_mut(index.0)
            .ok_or(ErrorKind::ConnectionClosed)?
            .read(chunk, offset, len)
    }

    fn write(&mut self, index: BlobIndex, bytes: &[u8], offset: u64) -> Result<()> {
        self.blobs
            .get_mut(index.0)
            .ok_or(ErrorKind::ConnectionClosed)?
            .write(bytes, offset)
    }

    fn reopen(&mut self, index: BlobIndex, row: i64) -> Result<()> {
        self.blobs
            .get_mut(index.0)
            .ok_or(ErrorKind::ConnectionClosed)?
            .reopen(row)
    }

    fn close_blob(&mut self, index: BlobIndex) {
        self.blobs.remove(index.0);
    }
}
