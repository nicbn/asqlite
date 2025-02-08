use self::statement_cache::StatementCache;
use crate::{
    convert::{FromRow, IntoStatement, ParamList, __StrOrStatement},
    internal::{worker, RequestMessage, Sender},
    statement::PrepareWith,
    Blob, BlobOpenMode, Error, ErrorKind, Result, Statement,
};
use futures_lite::{Stream, StreamExt};
use std::{
    cmp::Ordering,
    ffi::CString,
    fmt,
    future::{self, Future},
    mem,
    pin::Pin,
    sync::{Arc, Weak},
    task::{ready, Context, Poll},
    time::Duration,
};

mod builder;
mod statement_cache;

pub use self::builder::*;

/// A SQLite3 connection.
///
/// # Example
///
/// ```
/// # tokio_test::block_on(async {
/// let mut conn = asqlite::Connection::builder()
///     .write(true)
///     .create(true)
///     .open_memory(":memory")
///     .await?;
/// conn.execute("CREATE TABLE person (name TEXT, age INTEGER)", ())
///     .await?;
/// # asqlite::Result::<()>::Ok(())
/// # }).unwrap();
/// ```
pub struct Connection {
    pub(crate) tx_req: Sender,
    connection_handle: Weak<crate::internal::ConnectionHandle>,

    cache: StatementCache,
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection").finish_non_exhaustive()
    }
}

impl Connection {
    #[inline]
    async fn open(path: String, flags: i32, cache_capacity: usize) -> Result<Self> {
        let tx_req = worker();

        let path = CString::new(path).map_err(|_| {
            Error::new(
                ErrorKind::InvalidPath,
                "path cannot contain nul characters".to_string(),
            )
        })?;
        let (tx, rx) = oneshot::channel();
        tx_req
            .send(RequestMessage::Open { path, flags, tx })
            .map_err(|_| Error::background_task_failed())?;
        let connection_handle = rx.await.map_err(|_| Error::background_task_failed())?;

        Ok(Self {
            tx_req,
            connection_handle: connection_handle?,

            cache: StatementCache::new(cache_capacity),
        })
    }

    /// Create a new [`ConnectionBuilder`].
    ///
    /// Shorthand for [`ConnectionBuilder::new`].
    ///
    /// # Example
    ///
    /// ```
    /// # tokio_test::block_on(async {
    /// let conn = asqlite::Connection::builder()
    ///     .write(true)
    ///     .create(true)
    ///     .open_memory(":memory")
    ///     .await?;
    /// # asqlite::Result::<()>::Ok(())
    /// # }).unwrap();
    /// ```
    pub fn builder() -> ConnectionBuilder {
        ConnectionBuilder::new()
    }

    /// Get copy of the interrupt handle.
    pub fn interrupt_handle(&self) -> InterruptHandle {
        InterruptHandle {
            connection_handle: self.connection_handle.clone(),
        }
    }

    /// Close the connection.
    pub async fn close(self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx_req
            .send(RequestMessage::Close { tx: Some(tx) })
            .map_err(|_| Error::background_task_failed())?;
        rx.await.map_err(|_| Error::background_task_failed())?;
        Ok(())
    }

    /// Prepare a statement.
    ///
    /// For most applications it is not necessary to directly use `prepare`,
    /// as `Connection` already implements a prepared statement cache.
    /// This method is provided for use cases that need extra performance
    /// or manual control over the prepared statement.
    ///
    /// If `cache` is `true`, the prepared statement is added to the statement
    /// cache.
    ///
    /// Fails if there is more than one statement.
    ///
    /// # Example
    ///
    /// ```
    /// # tokio_test::block_on(async {
    /// # let mut conn = asqlite::Connection::builder()
    /// #    .write(true)
    /// #    .create(true)
    /// #    .open_memory(":memory")
    /// #    .await?;
    /// # conn.execute("CREATE TABLE user (user_id INT, secret TEXT)", ()).await?;
    /// # conn.execute("INSERT INTO user (user_id, secret) VALUES (1, ':)')", ()).await?;
    /// #
    /// use futures::TryStreamExt;
    ///
    /// let statement = conn.prepare(
    ///     "SELECT secret FROM user WHERE user_id = ?",
    ///     true,
    /// ).await?;
    ///
    /// let mut rows = conn.query(statement, asqlite::params!(1));
    /// let secret: Option<String> = rows.try_next().await?;
    /// # asqlite::Result::<()>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn prepare(&mut self, sql: &str, cache: bool) -> Result<Statement> {
        self.prepare_with(sql, cache).await
    }

    pub(crate) fn prepare_with(&mut self, sql: &str, cache: bool) -> Prepare {
        if let Some(cache) = self.cache.get(sql) {
            return Prepare {
                state: PrepareState::Cached {
                    statement: Some(cache),
                },
            };
        }

        let sql: Arc<str> = Arc::from(sql);
        let cache = if cache { Some(sql.clone()) } else { None };
        Prepare {
            state: PrepareState::New {
                inner: Statement::prepare_with(self.tx_req.clone(), sql),

                cache,

                connection: self,
            },
        }
    }

    fn statement(&mut self, statement: impl IntoStatement) -> IntoStatementStruct {
        let state = match IntoStatement::__into_str_or_statement(&statement) {
            __StrOrStatement::Str(v) => IntoStatementState::Prepare(self.prepare_with(v, true)),
            __StrOrStatement::Statement(v) => IntoStatementState::Statement(v),
        };

        IntoStatementStruct { state }
    }

    /// Prepare multiple statements, ignoring the statement cache.
    ///
    /// # Example
    ///
    /// ```
    /// # tokio_test::block_on(async {
    /// # let mut conn = asqlite::Connection::builder()
    /// #    .write(true)
    /// #    .create(true)
    /// #    .open_memory(":memory")
    /// #    .await?;
    /// # conn.execute("CREATE TABLE table_1 (id INT)", ()).await?;
    /// # conn.execute("CREATE TABLE table_2 (id INT)", ()).await?;
    /// use futures::TryStreamExt;
    ///
    /// let mut statements = conn.prepare_batch(
    ///     r#"SELECT * FROM table_1 WHERE id = ?;
    ///        SELECT * FROM table_2 WHERE id = ?;;"#,
    /// );
    ///
    /// let statement_1 = statements.try_next().await?.unwrap();
    /// let statement_2 = statements.try_next().await?.unwrap();
    /// # asqlite::Result::<()>::Ok(())
    /// # }).unwrap();
    pub fn prepare_batch(
        &mut self,
        sql: &str,
    ) -> impl Stream<Item = Result<Statement>> + Send + Unpin {
        Statement::prepare_batch(self.tx_req.clone(), Arc::from(sql))
    }

    /// Execute a statement, returns the count of modified rows.
    ///
    /// Fails if there is more than one statement.
    ///
    /// # Example
    ///
    /// ```
    /// # tokio_test::block_on(async {
    /// # let mut conn = asqlite::Connection::builder()
    /// #    .write(true)
    /// #    .create(true)
    /// #    .open_memory(":memory")
    /// #    .await?;
    /// conn.execute("CREATE TABLE user (user_id INT, secret TEXT)", ()).await?;
    /// # asqlite::Result::<()>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn execute(
        &mut self,
        statement: impl IntoStatement,
        param_list: impl Into<ParamList>,
    ) -> Result<u32> {
        self.statement(statement)
            .await?
            .execute(param_list.into())
            .await
    }

    /// Execute multiple statements, ignoring the statement cache.
    ///
    /// # Example
    ///
    /// ```
    /// # tokio_test::block_on(async {
    /// # let mut conn = asqlite::Connection::builder()
    /// #    .write(true)
    /// #    .create(true)
    /// #    .open_memory(":memory")
    /// #    .await?;
    /// conn.execute_batch(
    ///     r#"CREATE TABLE user (user_id INT, secret TEXT);
    ///        CREATE TABLE product (key TEXT);"#,
    /// ).await?;
    /// # asqlite::Result::<()>::Ok(())
    /// # }).unwrap();
    pub async fn execute_batch(&mut self, sql: &str) -> Result<u32> {
        let mut accumulator = 0;

        let mut batch = self.prepare_batch(sql);

        while let Some(v) = future::poll_fn(|cx| Pin::new(&mut batch).poll_next(cx)).await {
            let v = v?;
            accumulator += v.execute(ParamList::from(())).await?;
        }

        Ok(accumulator)
    }

    /// Execute a statement, returns the inserted row id.
    ///
    /// Fails if there is more than one statement.
    ///
    /// # Example
    ///
    /// ```
    /// # tokio_test::block_on(async {
    /// # let mut conn = asqlite::Connection::builder()
    /// #    .write(true)
    /// #    .create(true)
    /// #    .open_memory(":memory")
    /// #    .await?;
    /// # conn.execute("CREATE TABLE table_1 (id INT)", ()).await?;
    /// let row = conn.insert(
    ///     "INSERT INTO table_1 (id) VALUES (?)",
    ///     asqlite::params!(1),
    /// ).await?;
    /// # asqlite::Result::<()>::Ok(())
    /// # }).unwrap();
    pub async fn insert(
        &mut self,
        statement: impl IntoStatement,
        param_list: impl Into<ParamList>,
    ) -> Result<i64> {
        self.statement(statement)
            .await?
            .insert(param_list.into())
            .await
    }

    /// Execute a statement, returns each row.
    ///
    /// Fails if there is more than one statement.
    ///
    /// # Example
    ///
    /// ```
    /// # tokio_test::block_on(async {
    /// # let mut conn = asqlite::Connection::builder()
    /// #    .write(true)
    /// #    .create(true)
    /// #    .open_memory(":memory")
    /// #    .await?;
    /// # conn.execute("CREATE TABLE user (user_id INT, secret TEXT)", ()).await?;
    /// # conn.execute("INSERT INTO user (user_id, secret) VALUES (1, ':)')", ()).await?;
    /// #
    /// use futures::TryStreamExt;
    ///
    /// let mut rows = conn.query(
    ///     "SELECT secret FROM user WHERE user_id = ?",
    ///     asqlite::params!(1),
    /// );
    /// let secret: Option<String> = rows.try_next().await?;
    /// # asqlite::Result::<()>::Ok(())
    /// # }).unwrap();
    /// ```
    pub fn query<'a, R: FromRow + Send + 'static>(
        &'a mut self,
        statement: impl IntoStatement + 'a,
        param_list: impl Into<ParamList>,
    ) -> impl Stream<Item = Result<R>> + Send + Unpin + 'a {
        let param_list = param_list.into();

        Rows {
            state: RowsState::Preparing(self.statement(statement), param_list),
        }
    }

    /// Execute a statement, returns only the first row of the result.
    ///
    /// Fails if there is more than one statement.
    ///
    /// # Example
    ///
    /// ```
    /// # tokio_test::block_on(async {
    /// # let mut conn = asqlite::Connection::builder()
    /// #    .write(true)
    /// #    .create(true)
    /// #    .open_memory(":memory")
    /// #    .await?;
    /// # conn.execute("CREATE TABLE user (user_id INT, secret TEXT)", ()).await?;
    /// # conn.execute("INSERT INTO user (user_id, secret) VALUES (1, ':)')", ()).await?;
    /// #
    /// use futures::TryStreamExt;
    ///
    /// let secret: Option<String> = conn.query_first(
    ///     "SELECT secret FROM user WHERE user_id = ?",
    ///     asqlite::params!(1),
    /// ).await?;
    /// # asqlite::Result::<()>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn query_first<R: FromRow + Send + 'static>(
        &mut self,
        statement: impl IntoStatement,
        param_list: impl Into<ParamList>,
    ) -> Result<Option<R>> {
        self.query(statement, param_list).try_next().await
    }

    /// Reset the connection busy handler.
    pub fn reset_busy_handler(&mut self) -> Result<()> {
        self.tx_req.send(RequestMessage::ResetBusyHandler)
    }

    /// Set the connection busy handler.
    ///
    /// The first parameter is the amount of times the busy handler has been invoked
    /// previously for the same locking event.
    ///
    /// If returns true, another attempt is made to access the database.
    /// Otherwise [`DatabaseBusy`](ErrorKind::DatabaseBusy) is returned.
    pub fn set_busy_handler(&mut self, f: impl FnMut(u32) -> bool + Send + 'static) -> Result<()> {
        self.tx_req.send(RequestMessage::SetBusyHandler {
            handler: Box::new(f),
        })
    }

    /// Set the connection busy timeout.
    pub fn set_busy_timeout(&mut self, busy_timeout: Duration) -> Result<()> {
        self.tx_req.send(RequestMessage::SetBusyTimeout {
            duration: busy_timeout,
        })
    }

    /// Create a collation.
    pub async fn create_collation(
        &mut self,
        name: &str,
        f: impl FnMut(&str, &str) -> Ordering + Send + 'static,
    ) -> Result<()> {
        let name = CString::new(name).map_err(|_| {
            Error::new(
                ErrorKind::Generic,
                "collation name must not contain nul".to_string(),
            )
        })?;
        let (tx, rx) = oneshot::channel();
        self.tx_req.send(RequestMessage::CreateCollation {
            name,
            callback: Box::new(f),
            tx,
        })?;
        rx.await.map_err(|_| Error::background_task_failed())??;
        Ok(())
    }

    /// Flush the disk cache.
    pub async fn flush_disk_cache(&mut self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx_req.send(RequestMessage::FlushDiskCache { tx })?;
        rx.await.map_err(|_| Error::background_task_failed())??;
        Ok(())
    }

    /// Open a binary blob.
    ///
    /// The blob can be read and written to, but its size cannot be changed.
    ///
    /// # Example
    ///
    /// ```
    /// # tokio_test::block_on(async {
    /// # let mut conn = asqlite::Connection::builder()
    /// #    .write(true)
    /// #    .create(true)
    /// #    .open_memory(":memory")
    /// #    .await?;
    /// # conn.execute("CREATE TABLE my_table (column BLOB);", ()).await?;
    /// let id = conn.insert("INSERT INTO my_table (column) VALUES (?);",
    ///     asqlite::params!(asqlite::ZeroBlob(4096))).await?;
    /// let blob = conn.open_blob(
    ///     "main",
    ///     "my_table",
    ///     "column",
    ///     id,
    ///     asqlite::BlobOpenMode::ReadWrite,
    /// ).await?;
    /// # asqlite::Result::<()>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn open_blob(
        &mut self,
        database: &str,
        table: &str,
        column: &str,
        row: i64,
        open_mode: BlobOpenMode,
    ) -> Result<Blob> {
        Blob::open(
            self.tx_req.clone(),
            database,
            table,
            column,
            row,
            open_mode as u8 as i32,
        )
        .await
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.cache.clear();
        let _ = self.tx_req.send(RequestMessage::Close { tx: None });
    }
}

/// Interrupt handle for a connection.
#[derive(Clone)]
pub struct InterruptHandle {
    connection_handle: Weak<crate::internal::ConnectionHandle>,
}

impl fmt::Debug for InterruptHandle {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("InterruptHandle").finish_non_exhaustive()
    }
}

impl InterruptHandle {
    /// Interrupt.
    ///
    /// Interrupting an operation is inherently prone to race conditions.
    pub fn interrupt(&self) {
        if let Some(connection_handle) = self.connection_handle.upgrade() {
            connection_handle.interrupt();
        }
    }
}

pub(crate) struct Prepare<'a> {
    state: PrepareState<'a>,
}

enum PrepareState<'a> {
    New {
        inner: PrepareWith,
        cache: Option<Arc<str>>,
        connection: &'a mut Connection,
    },
    Cached {
        statement: Option<Statement>,
    },
}

impl Future for Prepare<'_> {
    type Output = Result<Statement>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match &mut self.state {
            PrepareState::New {
                inner,
                cache,
                connection,
            } => {
                let statement = ready!(Pin::new(inner).poll(cx))?;

                if let Some(sql) = cache.take() {
                    connection.cache.insert(sql, statement.clone());
                }

                Poll::Ready(Ok(statement))
            }

            PrepareState::Cached { statement } => Poll::Ready(Ok(statement.take().unwrap())),
        }
    }
}

pub(crate) struct IntoStatementStruct<'a> {
    state: IntoStatementState<'a>,
}

enum IntoStatementState<'a> {
    Statement(Statement),
    Prepare(Prepare<'a>),
}

impl Future for IntoStatementStruct<'_> {
    type Output = Result<Statement>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match &mut self.state {
            IntoStatementState::Statement(v) => Poll::Ready(Ok(v.clone())),
            IntoStatementState::Prepare(x) => Pin::new(x).poll(cx),
        }
    }
}

struct Rows<'a, R> {
    state: RowsState<'a, R>,
}

enum RowsState<'a, R> {
    Preparing(IntoStatementStruct<'a>, ParamList),

    Rows(crate::statement::Rows<R>),

    Finish,
}

impl<R> Stream for Rows<'_, R>
where
    R: FromRow,
    R: Send + 'static,
{
    type Item = Result<R>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Result<R>>> {
        loop {
            match &mut self.state {
                RowsState::Preparing(x, _) => match ready!(Pin::new(x).poll(cx)) {
                    Ok(v) => {
                        let RowsState::Preparing(_, param_list) =
                            mem::replace(&mut self.state, RowsState::Finish)
                        else {
                            panic!()
                        };

                        self.state = RowsState::Rows(v.query_with(param_list));
                    }

                    Err(r) => {
                        self.state = RowsState::Finish;

                        return Poll::Ready(Some(Err(r)));
                    }
                },

                RowsState::Rows(x) => return Pin::new(x).poll_next(cx),

                RowsState::Finish => return Poll::Ready(None),
            }
        }
    }
}
