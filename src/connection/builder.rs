use super::Connection;
use crate::{Error, ErrorKind, Result};
use std::path::Path;

/// Options for building a [`Connection`].
#[derive(Clone, Debug)]
pub struct ConnectionBuilder {
    write: bool,
    create: bool,
    shared_cache: bool,
    statement_cache_capacity: usize,
}

impl ConnectionBuilder {
    /// Create a new connection builder.
    ///
    /// # Example
    ///
    /// ```
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let conn = asqlite::ConnectionBuilder::new()
    ///     .write(true)
    ///     .create(true)
    ///     .open_memory(":memory")
    ///     .await?;
    /// # asqlite::Result::<()>::Ok(())
    /// # }).unwrap();
    /// ```
    #[inline]
    pub fn new() -> Self {
        Self {
            write: false,
            create: false,
            shared_cache: false,
            statement_cache_capacity: 8,
        }
    }

    /// Allow writing to the database.
    ///
    /// If `false`, database is opened in read-only mode.
    ///
    /// By default, `false`.
    ///
    /// # Example
    ///
    /// ```
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let conn = asqlite::ConnectionBuilder::new()
    ///     .write(true)
    ///     .open_memory(":memory")
    ///     .await?;
    /// # asqlite::Result::<()>::Ok(())
    /// # }).unwrap();
    /// ```
    #[inline]
    pub fn write(mut self, write: bool) -> Self {
        self.write = write;
        self
    }

    /// Create the database if it does not already exist.
    ///
    /// Does not do anything unless [`write`](Self::write) is also `true`.
    ///
    /// By default, `false`.
    ///
    /// # Example
    ///
    /// ```
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let conn = asqlite::ConnectionBuilder::new()
    ///     .write(true)
    ///     .create(true)
    ///     .open_memory(":memory")
    ///     .await?;
    /// # asqlite::Result::<()>::Ok(())
    /// # }).unwrap();
    /// ```
    #[inline]
    pub fn create(mut self, create: bool) -> Self {
        self.create = create;
        self
    }

    /// Set whether shared cache will be used. Shared cache usage is
    /// discouraged by SQLite3 docs.
    ///
    /// By default, `false`.
    ///
    /// # Example
    ///
    /// ```
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let conn = asqlite::ConnectionBuilder::new()
    ///     .shared_cache(true)
    ///     .open_memory(":memory")
    ///     .await?;
    /// # asqlite::Result::<()>::Ok(())
    /// # }).unwrap();
    /// ```
    #[inline]
    pub fn shared_cache(mut self, shared_cache: bool) -> Self {
        self.shared_cache = shared_cache;
        self
    }

    /// Set the statement cache capacity.
    ///
    /// By default, `8`.
    ///
    /// # Example
    ///
    /// ```
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let conn = asqlite::ConnectionBuilder::new()
    ///     .statement_cache_capacity(1024)
    ///     .open_memory(":memory")
    ///     .await?;
    /// # asqlite::Result::<()>::Ok(())
    /// # }).unwrap();
    /// ```
    #[inline]
    pub fn statement_cache_capacity(mut self, statement_cache_capacity: usize) -> Self {
        self.statement_cache_capacity = statement_cache_capacity;
        self
    }

    /// Open a database from a filesystem path.
    ///
    /// The path will not be interpreted as an URI, unlike [`open_uri`](Self::open_uri).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let conn = asqlite::ConnectionBuilder::new()
    ///     .open("/path/FileOfMyDatabase.sqlite3")
    ///     .await?;
    /// # asqlite::Result::<()>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn open(self, path: impl AsRef<Path>) -> Result<Connection> {
        let path = path.as_ref().to_str().ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidPath,
                "path must be convertible to utf-8".to_string(),
            )
        })?;

        let mut pathbuf = String::with_capacity(path.len() + 1);
        pathbuf.push_str(path);

        Connection::open(pathbuf, self.flags(), self.statement_cache_capacity).await
    }

    /// Open a database from a URI.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let conn = asqlite::ConnectionBuilder::new()
    ///     .open_uri("file:/path/FileOfMyDatabase.sqlite3")
    ///     .await?;
    /// # asqlite::Result::<()>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn open_uri(self, path: impl AsRef<Path>) -> Result<Connection> {
        let path = path.as_ref().to_str().ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidPath,
                "path must be convertible to utf-8".to_string(),
            )
        })?;

        let mut pathbuf = String::with_capacity(path.len() + 1);
        pathbuf.push_str(path);

        Connection::open(
            pathbuf,
            self.flags() | libsqlite3_sys::SQLITE_OPEN_URI,
            self.statement_cache_capacity,
        )
        .await
    }

    /// Open a database from memory.
    ///
    /// # Example
    ///
    /// ```
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let conn = asqlite::ConnectionBuilder::new()
    ///     .open_memory(":memory")
    ///     .await?;
    /// # asqlite::Result::<()>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn open_memory(self, name: &str) -> Result<Connection> {
        Connection::open(
            name.to_string(),
            self.flags() | libsqlite3_sys::SQLITE_OPEN_MEMORY,
            self.statement_cache_capacity,
        )
        .await
    }

    fn flags(&self) -> i32 {
        let mut x = libsqlite3_sys::SQLITE_OPEN_NOMUTEX;

        x |= if self.write && self.create {
            libsqlite3_sys::SQLITE_OPEN_CREATE | libsqlite3_sys::SQLITE_OPEN_READWRITE
        } else if self.write {
            libsqlite3_sys::SQLITE_OPEN_READWRITE
        } else {
            libsqlite3_sys::SQLITE_OPEN_READONLY
        };

        x |= if self.shared_cache {
            libsqlite3_sys::SQLITE_OPEN_SHAREDCACHE
        } else {
            libsqlite3_sys::SQLITE_OPEN_PRIVATECACHE
        };

        x
    }
}

impl Default for ConnectionBuilder {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}
