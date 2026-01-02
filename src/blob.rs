use crate::{blocking::BlobIndex, worker::Sender, Error, ErrorKind, Result};
use futures_io::{AsyncBufRead, AsyncRead, AsyncSeek, AsyncWrite};
use std::{
    ffi::CString,
    fmt, future,
    io::{self, SeekFrom},
    pin::Pin,
    task::{ready, Context, Poll},
};

mod read;
mod write;

/// Binary blob.
///
/// Can be used to read and write to a blob. Reads and writes are buffered.
///
/// Created by [`Connection::open_blob`].
///
/// # Example
///
/// ```
/// # use std::io;
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// # let mut conn = asqlite::Connection::builder()
/// #    .write(true)
/// #    .create(true)
/// #    .open_memory(":memory")
/// #    .await?;
/// # conn.execute("CREATE TABLE my_table (column BLOB);", ()).await?;
/// use futures::AsyncWriteExt;
///
/// let id = conn.insert("INSERT INTO my_table (column) VALUES (?);",
///     asqlite::params!(asqlite::ZeroBlob(4096))).await?;
/// let mut blob = conn.open_blob(
///     "main",
///     "my_table",
///     "column",
///     id,
///     asqlite::BlobOpenMode::ReadWrite,
/// ).await?;
/// blob.write_all(b"example").await?;
/// blob.flush().await?;
/// # io::Result::<()>::Ok(())
/// # }).unwrap();
/// ```
///
/// [`Connection::open_blob`]: crate::Connection::open_blob
pub struct Blob {
    tx_req: Sender,
    index: BlobIndex,

    len: u64,

    offset: u64,

    op: Option<Operation>,
}

enum Operation {
    Read(read::BlobRead),
    Write(write::BlobWrite),
}

impl Blob {
    pub(crate) async fn open(
        tx_req: Sender,
        database: &str,
        table: &str,
        column: &str,
        row: i64,
        flags: i32,
    ) -> Result<Self> {
        let database = CString::new(database).map_err(|_| {
            Error::new(
                ErrorKind::Generic,
                "database name cannot contain nul characters".to_string(),
            )
        })?;
        let table = CString::new(table).map_err(|_| {
            Error::new(
                ErrorKind::Generic,
                "table name cannot contain nul characters".to_string(),
            )
        })?;
        let column = CString::new(column).map_err(|_| {
            Error::new(
                ErrorKind::Generic,
                "column name cannot contain nul characters".to_string(),
            )
        })?;
        let (len, index) = tx_req
            .call(move |c| {
                let b = c.open_blob(&database, &table, &column, row, flags)?;
                Ok((c.blob_size(b)?, b))
            })
            .await?;
        Ok(Self {
            tx_req,
            index,

            len,

            offset: 0,

            op: None,
        })
    }

    /// Read some bytes into `bytes`.
    ///
    /// See also: [`read`](Self::read).
    pub fn poll_read(&mut self, cx: &mut Context, bytes: &mut [u8]) -> Poll<Result<usize>> {
        let buf = ready!(self.poll_fill_buf(cx))?;
        let v = buf.len().min(bytes.len());
        bytes[..v].copy_from_slice(&buf[..v]);

        self.consume(v);

        Poll::Ready(Ok(v))
    }

    /// Write some bytes into `buffer`.
    ///
    /// See also: [`write`](Self::write).
    pub fn poll_write(&mut self, cx: &mut Context, bytes: &[u8]) -> Poll<Result<usize>> {
        match &mut self.op {
            Some(Operation::Write(w)) if self.offset == w.offset() => {}
            _ => {
                ready!(self.poll_flush(cx))?;
                self.op = Some(Operation::Write(write::BlobWrite::new(
                    &self.tx_req,
                    self.index,
                    self.offset,
                    self.len,
                )));
            }
        }

        match &mut self.op {
            Some(Operation::Write(w)) if self.offset == w.offset() => {
                let size = ready!(w.poll_write(cx, bytes))?;
                self.offset += size as u64;
                Poll::Ready(Ok(size))
            }
            _ => unreachable!(),
        }
    }

    /// Flush written bytes to the database.
    ///
    /// See also: [`flush`](Self::flush).
    pub fn poll_flush(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        match &mut self.op {
            Some(Operation::Write(w)) => w.poll_flush(cx),
            _ => Poll::Ready(Ok(())),
        }
    }

    /// Read the content of the internal buffer, filling it if empty.
    ///
    /// See also: [`fill_buf`](Self::fill_buf).
    pub fn poll_fill_buf(&mut self, cx: &mut Context) -> Poll<Result<&[u8]>> {
        ready!(self.poll_fill_buf_without_returning_buffer(cx))?;

        match &mut self.op {
            Some(Operation::Read(r)) if self.offset == r.offset() => Poll::Ready(Ok(r.buf())),
            _ => unreachable!(),
        }
    }

    fn poll_fill_buf_without_returning_buffer(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        match &mut self.op {
            Some(Operation::Read(r)) if self.offset == r.offset() => {}
            _ => {
                ready!(self.poll_flush(cx))?;
                self.op = Some(Operation::Read(read::BlobRead::new(
                    &self.tx_req,
                    self.index,
                    self.offset,
                    self.len,
                )));
            }
        }

        match &mut self.op {
            Some(Operation::Read(r)) if self.offset == r.offset() => r.poll_fill_buf(cx),
            _ => unreachable!(),
        }
    }

    /// Consume the internal buffer.
    ///
    /// This function is a lower-level call that should be paired with
    /// [`poll_fill_buf`](Self::poll_fill_buf) to work properly.
    pub fn consume(&mut self, bytes: usize) {
        match &mut self.op {
            Some(Operation::Read(r)) if self.offset == r.offset() => r.consume(bytes),
            _ => {}
        }

        // Advance offset

        self.offset += bytes as u64;
    }

    /// Seek to an offset.
    pub fn seek(&mut self, position: SeekFrom) -> Result<u64> {
        match position {
            SeekFrom::Current(v) => self.offset = ((self.offset as i64) + v) as u64,
            SeekFrom::End(v) => self.offset = ((self.len as i64) + v) as u64,
            SeekFrom::Start(v) => self.offset = v,
        }

        Ok(self.offset)
    }

    /// Return the size of the binary blob.
    pub fn size(&self) -> u64 {
        self.len
    }

    /// Read some bytes into `buffer`.
    ///
    /// Returns the number of bytes read, or 0 when the EOF is reached.
    pub async fn read(&mut self, bytes: &mut [u8]) -> Result<usize> {
        future::poll_fn(|cx| self.poll_read(cx, bytes)).await
    }

    /// Write some bytes into `buffer`.
    ///
    /// Returns amount of bytes written.
    pub async fn write(&mut self, bytes: &[u8]) -> Result<usize> {
        future::poll_fn(|cx| self.poll_write(cx, bytes)).await
    }

    /// Flush written bytes to the database.
    pub async fn flush(&mut self) -> Result<()> {
        future::poll_fn(|cx| self.poll_flush(cx)).await
    }

    /// Read the content of the internal buffer, filling it if empty.
    ///
    /// This function is a lower-level call that should be paired with
    /// [`consume`](Self::consume) to work properly.
    pub async fn fill_buf(&mut self) -> Result<&[u8]> {
        future::poll_fn(|cx| self.poll_fill_buf_without_returning_buffer(cx)).await?;

        match &mut self.op {
            Some(Operation::Read(r)) if self.offset == r.offset() => Ok(r.buf()),
            _ => unreachable!(),
        }
    }

    /// Move the blob to another row, reopening the blob at the selected row.
    pub async fn reopen(&mut self, row: i64) -> Result<()> {
        self.flush().await?;
        let index = self.index;
        self.op = None;
        self.tx_req.call(move |c| c.reopen_blob(index, row)).await
    }
}

impl fmt::Debug for Blob {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Blob")
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}

impl Drop for Blob {
    fn drop(&mut self) {
        let index = self.index;
        self.tx_req.call_without_return_value(move |c| {
            c.close_blob(index);
        });
    }
}

impl AsyncRead for Blob {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().poll_read(cx, buf).map_err(io::Error::from)
    }
}

impl AsyncWrite for Blob {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.get_mut().poll_write(cx, buf).map_err(io::Error::from)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        self.get_mut().poll_flush(cx).map_err(io::Error::from)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}

impl AsyncSeek for Blob {
    fn poll_seek(self: Pin<&mut Self>, _: &mut Context, pos: SeekFrom) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.get_mut().seek(pos)?))
    }
}

impl AsyncBufRead for Blob {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<&[u8]>> {
        self.get_mut().poll_fill_buf(cx).map_err(io::Error::from)
    }

    fn consume(self: Pin<&mut Self>, bytes: usize) {
        self.get_mut().consume(bytes);
    }
}
