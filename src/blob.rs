use crate::{
    internal::{BlobIndex, RequestMessage, Sender},
    Error, ErrorKind, Result,
};
use futures_lite::{AsyncBufRead, AsyncRead, AsyncSeek, AsyncWrite};
use std::{
    ffi::CString,
    fmt,
    future::{self, Future},
    io::{self, SeekFrom},
    mem,
    pin::Pin,
    task::{ready, Context, Poll},
};

const BUFFER_CAPACITY: usize = 1024;

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
/// # tokio_test::block_on(async {
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
    operation: Option<BlobOperation>,

    len: u64,

    offset: u64,

    buf: Vec<u8>,
}

/// Operation.
enum BlobOperation {
    /// Currently reading from the buffer.
    ReadingFromBuffer {
        /// Offset of the read in the buffer.
        index: usize,
    },

    /// Currently writing to the buffer.
    WritingToBuffer {
        /// Offset of the buffer.
        offset: u64,
    },

    /// Currently reading from the database.
    Reading {
        /// Offset of the buffer.
        offset: u64,
        /// Receiver.
        rx: oneshot::Receiver<(Vec<u8>, Result<()>)>,
    },

    /// Currently writing to the database.
    Writing {
        /// Receiver.
        rx: oneshot::Receiver<(Vec<u8>, Result<()>)>,
    },
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
        let (tx, rx) = oneshot::channel();
        tx_req.send(RequestMessage::OpenBlob {
            database,
            table,
            column,
            row,
            flags,
            tx,
        })?;
        let (len, index) = rx.await.map_err(|_| ErrorKind::ConnectionClosed)??;
        Ok(Self {
            tx_req,
            index,
            operation: None,

            len,

            offset: 0,

            buf: Vec::with_capacity(BUFFER_CAPACITY),
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
        loop {
            match &mut self.operation {
                Some(BlobOperation::ReadingFromBuffer { .. })
                | Some(BlobOperation::Reading { .. })
                | None => {
                    self.operation = Some(BlobOperation::WritingToBuffer {
                        offset: self.offset,
                    });
                    self.buf.clear();
                }

                Some(BlobOperation::WritingToBuffer { offset }) => {
                    if self.offset >= self.len {
                        return Poll::Ready(Ok(0));
                    } else if self.offset + self.buf.len() as u64 == *offset {
                        let v = bytes
                            .len()
                            .min(BUFFER_CAPACITY)
                            .min((self.len - self.offset).try_into().unwrap_or(usize::MAX));
                        if v > 0 {
                            self.buf.extend_from_slice(&bytes[..v]);
                            self.offset += v as u64;
                            return Poll::Ready(Ok(v));
                        } else {
                            ready!(self.poll_flush(cx))?;
                        }
                    } else {
                        ready!(self.poll_flush(cx))?;
                    }
                }

                Some(BlobOperation::Writing { .. }) => {
                    ready!(self.poll_flush(cx))?;
                }
            }
        }
    }

    /// Flush written bytes to the database.
    ///
    /// See also: [`flush`](Self::flush).
    pub fn poll_flush(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        loop {
            match &mut self.operation {
                Some(BlobOperation::ReadingFromBuffer { .. }) => return Poll::Ready(Ok(())),

                Some(BlobOperation::WritingToBuffer { offset }) => {
                    if self.buf.is_empty() {
                        self.operation = None;

                        return Poll::Ready(Ok(()));
                    }

                    let (tx, rx) = oneshot::channel();

                    let buf = mem::take(&mut self.buf);
                    self.tx_req.send(RequestMessage::Write {
                        index: self.index,
                        offset: *offset,
                        chunk: buf,
                        tx,
                    })?;
                    self.operation = Some(BlobOperation::Writing { rx });
                }

                Some(BlobOperation::Reading { .. }) => return Poll::Ready(Ok(())),

                Some(BlobOperation::Writing { rx }) => match ready!(Pin::new(rx).poll(cx)) {
                    Ok((b, v)) => {
                        self.buf = b;

                        self.operation = None;

                        return Poll::Ready(v);
                    }
                    Err(_) => {
                        self.operation = None;

                        return Poll::Ready(Err(Error::background_task_failed()));
                    }
                },

                None => return Poll::Ready(Ok(())),
            }
        }
    }

    /// Read the content of the internal buffer, filling it if empty.
    ///
    /// See also: [`fill_buf`](Self::fill_buf).
    pub fn poll_fill_buf(&mut self, cx: &mut Context) -> Poll<Result<&[u8]>> {
        ready!(self.poll_fill_buf_without_returning_buffer(cx))?;

        if let Some(BlobOperation::ReadingFromBuffer { index, .. }) = &self.operation {
            Poll::Ready(Ok(&self.buf[*index..]))
        } else {
            Poll::Ready(Ok(&[]))
        }
    }

    fn poll_fill_buf_without_returning_buffer(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        loop {
            match &mut self.operation {
                Some(BlobOperation::Writing { .. })
                | Some(BlobOperation::WritingToBuffer { .. }) => {
                    ready!(self.poll_flush(cx))?;
                }

                Some(BlobOperation::ReadingFromBuffer { index }) => {
                    let index = *index;
                    if index < self.buf.len() {
                        return Poll::Ready(Ok(()));
                    }

                    self.operation = None;
                }

                Some(BlobOperation::Reading { offset, rx }) => {
                    match ready!(Pin::new(rx).poll(cx)) {
                        Ok((b, Ok(()))) => {
                            self.buf = b;

                            if *offset != self.offset {
                                self.operation = None;
                            } else {
                                self.operation =
                                    Some(BlobOperation::ReadingFromBuffer { index: 0 });
                            }
                        }
                        Ok((b, Err(error))) => {
                            self.buf = b;

                            self.operation = None;

                            return Poll::Ready(Err(error));
                        }
                        Err(_) => {
                            self.operation = None;

                            return Poll::Ready(Err(Error::background_task_failed()));
                        }
                    }
                }

                None => {
                    let (tx, rx) = oneshot::channel();

                    let mut buf = mem::take(&mut self.buf);
                    buf.clear();
                    self.tx_req.send(RequestMessage::Read {
                        index: self.index,
                        len: BUFFER_CAPACITY
                            .min((self.len - self.offset).try_into().unwrap_or(usize::MAX)),
                        chunk: buf,
                        offset: self.offset,
                        tx,
                    })?;
                    self.operation = Some(BlobOperation::Reading {
                        offset: self.offset,
                        rx,
                    });
                }
            }
        }
    }

    /// Consume the internal buffer.
    ///
    /// This function is a lower-level call that should be paired with
    /// [`poll_fill_buf`](Self::poll_fill_buf) to work properly.
    pub fn consume(&mut self, bytes: usize) {
        if let Some(BlobOperation::ReadingFromBuffer { index }) = &mut self.operation {
            self.offset += bytes as u64;
            *index += bytes;
        }
    }

    /// Seek to an offset.
    pub fn seek(&mut self, position: SeekFrom) -> Result<u64> {
        if let Some(BlobOperation::ReadingFromBuffer { .. }) = &self.operation {
            self.operation = None;
        }
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

        if let Some(BlobOperation::ReadingFromBuffer { index, .. }) = &self.operation {
            Ok(&self.buf[*index..])
        } else {
            Ok(&[])
        }
    }

    /// Move the blob to another row, reopening the blob at the selected row.
    pub async fn reopen(&mut self, row: i64) -> Result<()> {
        self.flush().await?;
        self.operation = None;
        let (tx, rx) = oneshot::channel();
        self.tx_req.send(RequestMessage::Reopen {
            index: self.index,
            row,
            tx,
        })?;
        rx.await.map_err(|_| Error::background_task_failed())?
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
        if let Some(BlobOperation::WritingToBuffer { offset }) = self.operation.as_ref() {
            let (tx, _) = oneshot::channel();
            let buf = mem::take(&mut self.buf);
            let _ = self.tx_req.send(RequestMessage::Write {
                index: self.index,
                offset: *offset,
                chunk: buf,
                tx,
            });
        }

        let _ = self
            .tx_req
            .send(RequestMessage::CloseBlob { index: self.index });
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
