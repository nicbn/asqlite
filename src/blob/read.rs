use crate::{
    blocking::{self, BlobIndex},
    worker::{AsyncRequest, Sender},
    ErrorKind, Result,
};
use futures_channel::mpsc;
use futures_core::Stream;
use std::{
    iter,
    pin::Pin,
    task::{ready, Context, Poll},
};

const CHUNK_LIMIT: usize = 65536;

const CHANNEL_CAP: usize = 8;

/// Blob reader.
pub(crate) struct BlobRead {
    /// Current buffer being read.
    buf: Option<Box<[u8]>>,

    /// Offset being read.
    offset: u64,

    /// Offset of the read in the buffer.
    buffer_offset: usize,

    /// Receiver
    rx: mpsc::Receiver<Result<Box<[u8]>>>,
}

impl BlobRead {
    pub(crate) fn new(tx_req: &Sender, index: BlobIndex, offset: u64, len: u64) -> Self {
        let (tx, rx) = mpsc::channel(CHANNEL_CAP);

        tx_req.spawn(ReadRequest {
            index,
            len,
            offset,
            tx,
        });

        Self {
            buf: None,

            offset,

            buffer_offset: 0,

            rx,
        }
    }

    pub(crate) fn offset(&self) -> u64 {
        self.offset
    }

    pub(crate) fn poll_fill_buf(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        if let Some(buf) = &self.buf {
            if self.buffer_offset < buf.len() {
                return Poll::Ready(Ok(()));
            } else {
                self.buf = None;
            }
        }
        match ready!(Pin::new(&mut self.rx).poll_next(cx)) {
            Some(Ok(c)) => {
                self.buffer_offset = 0;
                self.buf = Some(c);
            }
            Some(Err(error)) => {
                return Poll::Ready(Err(error));
            }
            None => {}
        }
        Poll::Ready(Ok(()))
    }

    pub(crate) fn buf(&self) -> &[u8] {
        if let Some(buf) = &self.buf {
            if self.buffer_offset < buf.len() {
                return &buf[self.buffer_offset..];
            }
        }
        &[]
    }

    pub(crate) fn consume(&mut self, bytes: usize) {
        self.offset += bytes as u64;
        self.buffer_offset += bytes;
    }
}

struct ReadRequest {
    index: BlobIndex,
    len: u64,
    offset: u64,
    tx: mpsc::Sender<Result<Box<[u8]>>>,
}

impl AsyncRequest for ReadRequest {
    fn poll(
        &mut self,
        cx: &mut Context,
        connection: Option<&mut dyn blocking::Connection>,
    ) -> Poll<()> {
        let Some(connection) = connection else {
            let _ = ready!(self.tx.poll_ready(cx));
            let _ = self.tx.try_send(Err(ErrorKind::ConnectionClosed.into()));
            return Poll::Ready(());
        };

        loop {
            if ready!(self.tx.poll_ready(cx)).is_err() {
                return Poll::Ready(());
            }

            let size = (usize::try_from(self.len / CHANNEL_CAP as u64).unwrap_or(usize::MAX))
                .min(CHUNK_LIMIT)
                .min(usize::try_from(self.len - self.offset).unwrap_or(usize::MAX));

            if size == 0 {
                return Poll::Ready(());
            }

            let mut buf = iter::repeat_n(0, size).collect::<Box<[u8]>>();

            if let Err(error) = connection.read_blob(self.index, &mut buf, self.offset) {
                let _ = self.tx.try_send(Err(error));
                return Poll::Ready(());
            }

            self.offset += size as u64;

            let _ = self.tx.try_send(Ok(buf));
        }
    }
}
