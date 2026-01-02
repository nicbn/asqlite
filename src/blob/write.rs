use crate::{
    blocking::{self, BlobIndex},
    worker::{AsyncRequest, Sender},
    Error, ErrorKind, Result,
};
use futures_channel::mpsc;
use futures_core::Stream;
use std::{
    pin::Pin,
    task::{ready, Context, Poll},
};

const CHUNK_LIMIT: usize = 65536;

const CHANNEL_CAP: usize = 8;

/// Blob writer.
pub(crate) struct BlobWrite {
    offset: u64,
    len: u64,
    last_flushed: u64,
    rx_res: mpsc::Receiver<Result<u64>>,
    tx: mpsc::Sender<Box<[u8]>>,
}

impl BlobWrite {
    pub(crate) fn new(tx_req: &Sender, index: BlobIndex, offset: u64, len: u64) -> Self {
        let (tx_res, rx_res) = mpsc::channel(CHANNEL_CAP);
        let (tx, rx) = mpsc::channel(CHANNEL_CAP);

        tx_req.spawn(WriteRequest {
            index,
            offset,
            tx_res,
            rx,
        });

        Self {
            offset,
            len,
            last_flushed: offset,
            rx_res,
            tx,
        }
    }

    pub(crate) fn offset(&self) -> u64 {
        self.offset
    }

    pub(crate) fn poll_write(&mut self, cx: &mut Context, bytes: &[u8]) -> Poll<Result<usize>> {
        // Check responses

        if let Poll::Ready(x) = self.poll_flush(cx) {
            x?;
        }

        if ready!(self.tx.poll_ready(cx)).is_err() {
            return Poll::Ready(Err(ErrorKind::ConnectionClosed.into()));
        }

        // Create chunk

        let size = (usize::try_from(self.len / CHANNEL_CAP as u64).unwrap_or(usize::MAX))
            .min(CHUNK_LIMIT)
            .min(usize::try_from(self.len - self.offset).unwrap_or(usize::MAX))
            .min(bytes.len());
        let chunk = Box::from(&bytes[..size]);

        // Write

        if self.tx.try_send(chunk).is_err() {
            return Poll::Ready(Err(ErrorKind::ConnectionClosed.into()));
        }

        self.offset += size as u64;

        Poll::Ready(Ok(size))
    }

    pub(crate) fn poll_flush(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        while let Poll::Ready(x) = Pin::new(&mut self.rx_res).poll_next(cx) {
            match x {
                Some(Ok(offset)) => self.last_flushed = offset,
                Some(Err(error)) => {
                    return Poll::Ready(Err(error));
                }

                None => {
                    return Poll::Ready(Err(Error::background_task_failed()));
                }
            }
        }

        if self.last_flushed == self.offset {
            return Poll::Ready(Ok(()));
        }

        Poll::Pending
    }
}

struct WriteRequest {
    index: BlobIndex,
    offset: u64,
    tx_res: mpsc::Sender<Result<u64>>,
    rx: mpsc::Receiver<Box<[u8]>>,
}

impl AsyncRequest for WriteRequest {
    fn poll(
        &mut self,
        cx: &mut Context,
        connection: Option<&mut dyn blocking::Connection>,
    ) -> Poll<()> {
        let Some(connection) = connection else {
            let _ = ready!(self.tx_res.poll_ready(cx));
            let _ = self
                .tx_res
                .try_send(Err(ErrorKind::ConnectionClosed.into()));
            return Poll::Ready(());
        };

        let mut write = false;

        loop {
            if ready!(self.tx_res.poll_ready(cx)).is_err() {
                return Poll::Ready(());
            }

            let chunk = match Pin::new(&mut self.rx).poll_next(cx) {
                Poll::Ready(Some(chunk)) => chunk,
                Poll::Ready(None) => {
                    return Poll::Ready(());
                }

                Poll::Pending => {
                    if write {
                        let _ = self.tx_res.try_send(Ok(self.offset));
                    }

                    return Poll::Pending;
                }
            };

            if let Err(error) = connection.write_blob(self.index, &chunk, self.offset) {
                let _ = self.tx_res.try_send(Err(error));
                return Poll::Ready(());
            }

            self.offset += chunk.len() as u64;
            write = true;
        }
    }
}
