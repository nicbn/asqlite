use crate::{
    blocking::{self, Connection, ConnectionFactory},
    Error, ErrorKind, Result,
};
use std::{
    ffi::CString,
    future::Future,
    pin::Pin,
    sync::{mpsc, Arc, Weak},
    task::{Context, Poll},
    thread,
};

struct Database<C: ConnectionFactory> {
    connection_factory: C,

    connection: Option<C::Connection>,
}

#[derive(Clone)]
pub(crate) struct Sender {
    tx: mpsc::Sender<RequestMessage>,
}

impl Sender {
    pub(crate) async fn open(
        &self,
        path: CString,
        flags: i32,
    ) -> Result<Weak<dyn blocking::InterruptHandle>> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send(RequestMessage::Open { path, flags, tx })
            .map_err(|_| Error::background_task_failed())?;

        rx.await.map_err(|_| Error::background_task_failed())?
    }

    pub(crate) async fn close(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send(RequestMessage::Close { tx: Some(tx) })
            .map_err(|_| Error::background_task_failed())?;

        rx.await.map_err(|_| Error::background_task_failed())
    }

    pub(crate) fn close_drop(&self) {
        let _ = self.tx.send(RequestMessage::Close { tx: None });
    }

    pub(crate) fn call<T: Send + 'static>(
        &self,
        function: impl FnOnce(&mut dyn blocking::Connection) -> Result<T> + Send + 'static,
    ) -> Work<T> {
        let (tx, rx) = oneshot::channel();

        let rx = self
            .tx
            .send(RequestMessage::Call {
                function: Box::new(|inner| match inner {
                    Ok(inner) => {
                        let _ = tx.send(function(inner));
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e));
                    }
                }),
            })
            .ok()
            .map(|_| rx);

        Work { receiver: rx }
    }

    pub(crate) fn call_without_return_value(
        &self,
        function: impl FnOnce(&mut dyn blocking::Connection) + Send + 'static,
    ) {
        let _ = self.tx.send(RequestMessage::Call {
            function: Box::new(|inner| {
                if let Ok(inner) = inner {
                    function(inner);
                }
            }),
        });
    }
}

pub(crate) struct Work<T> {
    receiver: Option<oneshot::Receiver<Result<T>>>,
}

impl<T> Future for Work<T> {
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<T>> {
        match self.receiver.as_mut() {
            Some(receiver) => Pin::new(receiver)
                .poll(cx)
                .map_err(|_| Error::background_task_failed())?,

            None => Poll::Ready(Err(Error::background_task_failed())),
        }
    }
}

enum RequestMessage {
    Open {
        path: CString,
        flags: i32,
        tx: oneshot::Sender<Result<Weak<dyn blocking::InterruptHandle>>>,
    },
    Close {
        tx: Option<oneshot::Sender<()>>,
    },
    Call {
        function: Box<dyn FnOnce(Result<&mut dyn blocking::Connection>) + Send>,
    },
}

pub(crate) fn worker<C>(connection_factory: C) -> Sender
where
    C: ConnectionFactory,
    C: 'static,
{
    let (tx_req, rx_req) = mpsc::channel::<RequestMessage>();

    thread::spawn(move || {
        let mut database = Database {
            connection_factory,
            connection: None,
        };

        while let Ok(x) = rx_req.recv() {
            match x {
                RequestMessage::Open { path, flags, tx } => {
                    let _ = tx.send(match database.connection_factory.open(&path, flags) {
                        Ok(c) => {
                            let interrupt_handle = Arc::downgrade(&c.interrupt_handle());
                            database.connection = Some(c);

                            Ok(interrupt_handle)
                        }
                        Err(e) => Err(e),
                    });
                }
                RequestMessage::Close { tx } => {
                    database.connection = None;
                    if let Some(tx) = tx {
                        let _ = tx.send(());
                    }

                    return;
                }
                RequestMessage::Call { function } => {
                    if let Some(c) = database.connection.as_mut() {
                        function(Ok(c));
                    } else {
                        function(Err(ErrorKind::ConnectionClosed.into()));
                    }
                }
            }
        }
    });
    Sender { tx: tx_req }
}
