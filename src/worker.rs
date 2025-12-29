use crate::{
    blocking::{self, Connection, ConnectionFactory},
    utils::block_on::block_on,
    Error, ErrorKind, Result,
};
use futures_channel::{mpsc, oneshot};
use futures_core::Stream;
use std::{
    ffi::CString,
    future::Future,
    pin::Pin,
    sync::{Arc, Weak},
    task::{Context, Poll},
    thread,
};

struct Database<C: ConnectionFactory> {
    connection_factory: C,

    connection: Option<C::Connection>,
}

pub(crate) trait AsyncRequest: Send {
    fn poll(
        &mut self,
        cx: &mut Context,
        connection: Option<&mut dyn blocking::Connection>,
    ) -> Poll<()>;
}

#[derive(Clone)]
pub(crate) struct Sender {
    tx: mpsc::UnboundedSender<RequestMessage>,
}

impl Sender {
    pub(crate) async fn open(
        &self,
        path: CString,
        flags: i32,
    ) -> Result<Weak<dyn blocking::InterruptHandle>> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .unbounded_send(RequestMessage::Open { path, flags, tx })
            .map_err(|_| Error::background_task_failed())?;

        rx.await.map_err(|_| Error::background_task_failed())?
    }

    pub(crate) async fn close(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .unbounded_send(RequestMessage::Close { tx: Some(tx) })
            .map_err(|_| Error::background_task_failed())?;

        rx.await.map_err(|_| Error::background_task_failed())
    }

    pub(crate) fn close_drop(&self) {
        let _ = self.tx.unbounded_send(RequestMessage::Close { tx: None });
    }

    pub(crate) fn call<T: Send + 'static>(
        &self,
        function: impl FnOnce(&mut dyn blocking::Connection) -> Result<T> + Send + 'static,
    ) -> Work<T> {
        let (tx, rx) = oneshot::channel();

        let _ = self.tx.unbounded_send(RequestMessage::Call {
            function: Box::new(|inner| match inner {
                Ok(inner) => {
                    let _ = tx.send(function(inner));
                }
                Err(e) => {
                    let _ = tx.send(Err(e));
                }
            }),
        });

        Work { receiver: rx }
    }

    pub(crate) fn spawn<T: AsyncRequest + 'static>(&self, future: T) {
        let _ = self.tx.unbounded_send(RequestMessage::Spawn {
            future: Box::new(future),
        });
    }

    pub(crate) fn call_without_return_value(
        &self,
        function: impl FnOnce(&mut dyn blocking::Connection) + Send + 'static,
    ) {
        let _ = self.tx.unbounded_send(RequestMessage::Call {
            function: Box::new(|inner| {
                if let Ok(inner) = inner {
                    function(inner);
                }
            }),
        });
    }
}

pub(crate) struct Work<T> {
    receiver: oneshot::Receiver<Result<T>>,
}

impl<T> Future for Work<T> {
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<T>> {
        Pin::new(&mut self.receiver)
            .poll(cx)
            .map_err(|_| Error::background_task_failed())?
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
    Spawn {
        future: Box<dyn AsyncRequest>,
    },
}

pub(crate) fn worker<C>(connection_factory: C) -> Sender
where
    C: ConnectionFactory,
    C: 'static,
{
    let (tx_req, rx_req) = mpsc::unbounded::<RequestMessage>();

    thread::spawn(move || {
        block_on(Worker {
            rx_req,

            database: Database {
                connection_factory,
                connection: None,
            },

            futures: Vec::new(),
        })
    });

    Sender { tx: tx_req }
}

struct Worker<C>
where
    C: ConnectionFactory,
    C: 'static,
{
    rx_req: mpsc::UnboundedReceiver<RequestMessage>,

    database: Database<C>,

    futures: Vec<Box<dyn AsyncRequest>>,
}

impl<C> Future for Worker<C>
where
    C: ConnectionFactory,
    C: 'static,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let Self {
            rx_req,
            database,
            futures,
        } = self.get_mut();

        while let Poll::Ready(x) = Pin::new(&mut *rx_req).poll_next(cx) {
            match x {
                Some(RequestMessage::Open { path, flags, tx }) => {
                    let _ = tx.send(match database.connection_factory.open(&path, flags) {
                        Ok(c) => {
                            let interrupt_handle = Arc::downgrade(&c.interrupt_handle());
                            database.connection = Some(c);

                            Ok(interrupt_handle)
                        }
                        Err(e) => Err(e),
                    });
                }
                Some(RequestMessage::Close { tx }) => {
                    database.connection = None;
                    if let Some(tx) = tx {
                        let _ = tx.send(());
                    }

                    return Poll::Ready(());
                }
                Some(RequestMessage::Call { function }) => {
                    if let Some(c) = database.connection.as_mut() {
                        function(Ok(c));
                    } else {
                        function(Err(ErrorKind::ConnectionClosed.into()));
                    }
                }
                Some(RequestMessage::Spawn { future }) => {
                    futures.push(future);
                }
                None => return Poll::Ready(()),
            }
        }

        futures.retain_mut(|future| {
            future
                .as_mut()
                .poll(
                    cx,
                    database
                        .connection
                        .as_mut()
                        .map(|c| c as &mut dyn blocking::Connection),
                )
                .is_pending()
        });

        Poll::Pending
    }
}
