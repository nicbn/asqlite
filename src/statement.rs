use crate::{
    convert::{FromRow, ParamList},
    internal::{RequestMessage, Sender, StatementIndex},
    Error, ErrorKind, Result,
};
use futures_lite::Stream;
use std::{
    fmt,
    future::Future,
    mem,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};

/// Prepared statement.
///
/// Created by [`Connection::prepare`].
///
/// [`Connection::prepare`]: crate::Connection::prepare
#[derive(Clone)]
pub struct Statement(Arc<StatementInner>);

struct StatementInner {
    tx_req: Sender,
    index: StatementIndex,
}

impl Statement {
    pub(crate) fn prepare_with(tx_req: Sender, sql: Arc<str>) -> PrepareWith {
        PrepareWith {
            tx_req,
            state: PrepareState::MessageNotSent,
            sql,
        }
    }

    pub(crate) fn prepare_batch(tx_req: Sender, sql: Arc<str>) -> Batch {
        Batch {
            sql,
            index_of_start: 0,
            tx_req,
            state: BatchState::MessageNotSent,
        }
    }

    pub(crate) async fn execute(&self, param_list: ParamList) -> Result<u32> {
        let (tx, rx) = oneshot::channel();
        self.0.tx_req.send(RequestMessage::StatementBind {
            index: self.0.index,
            param_list,
        })?;
        self.0.tx_req.send(RequestMessage::StatementExecute {
            index: self.0.index,
            tx,
        })?;
        self.0.tx_req.send(RequestMessage::StatementReset {
            index: self.0.index,
        })?;
        let (r, _) = rx.await.map_err(|_| Error::background_task_failed())??;
        Ok(r)
    }

    pub(crate) async fn insert(&self, param_list: ParamList) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.0.tx_req.send(RequestMessage::StatementBind {
            index: self.0.index,
            param_list,
        })?;
        self.0.tx_req.send(RequestMessage::StatementExecute {
            index: self.0.index,
            tx,
        })?;
        self.0.tx_req.send(RequestMessage::StatementReset {
            index: self.0.index,
        })?;
        let (_, r) = rx.await.map_err(|_| Error::background_task_failed())??;
        Ok(r)
    }

    pub(crate) fn query_with<R>(self, param_list: ParamList) -> Rows<R>
    where
        R: FromRow + Send + 'static,
    {
        Rows {
            statement: self,
            state: RowsState::Param(param_list),
        }
    }
}

impl fmt::Debug for Statement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Statement")
            .field("index", &self.0.index)
            .finish_non_exhaustive()
    }
}

impl Drop for StatementInner {
    fn drop(&mut self) {
        let index = self.index;
        let _ = self.tx_req.send(RequestMessage::Finish { index });
    }
}

pub(crate) struct Rows<R> {
    statement: Statement,
    state: RowsState<R>,
}

enum RowsState<R> {
    Param(ParamList),
    Bound,
    Step(oneshot::Receiver<Option<Result<R>>>),
    Finish,
}

impl<R> Stream for Rows<R>
where
    R: FromRow,
    R: Send + 'static,
{
    type Item = Result<R>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Result<R>>> {
        loop {
            match &mut self.state {
                RowsState::Param(_) => match mem::replace(&mut self.state, RowsState::Finish) {
                    RowsState::Param(param_list) => {
                        match self.statement.0.tx_req.send(RequestMessage::StatementBind {
                            index: self.statement.0.index,
                            param_list,
                        }) {
                            Ok(_) => {
                                self.state = RowsState::Bound;
                            }
                            Err(e) => return Poll::Ready(Some(Err(e))),
                        }
                    }
                    _ => panic!(),
                },

                RowsState::Bound => {
                    let (tx, rx) = oneshot::channel();
                    if let Err(e) = self.statement.0.tx_req.send(RequestMessage::StatementNext {
                        index: self.statement.0.index,
                        callback: Box::new(|r| match r {
                            Ok((mut row_reader, crate::internal::Step::Row)) => {
                                let _ = tx.send(Some(R::from_row(&mut row_reader)));
                            }
                            Ok((_, crate::internal::Step::Done { .. })) => {
                                let _ = tx.send(None);
                            }
                            Err(e) => {
                                let _ = tx.send(Some(Err(e)));
                            }
                        }),
                    }) {
                        self.state = RowsState::Finish;
                        return Poll::Ready(Some(Err(e)));
                    }
                    self.state = RowsState::Step(rx);
                }

                RowsState::Step(rx) => match ready!(Pin::new(rx).poll(cx)) {
                    Ok(Some(Ok(v))) => {
                        self.state = RowsState::Bound;

                        return Poll::Ready(Some(Ok(v)));
                    }

                    Ok(None) => {
                        self.state = RowsState::Finish;
                    }

                    Ok(Some(Err(e))) => {
                        self.state = RowsState::Finish;

                        return Poll::Ready(Some(Err(e)));
                    }

                    Err(_) => {
                        self.state = RowsState::Finish;

                        return Poll::Ready(Some(Err(Error::background_task_failed())));
                    }
                },

                RowsState::Finish => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl<R> Drop for Rows<R> {
    fn drop(&mut self) {
        let _ = self
            .statement
            .0
            .tx_req
            .send(RequestMessage::StatementReset {
                index: self.statement.0.index,
            });
    }
}

pub(crate) struct PrepareWith {
    tx_req: Sender,
    state: PrepareState,
    sql: Arc<str>,
}

enum PrepareState {
    MessageNotSent,
    MessageSent(oneshot::Receiver<Result<Option<(StatementIndex, usize)>>>),
    Finished,
}

impl Future for PrepareWith {
    type Output = Result<Statement>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        loop {
            match &mut self.state {
                PrepareState::MessageNotSent => {
                    let (tx, rx) = oneshot::channel();
                    self.tx_req
                        .send(RequestMessage::Prepare {
                            sql: self.sql.clone(),
                            index_from_start: 0,
                            tx,
                        })
                        .map_err(|_| Error::background_task_failed())?;
                    self.state = PrepareState::MessageSent(rx);
                }

                PrepareState::MessageSent(rx) => match ready!(Pin::new(rx).poll(cx)) {
                    Ok(index_and_rest) => {
                        let index = index_and_rest?
                            .ok_or_else(|| Error::new(ErrorKind::Generic, "no statement".into()))?
                            .0;
                        self.state = PrepareState::Finished;
                        return Poll::Ready(Ok(Statement(Arc::new(StatementInner {
                            tx_req: self.tx_req.clone(),
                            index,
                        }))));
                    }
                    Err(_) => {
                        self.state = PrepareState::Finished;
                        return Poll::Ready(Err(Error::background_task_failed()));
                    }
                },

                PrepareState::Finished => panic!("`Ready` polled after completion"),
            }
        }
    }
}

pub(crate) struct Batch {
    sql: Arc<str>,
    index_of_start: usize,
    tx_req: Sender,
    state: BatchState,
}

enum BatchState {
    MessageNotSent,
    MessageSent(oneshot::Receiver<Result<Option<(StatementIndex, usize)>>>),
    Finished,
}

impl Stream for Batch {
    type Item = Result<Statement>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                BatchState::MessageNotSent => {
                    let (tx, rx) = oneshot::channel();
                    self.tx_req
                        .send(RequestMessage::Prepare {
                            sql: self.sql.clone(),
                            index_from_start: self.index_of_start,
                            tx,
                        })
                        .map_err(|_| Error::background_task_failed())?;
                    self.state = BatchState::MessageSent(rx);
                }

                BatchState::MessageSent(rx) => match ready!(Pin::new(rx).poll(cx)) {
                    Ok(index_and_rest) => {
                        if let Some((index, rest)) = index_and_rest? {
                            self.index_of_start = rest;
                            self.state = if self.index_of_start >= self.sql.len() {
                                BatchState::Finished
                            } else {
                                BatchState::MessageNotSent
                            };
                            return Poll::Ready(Some(Ok(Statement(Arc::new(StatementInner {
                                tx_req: self.tx_req.clone(),
                                index,
                            })))));
                        } else {
                            self.state = BatchState::Finished;
                        }
                    }
                    Err(_) => {
                        self.state = BatchState::Finished;
                        return Poll::Ready(Some(Err(Error::background_task_failed())));
                    }
                },

                BatchState::Finished => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}
