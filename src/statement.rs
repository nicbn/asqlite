use crate::{
    blocking::{self, StatementIndex},
    convert::{FromRow, ParamList, RowReader},
    worker::{AsyncRequest, Sender, Work},
    Error, ErrorKind, Result,
};
use futures_channel::mpsc;
use futures_core::Stream;
use std::{
    fmt,
    future::Future,
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
        let (tx, rx) = mpsc::channel::<Result<StatementIndex>>(1);
        let request = BatchRequest {
            sql,
            index_of_start: 0,
            tx,
        };
        tx_req.spawn(request);
        Batch(tx_req, rx)
    }

    pub(crate) async fn execute(&self, param_list: ParamList) -> Result<u32> {
        let statement = self.0.index;
        self.0
            .tx_req
            .call(move |c| {
                c.bind(statement, param_list);
                let r = c.execute(statement).map(|_| c.total_changes());
                c.reset(statement);
                r
            })
            .await
    }

    pub(crate) async fn insert(&self, param_list: ParamList) -> Result<i64> {
        let statement = self.0.index;
        self.0
            .tx_req
            .call(move |c| {
                c.bind(statement, param_list);
                let r = c.execute(statement).map(|_| c.last_insert_row_id());
                c.reset(statement);
                r
            })
            .await
    }

    pub(crate) async fn query_first<R>(self, param_list: ParamList) -> Result<Option<R>>
    where
        R: FromRow + Send + 'static,
    {
        let statement = self.0.index;
        self.0
            .tx_req
            .call(move |c| {
                c.bind(statement, param_list);
                let row = match c.step(statement) {
                    Ok(Some(row)) => {
                        let mut reader = RowReader::new(row);
                        let result = R::from_row(&mut reader);
                        result.map(Some)
                    }
                    Ok(None) => Ok(None),
                    Err(e) => Err(e),
                };
                c.reset(statement);
                row
            })
            .await
    }

    pub(crate) fn query_with<R>(self, param_list: ParamList) -> Rows<R>
    where
        R: FromRow + Send + 'static,
    {
        let (tx, rx) = mpsc::channel::<Result<R>>(1);
        let request = RowsRequest {
            statement: self.0.index,
            param: Some(param_list),
            tx,
        };
        self.0.tx_req.spawn(request);
        rx
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
        let statement = self.index;
        self.tx_req
            .call_without_return_value(move |c| c.finalize(statement));
    }
}

pub(crate) type Rows<R> = mpsc::Receiver<Result<R>>;

pub(crate) struct PrepareWith {
    tx_req: Sender,
    state: PrepareState,
    sql: Arc<str>,
}

enum PrepareState {
    MessageNotSent,
    MessageSent(Work<Option<(StatementIndex, usize)>>),
    Finished,
}

impl Future for PrepareWith {
    type Output = Result<Statement>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        loop {
            match &mut self.state {
                PrepareState::MessageNotSent => {
                    let sql = self.sql.clone();
                    self.state =
                        PrepareState::MessageSent(self.tx_req.call(move |c| c.prepare(&sql)));
                }

                PrepareState::MessageSent(rx) => match ready!(Pin::new(rx).poll(cx)) {
                    Ok(index_and_rest) => {
                        let index = index_and_rest
                            .ok_or_else(|| Error::new(ErrorKind::Generic, "no statement".into()))?
                            .0;
                        self.state = PrepareState::Finished;
                        return Poll::Ready(Ok(Statement(Arc::new(StatementInner {
                            tx_req: self.tx_req.clone(),
                            index,
                        }))));
                    }
                    Err(e) => {
                        self.state = PrepareState::Finished;
                        return Poll::Ready(Err(e));
                    }
                },

                PrepareState::Finished => panic!("`Ready` polled after completion"),
            }
        }
    }
}

pub(crate) struct Batch(Sender, mpsc::Receiver<Result<StatementIndex>>);

impl Stream for Batch {
    type Item = Result<Statement>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match ready!(Pin::new(&mut self.1).poll_next(cx)) {
            Some(Ok(index)) => Poll::Ready(Some(Ok(Statement(Arc::new(StatementInner {
                tx_req: self.0.clone(),
                index,
            }))))),
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        }
    }
}

struct BatchRequest {
    sql: Arc<str>,
    index_of_start: usize,
    tx: mpsc::Sender<Result<StatementIndex>>,
}

impl AsyncRequest for BatchRequest {
    fn poll(
        &mut self,
        cx: &mut Context,
        connection: Option<&mut dyn blocking::Connection>,
    ) -> Poll<()> {
        let Some(connection) = connection else {
            let _ = ready!(self.tx.poll_ready(cx)); // ignore error
            let _ = self.tx.try_send(Err(ErrorKind::ConnectionClosed.into())); // ignore error
            return Poll::Ready(());
        };

        loop {
            match connection.prepare(&self.sql[self.index_of_start..]) {
                Ok(Some((statement_index, rest))) => {
                    if ready!(self.tx.poll_ready(cx)).is_err()
                        || self.tx.try_send(Ok(statement_index)).is_err()
                    {
                        break;
                    }

                    self.index_of_start += rest;
                }
                Ok(None) => break,
                Err(e) => {
                    let _ = ready!(self.tx.poll_ready(cx)); // ignore error
                    let _ = self.tx.try_send(Err(e)); // ignore error
                    break;
                }
            }
        }

        Poll::Ready(())
    }
}

struct RowsRequest<R> {
    statement: StatementIndex,
    param: Option<ParamList>,
    tx: mpsc::Sender<Result<R>>,
}

impl<R> AsyncRequest for RowsRequest<R>
where
    R: FromRow,
    R: Send + 'static,
{
    fn poll(
        &mut self,
        cx: &mut Context,
        connection: Option<&mut dyn blocking::Connection>,
    ) -> Poll<()> {
        let Some(connection) = connection else {
            let _ = ready!(self.tx.poll_ready(cx)); // ignore error
            let _ = self.tx.try_send(Err(ErrorKind::ConnectionClosed.into())); // ignore error
            return Poll::Ready(());
        };

        if let Some(param_list) = self.param.take() {
            connection.bind(self.statement, param_list);
        }

        loop {
            match connection.step(self.statement) {
                Ok(Some(row)) => {
                    let mut reader = RowReader::new(row);
                    let result = R::from_row(&mut reader);
                    if ready!(self.tx.poll_ready(cx)).is_err() || self.tx.try_send(result).is_err()
                    {
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    let _ = ready!(self.tx.poll_ready(cx)); // ignore error
                    let _ = self.tx.try_send(Err(e)); // ignore error
                    break;
                }
            }
        }

        connection.reset(self.statement);
        Poll::Ready(())
    }
}
