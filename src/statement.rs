use crate::{
    blocking::StatementIndex,
    convert::{FromRow, ParamList, RowReader},
    worker::{Sender, Work},
    Error, ErrorKind, Result,
};
use futures_lite::Stream;
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
        Batch {
            sql,
            index_of_start: 0,
            tx_req,
            state: BatchState::MessageNotSent,
        }
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
        let statement = self.index;
        self.tx_req
            .call_without_return_value(move |c| c.finalize(statement));
    }
}

pub(crate) struct Rows<R> {
    statement: Statement,
    state: RowsState<R>,
}

enum RowsState<R> {
    Param(ParamList),
    Bound,
    Step(Work<Option<R>>),
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
                RowsState::Param(param_list) => {
                    let param_list = param_list.take();
                    let statement_index = self.statement.0.index;

                    self.state = RowsState::Bound;
                    self.statement.0.tx_req.call_without_return_value(move |c| {
                        c.bind(statement_index, param_list);
                    });
                }

                RowsState::Bound => {
                    let statement_index = self.statement.0.index;
                    self.state = RowsState::Step(self.statement.0.tx_req.call(move |c| {
                        let mut row = None;
                        c.step(statement_index, &mut |iterator| {
                            if let Some(iterator) = iterator {
                                row = Some(R::from_row(RowReader::new(iterator)));
                            } else {
                                row = None;
                            }
                        });
                        row.transpose()
                    }));
                }

                RowsState::Step(rx) => match ready!(Pin::new(rx).poll(cx)) {
                    Ok(Some(v)) => {
                        self.state = RowsState::Bound;

                        return Poll::Ready(Some(Ok(v)));
                    }

                    Ok(None) => {
                        self.state = RowsState::Finish;
                    }

                    Err(e) => {
                        self.state = RowsState::Finish;

                        return Poll::Ready(Some(Err(e)));
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
        let statement_index = self.statement.0.index;
        self.statement.0.tx_req.call_without_return_value(move |c| {
            c.reset(statement_index);
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

pub(crate) struct Batch {
    sql: Arc<str>,
    index_of_start: usize,
    tx_req: Sender,
    state: BatchState,
}

enum BatchState {
    MessageNotSent,
    MessageSent(Work<Option<(StatementIndex, usize)>>),
    Finished,
}

impl Stream for Batch {
    type Item = Result<Statement>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                BatchState::MessageNotSent => {
                    let sql = self.sql.clone();
                    let index = self.index_of_start;

                    self.state = BatchState::MessageSent(
                        self.tx_req.call(move |c| c.prepare(&sql[index..])),
                    );
                }

                BatchState::MessageSent(rx) => match ready!(Pin::new(rx).poll(cx)) {
                    Ok(index_and_rest) => {
                        if let Some((index, rest)) = index_and_rest {
                            self.index_of_start += rest;
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
                    Err(e) => {
                        self.state = BatchState::Finished;
                        return Poll::Ready(Some(Err(e)));
                    }
                },

                BatchState::Finished => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}
