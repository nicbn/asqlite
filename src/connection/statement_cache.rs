// Design:
//
// The cache is searched linearly, in an attempt to improve the performance by
// better using the CPU cache.
//
// To fit more elements in a cache line, we generate a 32 bit hash from the
// SQL query.

use crate::{utils::hash_deque::HashVecDeque, Statement};
use rustc_hash::FxBuildHasher;
use std::{
    borrow::Borrow,
    hash::{Hash, Hasher},
    sync::Arc,
};

/// Prepared statement cache.
pub(crate) struct StatementCache {
    /// Statement cache.
    ///
    /// New statements are pushed to the front.
    cache: HashVecDeque<StatementCached, FxBuildHasher>,

    /// Maximum size of the statement cache.
    capacity: usize,
}

impl StatementCache {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            cache: HashVecDeque::with_capacity(capacity),
            capacity,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.cache.clear();
    }

    pub(crate) fn insert(&mut self, sql: Arc<str>, v: Statement) {
        if self.capacity == 0 {
            return;
        }

        while self.cache.len() > self.capacity {
            self.cache.pop_back();
        }

        self.cache.push_front(StatementCached { sql, statement: v });
    }

    pub(crate) fn get(&mut self, sql: &str) -> Option<Statement> {
        let v = self.cache.remove(sql)?;
        let statement = v.statement.clone();
        self.cache.push_front(v);
        Some(statement)
    }
}

struct StatementCached {
    sql: Arc<str>,
    statement: Statement,
}

impl Borrow<str> for StatementCached {
    fn borrow(&self) -> &str {
        &self.sql
    }
}

impl Hash for StatementCached {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.sql.hash(state)
    }
}
