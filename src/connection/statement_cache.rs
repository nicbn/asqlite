// Design:
//
// The cache is searched linearly, in an attempt to improve the performance by
// better using the CPU cache.
//
// To fit more elements in a cache line, we generate a 32 bit hash from the
// SQL query.

use crate::Statement;
use rustc_hash::FxHasher;
use std::{
    collections::VecDeque,
    hash::{Hash, Hasher},
    sync::Arc,
};

/// Prepared statement cache.
pub(crate) struct StatementCache {
    /// Statement cache.
    ///
    /// New statements are pushed to the front.
    cache: VecDeque<(Statement, Arc<str>)>,

    /// Stores the hashes for the statement cache sequentially. Speeds up
    /// linear search.
    ///
    /// The hashes are 32 bit to fit more elements in a cache line.
    hash: VecDeque<u32>,

    /// Maximum size of the statement cache.
    capacity: usize,
}

impl StatementCache {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            cache: VecDeque::with_capacity(capacity),
            hash: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.cache.clear();
        self.hash.clear();
    }

    pub(crate) fn insert(&mut self, sql: Arc<str>, v: Statement) {
        if self.capacity == 0 {
            return;
        }

        while self.cache.len() > self.capacity {
            self.cache.pop_back();
            self.hash.pop_back();
        }

        self.hash.push_front(sql_hash(&sql));

        self.cache.push_front((v, sql));
    }

    pub(crate) fn get(&mut self, sql: &str) -> Option<Statement> {
        let sql_hash = sql_hash(sql);

        for (i, (hash, cached)) in self.hash.iter().zip(self.cache.iter_mut()).enumerate() {
            if *hash != sql_hash {
                continue;
            }

            if *sql != *cached.1 {
                continue;
            }

            let statement = cached.clone();
            self.hash.remove(i);
            self.cache.remove(i);
            self.cache.push_front(statement.clone());

            return Some(statement.0);
        }

        None
    }
}

#[inline]
fn sql_hash(sql: &str) -> u32 {
    let mut hasher = FxHasher::default();
    sql.hash(&mut hasher);
    hasher.finish() as u32
}
