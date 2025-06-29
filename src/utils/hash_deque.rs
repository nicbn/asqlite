use std::{
    borrow::Borrow,
    collections::VecDeque,
    hash::{BuildHasher, Hash, RandomState},
};

#[cfg(test)]
mod tests;

/// Linear search hash deque.
pub(crate) struct HashVecDeque<T, S = RandomState> {
    items: VecDeque<T>,

    /// Stores the hashes sequentially. Speeds up linear search.
    ///
    /// The hashes are 32 bit to fit more elements in a cache line.
    hashes: VecDeque<u32>,

    hasher: S,
}

impl<T, S> HashVecDeque<T, S> {
    #[cfg(test)]
    pub(crate) fn new() -> Self
    where
        S: Default,
    {
        Self {
            items: VecDeque::new(),
            hashes: VecDeque::new(),
            hasher: Default::default(),
        }
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self
    where
        S: Default,
    {
        Self {
            items: VecDeque::with_capacity(capacity),
            hashes: VecDeque::with_capacity(capacity),
            hasher: Default::default(),
        }
    }

    pub(crate) fn clear(&mut self) {
        self.items.clear();
        self.hashes.clear();
    }

    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn push_front(&mut self, item: T)
    where
        S: BuildHasher,
        T: Hash,
    {
        self.hashes.push_front(self.hasher.hash_one(&item) as u32);
        self.items.push_front(item);
    }

    pub(crate) fn pop_back(&mut self) -> Option<T> {
        let _ = self.hashes.pop_back();
        self.items.pop_back()
    }

    pub(crate) fn remove<Q>(&mut self, item: &Q) -> Option<T>
    where
        S: BuildHasher,
        T: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hasher.hash_one(item) as u32;

        for (i, (h, x)) in self.hashes.iter().zip(self.items.iter_mut()).enumerate() {
            if hash != *h {
                continue;
            }
            if item != (x as &T).borrow() {
                continue;
            }
            self.hashes.remove(i);
            return self.items.remove(i);
        }

        None
    }
}
