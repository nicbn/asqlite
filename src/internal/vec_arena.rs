pub(super) struct VecArena<T> {
    free: usize,
    data: Vec<ValueOrFreeList<T>>,
}

enum ValueOrFreeList<T> {
    Value(T),
    FreeList(usize),
}

impl<T> VecArena<T> {
    pub(crate) fn new() -> Self {
        Self {
            free: usize::MAX,
            data: Vec::new(),
        }
    }

    pub(crate) fn push(&mut self, x: T) -> usize {
        let index = self.free;

        if index != usize::MAX {
            if let ValueOrFreeList::FreeList(free) = self.data[index] {
                self.free = free;
                self.data[index] = ValueOrFreeList::Value(x);
                return index;
            } else {
                panic!();
            }
        }

        let index = self.data.len();

        self.data.push(ValueOrFreeList::Value(x));
        index
    }

    pub(crate) fn remove(&mut self, index: usize) {
        if let Some(data) = self.data.get_mut(index) {
            *data = ValueOrFreeList::FreeList(self.free);

            self.free = index;
        }
    }

    pub(crate) fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        if let ValueOrFreeList::Value(x) = self.data.get_mut(index)? {
            Some(x)
        } else {
            None
        }
    }
}
