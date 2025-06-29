use crate::utils::hash_deque::HashVecDeque;

#[test]
fn push_pop() {
    let mut hash_deque: HashVecDeque<u64> = HashVecDeque::new();
    for _ in 0..2 {
        hash_deque.push_front(10);
        hash_deque.push_front(20);
        hash_deque.push_front(30);
        hash_deque.push_front(40);
        hash_deque.push_front(50);
        hash_deque.push_front(60);
        hash_deque.push_front(70);
        hash_deque.push_front(80);
        hash_deque.push_front(90);
        hash_deque.push_front(100);
        assert_eq!(hash_deque.pop_back(), Some(10));
        assert_eq!(hash_deque.pop_back(), Some(20));
        assert_eq!(hash_deque.pop_back(), Some(30));
        assert_eq!(hash_deque.pop_back(), Some(40));
        assert_eq!(hash_deque.pop_back(), Some(50));
        assert_eq!(hash_deque.pop_back(), Some(60));
        assert_eq!(hash_deque.pop_back(), Some(70));
        assert_eq!(hash_deque.pop_back(), Some(80));
        assert_eq!(hash_deque.pop_back(), Some(90));
        assert_eq!(hash_deque.pop_back(), Some(100));
        assert_eq!(hash_deque.pop_back(), None);
    }
}

#[test]
fn clear() {
    let mut hash_deque: HashVecDeque<u64> = HashVecDeque::new();
    hash_deque.push_front(10);
    hash_deque.push_front(20);
    hash_deque.push_front(30);
    hash_deque.push_front(40);
    hash_deque.push_front(50);
    hash_deque.push_front(60);
    hash_deque.push_front(70);
    hash_deque.push_front(80);
    hash_deque.push_front(90);
    hash_deque.push_front(100);
    assert_eq!(hash_deque.len(), 10);
    hash_deque.clear();
    assert_eq!(hash_deque.pop_back(), None);
}

#[test]
fn remove() {
    let mut hash_deque: HashVecDeque<u64> = HashVecDeque::new();
    hash_deque.push_front(10);
    hash_deque.push_front(20);
    hash_deque.push_front(30);
    hash_deque.push_front(40);
    hash_deque.push_front(50);
    hash_deque.push_front(60);
    hash_deque.push_front(70);
    hash_deque.push_front(80);
    hash_deque.push_front(90);
    hash_deque.push_front(100);
    assert_eq!(hash_deque.remove(&20), Some(20));
    assert_eq!(hash_deque.pop_back(), Some(10));
    assert_eq!(hash_deque.pop_back(), Some(30));
    assert_eq!(hash_deque.pop_back(), Some(40));
    assert_eq!(hash_deque.pop_back(), Some(50));
    assert_eq!(hash_deque.pop_back(), Some(60));
    assert_eq!(hash_deque.pop_back(), Some(70));
    assert_eq!(hash_deque.pop_back(), Some(80));
    assert_eq!(hash_deque.pop_back(), Some(90));
    assert_eq!(hash_deque.pop_back(), Some(100));
}
