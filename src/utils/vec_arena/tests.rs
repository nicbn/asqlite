use super::VecArena;

#[test]
fn test() {
    let mut vec_arena = VecArena::<u8>::new();

    vec_arena.push(0);
    vec_arena.push(1);
    vec_arena.push(2);
    vec_arena.push(3);
    vec_arena.push(4);
    vec_arena.push(5);
    vec_arena.push(6);
    vec_arena.push(7);
    vec_arena.push(8);
    vec_arena.push(9);

    vec_arena.remove(1);
    vec_arena.remove(3);
    
    vec_arena.push(11);
    vec_arena.push(12);
    vec_arena.push(14);
    vec_arena.push(15);

    assert_eq!(vec_arena.get(0), Some(&0));
    assert_eq!(vec_arena.get(1), Some(&12));
    assert_eq!(vec_arena.get(2), Some(&2));
    assert_eq!(vec_arena.get(3), Some(&11));
    assert_eq!(vec_arena.get(4), Some(&4));
    assert_eq!(vec_arena.get(5), Some(&5));
    assert_eq!(vec_arena.get(6), Some(&6));
    assert_eq!(vec_arena.get(7), Some(&7));
    assert_eq!(vec_arena.get(8), Some(&8));
    assert_eq!(vec_arena.get(9), Some(&9));
    assert_eq!(vec_arena.get(10), Some(&14));
    assert_eq!(vec_arena.get(11), Some(&15));
}
