use std::io::Write;

load_prelude!();

pub fn test_write() {
    // = CASES
    //
    // Legend:
    // []   : page delimiter
    // |    : offset / end of data
    // -    : data
    // o    : empty data
    //
    // == Aligned
    // 1.   [|-|]
    // 2.   [|--][--|]
    // 3.   [---][|--][--|]
    // 4.   [ooo][|--][--|]
    //
    // == Unaligned
    // 5.   [-||]
    // 6.   [-|-][--|]
    // 7.   [---][-|-][--|]
    // 8.   [ooo][o|-][--|]
    // 9.   [||o]
    // 10.  [|--][-|o]
    // 11.  [---][|--][-|o]
    // 12.  [ooo][|--][-|o]

    aligned(); // 1
    aligned_span(); // 2
    unaligned_end(); // 9
    unaligned_end_span(); // 10
}

const PAGE_SIZE: usize = 4096;

fn aligned() {
    let Test { bg_sess, rt, pool } = Test::new();

    let size = PAGE_SIZE;
    let bytes: Vec<u8> = rand::random_iter().take(size).collect();
    let mut file = create_file(path!(MP_PATH, "file")).unwrap();
    file.write_all(&bytes).unwrap();

    let db_bytes: Vec<u8> = rt
        .block_on(read_file_query!().bind(2).fetch_one(&pool))
        .unwrap();
    assert!(bytes == db_bytes);

    Test::cleanup(bg_sess);
}

fn aligned_span() {
    let Test { bg_sess, rt, pool } = Test::new();

    let size = PAGE_SIZE * 2;
    let bytes: Vec<u8> = rand::random_iter().take(size).collect();
    let mut file = create_file(path!(MP_PATH, "file")).unwrap();
    file.write_all(&bytes).unwrap();

    let db_bytes: Vec<u8> = rt
        .block_on(read_file_query!().bind(2).fetch_one(&pool))
        .unwrap();
    assert!(bytes == db_bytes);

    Test::cleanup(bg_sess);
}

fn unaligned_end() {
    let Test { bg_sess, rt, pool } = Test::new();

    let size = PAGE_SIZE - 512;
    let bytes: Vec<u8> = rand::random_iter().take(size).collect();
    let mut file = create_file(path!(MP_PATH, "file")).unwrap();
    file.write_all(&bytes).unwrap();

    let db_bytes: Vec<u8> = rt
        .block_on(read_file_query!().bind(2).fetch_one(&pool))
        .unwrap();
    assert!(bytes == db_bytes);

    Test::cleanup(bg_sess);
}

fn unaligned_end_span() {
    let Test { bg_sess, rt, pool } = Test::new();

    let size = PAGE_SIZE + 512;
    let bytes: Vec<u8> = rand::random_iter().take(size).collect();
    let mut file = create_file(path!(MP_PATH, "file")).unwrap();
    file.write_all(&bytes).unwrap();

    let db_bytes: Vec<u8> = rt
        .block_on(read_file_query!().bind(2).fetch_one(&pool))
        .unwrap();
    assert_eq!(bytes, db_bytes);

    Test::cleanup(bg_sess);
}
