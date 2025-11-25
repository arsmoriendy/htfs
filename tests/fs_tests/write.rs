load_prelude!();

pub fn test_write() {
    // = CASES
    //
    // Legend:
    // |    : page delimiter
    // [    : start of data
    // ]    : end of data
    // #    : data
    // -    : empty data
    // }    : resize
    //
    // == Aligned
    // 1.   |[---]|
    // 2.   |[----|----]|
    //
    // == Unaligned
    // 3.   |[-]--|
    // 4.   |[----|-]---|
    // 5.   |-----|-[-]-|

    aligned(); // 1
    aligned_span(); // 2
    unaligned_end(); // 3
    unaligned_end_span(); // 4
    unaligned_start_end_span(); // 5
}

fn unaligned_start_end_span() {
    let Test { bg_sess, rt, pool } = Test::new();

    let size = PAGE_SIZE - 512 * 2;
    let offset = PAGE_SIZE + 512;
    let bytes: Vec<u8> = rand::random_iter().take(size).collect();
    let file = create_file(path!(MP_PATH, "file")).unwrap();
    file.write_all_at(&bytes, offset.try_into().unwrap())
        .unwrap();

    let pages: u64 = rt
        .block_on(
            query_scalar("SELECT LENGTH(page) FROM file_contents WHERE ino = 2").fetch_one(&pool),
        )
        .unwrap();
    assert_eq!(pages, 1);

    let db_bytes: Vec<u8> = rt
        .block_on(
            query_scalar("SELECT bytes FROM file_contents WHERE ino = 2 AND page = 1")
                .fetch_one(&pool),
        )
        .unwrap();
    let mut new_bytes = vec![0u8; 512];
    new_bytes.extend_from_slice(&bytes);
    assert_eq!(new_bytes, db_bytes);

    Test::cleanup(bg_sess);
}

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
