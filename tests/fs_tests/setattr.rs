load_prelude!();

// =CASES
//
// Legend: look at ./write.rs
// == Resize
// 1.   |---)-|
// 2.   |###)#|
// 3.   |####-|---)-|
// 4.   |###)#|####-|
// 5.   |---)-|####-|

pub fn test_setattr() {
    resize_empty_file(); // 1
    truncate_file(); // 2
    extend_file(); // 3
    truncate_file2(); // 4
    truncate_file3(); // 5
}

fn truncate_file3() {
    let Test { bg_sess, rt, pool } = Test::new();

    let size = PAGE_SIZE - 512 * 2;
    let offset = PAGE_SIZE + 512;
    let bytes: Vec<u8> = rand::random_iter().take(size).collect();
    let file = create_file(path!(MP_PATH, "file")).unwrap();
    file.write_all_at(&bytes, offset.try_into().unwrap())
        .unwrap();
    file.set_len(size.try_into().unwrap()).unwrap();

    let pages: u64 = rt
        .block_on(
            query_scalar("SELECT LENGTH(page) FROM file_contents WHERE ino = 2").fetch_one(&pool),
        )
        .unwrap();
    assert_eq!(pages, 1);

    let db_bytes: Vec<u8> = rt
        .block_on(read_file_query!().bind(2).fetch_one(&pool))
        .unwrap();
    assert_eq!(db_bytes, vec![0u8; size]);

    Test::cleanup(bg_sess);
}

fn truncate_file2() {
    let Test { bg_sess, rt, pool } = Test::new();

    let size = PAGE_SIZE + 512;
    let bytes: Vec<u8> = rand::random_iter().take(size).collect();
    let mut file = create_file(path!(MP_PATH, "file")).unwrap();
    file.write_all(&bytes).unwrap();

    let new_size = PAGE_SIZE - 512;
    file.set_len(new_size.try_into().unwrap()).unwrap();

    let db_bytes: Vec<u8> = rt
        .block_on(read_file_query!().bind(2).fetch_one(&pool))
        .unwrap();
    assert!(bytes[..new_size] == db_bytes);

    let pages: u64 = rt
        .block_on(
            query_scalar("SELECT LENGTH(page) FROM file_contents WHERE ino = 2").fetch_one(&pool),
        )
        .unwrap();
    assert_eq!(pages, 1);

    Test::cleanup(bg_sess);
}

fn extend_file() {
    let Test { bg_sess, rt, pool } = Test::new();

    let size = PAGE_SIZE - 512;
    let mut bytes: Vec<u8> = rand::random_iter().take(size).collect();
    let mut file = create_file(path!(MP_PATH, "file")).unwrap();
    file.write_all(&bytes).unwrap();

    let new_size = PAGE_SIZE * 2 - 512;
    file.set_len(new_size.try_into().unwrap()).unwrap();

    let db_bytes: Vec<u8> = rt
        .block_on(read_file_query!().bind(2).fetch_one(&pool))
        .unwrap();
    bytes.resize(new_size, 0);
    assert!(bytes == db_bytes);

    Test::cleanup(bg_sess);
}

fn truncate_file() {
    let Test { bg_sess, rt, pool } = Test::new();

    let size = PAGE_SIZE;
    let bytes: Vec<u8> = rand::random_iter().take(size).collect();
    let mut file = create_file(path!(MP_PATH, "file")).unwrap();
    file.write_all(&bytes).unwrap();

    let new_size = size - 512;
    file.set_len(new_size.try_into().unwrap()).unwrap();

    let db_bytes: Vec<u8> = rt
        .block_on(read_file_query!().bind(2).fetch_one(&pool))
        .unwrap();
    assert!(bytes[..new_size] == db_bytes);

    Test::cleanup(bg_sess);
}

fn resize_empty_file() {
    let Test { bg_sess, rt, pool } = Test::new();

    let size = PAGE_SIZE - 512;
    let bytes = vec![0u8; size];
    let file = create_file(path!(MP_PATH, "file")).unwrap();
    file.set_len(size.try_into().unwrap()).unwrap();

    let db_bytes: Vec<u8> = rt
        .block_on(read_file_query!().bind(2).fetch_one(&pool))
        .unwrap();
    assert!(bytes == db_bytes);

    Test::cleanup(bg_sess);
}
