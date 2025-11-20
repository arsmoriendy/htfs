use std::io::{Read, Write};

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
    // [|-|]
    // [|--][--|]
    // [---][|--][--|]
    // [ooo][|--][--|]
    //
    // == Unaligned
    // [-||]
    // [-|-][--|]
    // [---][-|-][--|]
    // [ooo][-|-][--|]
    // [||-]
    // [|--][-|-]
    // [---][|--][-|-]
    // [ooo][|--][-|-]

    aligned();
    aligned_span();
}

const PAGE_SIZE: usize = 4096;

fn aligned() {
    let Test { bg_sess, rt, pool } = Test::new();

    let size = PAGE_SIZE;
    let bytes: Vec<u8> = rand::random_iter().take(size).collect();
    let mut file = create_file(path!(MP_PATH, "file")).unwrap();
    file.write_all(&bytes).unwrap();

    let db_bytes: Vec<u8> = rt
        .block_on(
            query_scalar("SELECT bytes FROM file_contents WHERE ino = 2 AND page = 0")
                .fetch_one(&pool),
        )
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
        .block_on(
            query_scalar(
                "SELECT (SELECT bytes FROM file_contents WHERE ino = 2 AND page = 0) || (SELECT \
                 bytes FROM file_contents WHERE ino = 2 AND page = 1)",
            )
            .fetch_one(&pool),
        )
        .unwrap();
    assert!(bytes == db_bytes);

    Test::cleanup(bg_sess);
}
