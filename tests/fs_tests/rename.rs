load_prelude!();

pub fn test_rename() {
    // TODO: test:
    // - premissions
    // - atime, ctime, mtime
    rename_p2u();
    rename_u_p2p();
    rename_u_p2u();
    rename_u_u2p();
    rename_u_u2u();
    rename_p_p2p(); // prefixed directory, from prefixed parent to another prefixed parent
    rename_p_p2u();
    rename_p_u2p();
    rename_p_u2u();
}

fn rename_p2u() {
    let t = Test::new();
    let Test { bg_sess, .. } = t;

    let dir_path = path!(MP_PATH, "#dir");
    create_dir(&dir_path).unwrap();

    let new_dir_path = path!(MP_PATH, "dir");
    let mv_op = rename(&dir_path, &new_dir_path);

    assert!(mv_op.is_err_and(|e| e.kind() == IoErrorKind::InvalidInput));

    Test::cleanup(bg_sess);
}

fn rename_u_p2p() {
    let Test { rt, pool, bg_sess } = Test::new();

    let parent_path = path!(MP_PATH, "#parent");
    create_dir(&parent_path).unwrap();

    let new_parent_path = path!(MP_PATH, "#new-parent");
    create_dir(&new_parent_path).unwrap();

    let child_path = path!(&parent_path; "child");
    create_dir(&child_path).unwrap();

    let new_child_path = path!(&new_parent_path; "new-child");
    rename(&child_path, &new_child_path).unwrap();

    // assert new tag association
    let new_tids: Vec<u64> = rt
        .block_on(query_scalar("SELECT tid FROM associated_tags WHERE ino = 4").fetch_all(pool))
        .unwrap();
    assert!(new_tids.len() == 1);
    assert!(new_tids.contains(&2));

    // assert new name
    rt.block_on(
        query("SELECT 1 FROM file_names WHERE ino = 4 AND name = 'new-child'").fetch_one(pool),
    )
    .unwrap();

    Test::cleanup(bg_sess);
}

fn rename_u_p2u() {
    let Test { rt, pool, bg_sess } = Test::new();

    let parent_path = path!(MP_PATH, "#parent");
    create_dir(&parent_path).unwrap();

    let new_parent_path = path!(MP_PATH, "new-parent");
    create_dir(&new_parent_path).unwrap();

    let child_path = path!(&parent_path; "child");
    create_dir(&child_path).unwrap();

    let new_child_path = path!(&new_parent_path; "new-child");
    rename(&child_path, &new_child_path).unwrap();

    // assert new tag association
    let new_tids: Vec<u64> = rt
        .block_on(query_scalar("SELECT tid FROM associated_tags WHERE ino = 4").fetch_all(pool))
        .unwrap();
    assert!(new_tids.len() == 0);

    // assert new name
    rt.block_on(
        query("SELECT 1 FROM file_names WHERE ino = 4 AND name = 'new-child'").fetch_one(pool),
    )
    .unwrap();

    Test::cleanup(bg_sess);
}

fn rename_u_u2p() {
    let Test { rt, pool, bg_sess } = Test::new();

    let parent_path = path!(MP_PATH, "parent");
    create_dir(&parent_path).unwrap();

    let new_parent_path = path!(MP_PATH, "#new-parent");
    create_dir(&new_parent_path).unwrap();

    let child_path = path!(&parent_path; "child");
    create_dir(&child_path).unwrap();

    let new_child_path = path!(&new_parent_path; "new-child");
    rename(&child_path, &new_child_path).unwrap();

    // assert new tag association
    let new_tids: Vec<u64> = rt
        .block_on(query_scalar("SELECT tid FROM associated_tags WHERE ino = 4").fetch_all(pool))
        .unwrap();
    assert!(new_tids.len() == 1);
    assert!(new_tids.contains(&1));

    // assert new name
    rt.block_on(
        query("SELECT 1 FROM file_names WHERE ino = 4 AND name = 'new-child'").fetch_one(pool),
    )
    .unwrap();

    Test::cleanup(bg_sess);
}

fn rename_u_u2u() {
    let Test { rt, pool, bg_sess } = Test::new();

    let parent_path = path!(MP_PATH, "parent");
    create_dir(&parent_path).unwrap();

    let new_parent_path = path!(MP_PATH, "new-parent");
    create_dir(&new_parent_path).unwrap();

    let child_path = path!(&parent_path; "child");
    create_dir(&child_path).unwrap();

    let new_child_path = path!(&new_parent_path; "new-child");
    rename(&child_path, &new_child_path).unwrap();

    // assert new tag association
    let new_tids: Vec<u64> = rt
        .block_on(query_scalar("SELECT tid FROM associated_tags WHERE ino = 4").fetch_all(pool))
        .unwrap();
    assert!(new_tids.len() == 0);

    // assert new name
    rt.block_on(
        query("SELECT 1 FROM file_names WHERE ino = 4 AND name = 'new-child'").fetch_one(pool),
    )
    .unwrap();

    Test::cleanup(bg_sess);
}

fn rename_p_p2p() {
    let Test { rt, pool, bg_sess } = Test::new();

    let parent_path = path!(MP_PATH, "#parent");
    create_dir(&parent_path).unwrap();

    let new_parent_path = path!(MP_PATH, "#new-parent");
    create_dir(&new_parent_path).unwrap();

    let child_path = path!(&parent_path; "#child");
    create_dir(&child_path).unwrap();

    let new_child_path = path!(&new_parent_path; "#new-child");
    rename(&child_path, &new_child_path).unwrap();

    // assert new tag association
    let new_tids: Vec<u64> = rt
        .block_on(query_scalar("SELECT tid FROM associated_tags WHERE ino = 4").fetch_all(pool))
        .unwrap();
    assert!(new_tids.len() == 2);
    assert!(new_tids.contains(&2));
    assert!(new_tids.contains(&4));

    // assert new name
    rt.block_on(
        query("SELECT 1 FROM file_names WHERE ino = 4 AND name = '#new-child'").fetch_one(pool),
    )
    .unwrap();

    // assert no dangling tags
    assert!(
        rt.block_on(query("SELECT 1 FROM tags WHERE name = '#child'").fetch_optional(pool))
            .unwrap()
            .is_none()
    );

    Test::cleanup(bg_sess);
}

fn rename_p_p2u() {
    let Test { rt, pool, bg_sess } = Test::new();

    let parent_path = path!(MP_PATH, "#parent");
    create_dir(&parent_path).unwrap();

    let new_parent_path = path!(MP_PATH, "new-parent");
    create_dir(&new_parent_path).unwrap();

    let child_path = path!(&parent_path; "#child");
    create_dir(&child_path).unwrap();

    let new_child_path = path!(&new_parent_path; "#new-child");
    rename(&child_path, &new_child_path).unwrap();

    // assert new tag association
    let new_tids: Vec<u64> = rt
        .block_on(query_scalar("SELECT tid FROM associated_tags WHERE ino = 4").fetch_all(pool))
        .unwrap();
    assert!(new_tids.len() == 1);
    assert!(new_tids.contains(&3));

    // assert child exists on new parent's dir_contents
    rt.block_on(
        query("SELECT 1 FROM dir_contents WHERE dir_ino = 3 AND cnt_ino = 4").fetch_all(pool),
    )
    .unwrap();

    // assert new name
    rt.block_on(
        query("SELECT 1 FROM file_names WHERE ino = 4 AND name = '#new-child'").fetch_one(pool),
    )
    .unwrap();

    // assert no dangling tags
    assert!(
        rt.block_on(query("SELECT 1 FROM tags WHERE name = '#child'").fetch_optional(pool))
            .unwrap()
            .is_none()
    );

    Test::cleanup(bg_sess);
}

fn rename_p_u2p() {
    let Test { rt, pool, bg_sess } = Test::new();

    let parent_path = path!(MP_PATH, "parent");
    create_dir(&parent_path).unwrap();

    let new_parent_path = path!(MP_PATH, "#new-parent");
    create_dir(&new_parent_path).unwrap();

    let child_path = path!(&parent_path; "#child");
    create_dir(&child_path).unwrap();

    let new_child_path = path!(&new_parent_path; "#new-child");
    rename(&child_path, &new_child_path).unwrap();

    // assert new tag association
    let new_tids: Vec<u64> = rt
        .block_on(query_scalar("SELECT tid FROM associated_tags WHERE ino = 4").fetch_all(pool))
        .unwrap();
    assert!(new_tids.len() == 2);
    assert!(new_tids.contains(&1));
    assert!(new_tids.contains(&3));

    // assert child no longer exists in old parent's dir_contents
    assert!(
        rt.block_on(
            query("SELECT 1 FROM dir_contents WHERE dir_ino = 2 AND cnt_ino = 4")
                .fetch_optional(pool)
        )
        .unwrap()
        .is_none()
    );

    // assert new name
    rt.block_on(
        query("SELECT 1 FROM file_names WHERE ino = 4 AND name = '#new-child'").fetch_one(pool),
    )
    .unwrap();

    // assert no dangling tags
    assert!(
        rt.block_on(query("SELECT 1 FROM tags WHERE name = '#child'").fetch_optional(pool))
            .unwrap()
            .is_none()
    );

    Test::cleanup(bg_sess);
}

fn rename_p_u2u() {
    let Test { rt, pool, bg_sess } = Test::new();

    let parent_path = path!(MP_PATH, "parent");
    create_dir(&parent_path).unwrap();

    let new_parent_path = path!(MP_PATH, "new-parent");
    create_dir(&new_parent_path).unwrap();

    let child_path = path!(&parent_path; "#child");
    create_dir(&child_path).unwrap();

    let new_child_path = path!(&new_parent_path; "#new-child");
    rename(&child_path, &new_child_path).unwrap();

    // assert new tag association
    let new_tids: Vec<u64> = rt
        .block_on(query_scalar("SELECT tid FROM associated_tags WHERE ino = 4").fetch_all(pool))
        .unwrap();
    assert!(new_tids.len() == 1);
    assert!(new_tids.contains(&2));

    // assert child exists on new parent's dir_contents
    rt.block_on(
        query("SELECT 1 FROM dir_contents WHERE dir_ino = 3 AND cnt_ino = 4").fetch_all(pool),
    )
    .unwrap();

    // assert child no longer exists in old parent's dir_contents
    assert!(
        rt.block_on(
            query("SELECT 1 FROM dir_contents WHERE dir_ino = 2 AND cnt_ino = 4")
                .fetch_optional(pool)
        )
        .unwrap()
        .is_none()
    );

    // assert new name
    rt.block_on(
        query("SELECT 1 FROM file_names WHERE ino = 4 AND name = '#new-child'").fetch_one(pool),
    )
    .unwrap();

    // assert no dangling tags
    assert!(
        rt.block_on(query("SELECT 1 FROM tags WHERE name = '#child'").fetch_optional(pool))
            .unwrap()
            .is_none()
    );

    Test::cleanup(bg_sess);
}
