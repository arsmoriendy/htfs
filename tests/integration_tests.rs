#[macro_use]
mod prelude;

#[cfg(test)]
mod integration_tests {
    use crate::prelude::*;

    #[test]
    fn main() {
        // rename
        // TODO: test premissions
        rename_p_p2p(); // prefixed directory, from prefixed parent to another prefixed parent
        rename_p_p2u();
        rename_p_u2p();
        rename_p_u2u();
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
            .block_on(
                query_scalar("SELECT tid FROM associated_tags WHERE ino = 4").fetch_all(&pool),
            )
            .unwrap();
        assert!(new_tids.len() == 2);
        assert!(new_tids.contains(&2));
        assert!(new_tids.contains(&4));

        // assert new name
        rt.block_on(
            query("SELECT 1 FROM file_names WHERE ino = 4 AND name = '#new-child'")
                .fetch_one(&pool),
        )
        .unwrap();

        // assert no dangling tags
        assert!(
            rt.block_on(query("SELECT 1 FROM tags WHERE name = '#child'").fetch_optional(&pool))
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
            .block_on(
                query_scalar("SELECT tid FROM associated_tags WHERE ino = 4").fetch_all(&pool),
            )
            .unwrap();
        assert!(new_tids.len() == 1);
        assert!(new_tids.contains(&3));

        // assert child exists on new parent's dir_contents
        rt.block_on(
            query("SELECT 1 FROM dir_contents WHERE dir_ino = 3 AND cnt_ino = 4").fetch_all(&pool),
        )
        .unwrap();

        // assert new name
        rt.block_on(
            query("SELECT 1 FROM file_names WHERE ino = 4 AND name = '#new-child'")
                .fetch_one(&pool),
        )
        .unwrap();

        // assert no dangling tags
        assert!(
            rt.block_on(query("SELECT 1 FROM tags WHERE name = '#child'").fetch_optional(&pool))
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
            .block_on(
                query_scalar("SELECT tid FROM associated_tags WHERE ino = 4").fetch_all(&pool),
            )
            .unwrap();
        assert!(new_tids.len() == 2);
        assert!(new_tids.contains(&1));
        assert!(new_tids.contains(&3));

        // assert child no longer exists in old parent's dir_contents
        assert!(
            rt.block_on(
                query("SELECT 1 FROM dir_contents WHERE dir_ino = 2 AND cnt_ino = 4")
                    .fetch_optional(&pool)
            )
            .unwrap()
            .is_none()
        );

        // assert new name
        rt.block_on(
            query("SELECT 1 FROM file_names WHERE ino = 4 AND name = '#new-child'")
                .fetch_one(&pool),
        )
        .unwrap();

        // assert no dangling tags
        assert!(
            rt.block_on(query("SELECT 1 FROM tags WHERE name = '#child'").fetch_optional(&pool))
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
            .block_on(
                query_scalar("SELECT tid FROM associated_tags WHERE ino = 4").fetch_all(&pool),
            )
            .unwrap();
        assert!(new_tids.len() == 1);
        assert!(new_tids.contains(&2));

        // assert child exists on new parent's dir_contents
        rt.block_on(
            query("SELECT 1 FROM dir_contents WHERE dir_ino = 3 AND cnt_ino = 4").fetch_all(&pool),
        )
        .unwrap();

        // assert child no longer exists in old parent's dir_contents
        assert!(
            rt.block_on(
                query("SELECT 1 FROM dir_contents WHERE dir_ino = 2 AND cnt_ino = 4")
                    .fetch_optional(&pool)
            )
            .unwrap()
            .is_none()
        );

        // assert new name
        rt.block_on(
            query("SELECT 1 FROM file_names WHERE ino = 4 AND name = '#new-child'")
                .fetch_one(&pool),
        )
        .unwrap();

        // assert no dangling tags
        assert!(
            rt.block_on(query("SELECT 1 FROM tags WHERE name = '#child'").fetch_optional(&pool))
                .unwrap()
                .is_none()
        );

        Test::cleanup(bg_sess);
    }
}
