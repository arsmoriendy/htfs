#[cfg(test)]
mod test {
    use async_std::task;
    use sqlx::{migrate, query, QueryBuilder, Sqlite, SqlitePool};

    use crate::db_helpers::chain_tagged_inos;

    #[test]
    fn migrate() {
        let pool = Box::new(SqlitePool::connect_lazy("sqlite::memory:").unwrap());

        task::block_on(async {
            migrate!().run(pool.as_ref()).await.unwrap();

            query("SELECT ino, size, blocks, atime, mtime, ctime, crtime, kind, perm, nlink, uid, gid, rdev, blksize, flags FROM file_attrs")
                .execute(pool.as_ref())
                .await
                .unwrap();

            query("SELECT ino, name FROM file_names")
                .execute(pool.as_ref())
                .await
                .unwrap();

            query("SELECT ino, size, blocks, atime, mtime, ctime, crtime, kind, perm, nlink, uid, gid, rdev, blksize, flags, name FROM readdir_rows")
                .execute(pool.as_ref())
                .await
                .unwrap();

            query("SELECT tid, name FROM tags")
                .execute(pool.as_ref())
                .await
                .unwrap();

            query("SELECT tid, ino FROM associated_tags")
                .execute(pool.as_ref())
                .await
                .unwrap();

            query("SELECT dir_ino, cnt_ino FROM dir_contents")
                .execute(pool.as_ref())
                .await
                .unwrap();

            query("SELECT ino, content FROM file_contents")
                .execute(pool.as_ref())
                .await
                .unwrap();

            pool.close().await;
        });
    }

    #[test]
    fn chain_tagged_inos_test() {
        let tags = vec![1u64, 2, 3];

        let mut qb = QueryBuilder::<Sqlite>::new("");

        chain_tagged_inos(&mut qb, &tags)
            .map_err(|_| "failed binding tags")
            .unwrap();

        assert_eq!(
            qb.sql(),
            "SELECT ino FROM associated_tags WHERE tid = ? AND ino IN (SELECT ino FROM associated_tags WHERE tid = ? AND ino IN (SELECT ino FROM associated_tags WHERE tid = ?))"
        )
    }
}
