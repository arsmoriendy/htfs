#[cfg(test)]
mod test {
    use async_std::task;
    use sqlx::{migrate, query, SqlitePool};

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
}
