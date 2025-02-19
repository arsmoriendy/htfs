#[cfg(test)]
mod test {
    use sqlx::{migrate, query, SqlitePool};

    use crate::*;

    #[test]
    fn migrate() {
        let pool = Box::new(SqlitePool::connect_lazy("sqlite::memory:").unwrap());

        task::block_on(async {
            migrate!().run(pool.as_ref()).await.unwrap();

            query("SELECT ino, size, blocks, atime, mtime, ctime, crtime, kind, perm, nlink, uid, gid, rdev, blksize, flags FROM file_attrs")
                .execute(pool.as_ref())
                .await
                .unwrap();

            pool.close().await;
        });
    }
}
