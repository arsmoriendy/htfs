#[cfg(test)]
mod test {
    use sqlx::{migrate, query, SqlitePool};

    use crate::*;

    struct Setup<'a> {
        monut_path: &'a str,
    }

    impl Setup<'_> {
        fn init(&self) {
            if let Err(e) = std::fs::create_dir(self.monut_path) {
                panic!("{e}");
            }
        }
    }

    impl Default for Setup<'_> {
        fn default() -> Self {
            let ret = Setup {
                monut_path: "mountpoint",
            };
            ret.init();
            return ret;
        }
    }

    impl Drop for Setup<'_> {
        fn drop(&mut self) {
            if let Err(e) = std::fs::remove_dir_all(self.monut_path) {
                panic!("{e}");
            }
        }
    }

    #[test]
    fn mount_unmount() {
        let stp = Setup::default();

        let pool = Box::new(SqlitePool::connect_lazy("sqlite::memory:").unwrap());

        let sess = spawn_mount2(TagFileSystem { pool }, stp.monut_path, &[]).unwrap();
        sess.join();
    }

    #[ignore]
    #[test]
    fn mount_interactive() {
        let stp = Setup::default();

        let pool = Box::new(SqlitePool::connect_lazy("sqlite::memory:").unwrap());
        task::block_on(migrate!().run(pool.as_ref())).unwrap();

        mount2(TagFileSystem { pool }, stp.monut_path, &[]).unwrap();
    }

    #[ignore]
    #[test]
    fn readdir_interactive() {
        let stp = Setup::default();

        let pool = Box::new(SqlitePool::connect_lazy("sqlite::memory:").unwrap());
        task::block_on(migrate!().run(pool.as_ref())).unwrap();

        for i in 2..1000 {
            task::block_on(async {
                query("INSERT INTO file_attrs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
                    .bind(i) // ino INTEGER PRIMARY KEY,
                    .bind(0) // size INTEGER,
                    .bind(0) // blocks INTEGER,
                    .bind(0) // atime INTEGER,
                    .bind(0) // mtime INTEGER,
                    .bind(0) // ctime INTEGER,
                    .bind(0) // crtime INTEGER,
                    .bind(4) // kind INTEGER,
                    .bind(0o777) // perm INTEGER,
                    .bind(1) // nlink INTEGER,
                    .bind(1000) // uid INTEGER,
                    .bind(1000) // gid INTEGER,
                    .bind(0) // rdev INTEGER,
                    .bind(0) // blksize INTEGER,
                    .bind(0) // flags INTEGER,
                    .execute(pool.as_ref())
                    .await
                    .unwrap();

                query("INSERT INTO file_names VALUES(?,?)")
                    .bind(i)
                    .bind(format!("filname-{}", i))
                    .execute(pool.as_ref())
                    .await
                    .unwrap()
            });
        }

        mount2(TagFileSystem { pool: pool.clone() }, stp.monut_path, &[]).unwrap();
    }
}
