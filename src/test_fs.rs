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

        let tfs = TagFileSystem { pool };

        for _ in 1..=1000 {
            task::block_on(async {
                let now = SystemTime::now();
                let inode = ins_attrs!(
                    query_as::<_, (i64,)>,
                    FileAttr {
                        ino: 0,
                        size: 0,
                        blocks: 0,
                        atime: now,
                        mtime: now,
                        ctime: now,
                        crtime: now,
                        kind: FileType::RegularFile,
                        perm: 0o777,
                        nlink: 1,
                        uid: 1000,
                        gid: 1000,
                        rdev: 0,
                        blksize: 0,
                        flags: 0,
                    },
                    "RETURNING ino"
                )
                .fetch_one(tfs.pool.as_ref())
                .await
                .unwrap()
                .0;

                query("INSERT INTO file_names VALUES(?,?)")
                    .bind(inode)
                    .bind(format!("filname-{}", inode))
                    .execute(tfs.pool.as_ref())
                    .await
                    .unwrap()
            });
        }

        mount2(tfs, stp.monut_path, &[]).unwrap();
    }
}
