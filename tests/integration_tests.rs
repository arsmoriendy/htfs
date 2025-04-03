#[cfg(test)]
mod integration_tests {
    use std::{
        fs::{create_dir, metadata, remove_file, File},
        io::{self, Read, Write},
        os::unix::fs::{FileExt, MetadataExt},
        path::{Path, PathBuf},
        str::FromStr,
    };

    use async_std::task;
    use fuser::{spawn_mount2, BackgroundSession};
    use sqlx::{migrate, query, query_as, sqlite::SqliteConnectOptions, Pool, Sqlite, SqlitePool};
    use tfs::TagFileSystem;

    struct Setup {
        mount_path: PathBuf,
        db_path: PathBuf,
        pool: &'static Pool<Sqlite>,
        bg_sess: Option<BackgroundSession>,
    }

    impl Default for Setup {
        fn default() -> Self {
            let mount_path = PathBuf::from("mountpoint");
            loop {
                if let Err(e) = std::fs::create_dir(&mount_path) {
                    if e.kind() == io::ErrorKind::AlreadyExists {
                        continue;
                    } else {
                        panic!("{e}");
                    }
                };
                break;
            }

            let db_path = PathBuf::from("tfs_test.sqlite");
            File::create(&db_path).unwrap();

            let pool = task::block_on(async {
                let pool: &'static Pool<Sqlite> = Box::leak(Box::new(
                    SqlitePool::connect_with(
                        SqliteConnectOptions::from_str(
                            format!("sqlite:{}", db_path.to_str().unwrap()).as_str(),
                        )
                        .unwrap()
                        .locking_mode(sqlx::sqlite::SqliteLockingMode::Normal)
                        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal),
                    )
                    .await
                    .unwrap(),
                ));

                migrate!().run(pool).await.unwrap();

                pool
            });

            let bg_sess = spawn_mount2(TagFileSystem { pool }, &mount_path, &[]).unwrap();

            // wait for initialization
            task::block_on(async {
                loop {
                    if let Some(_) = query("SELECT 1 FROM file_attrs WHERE ino = 1")
                        .fetch_optional(pool)
                        .await
                        .unwrap()
                    {
                        break;
                    };
                }
            });

            Setup {
                mount_path,
                db_path,
                pool,
                bg_sess: Some(bg_sess),
            }
        }
    }

    impl Drop for Setup {
        fn drop(&mut self) {
            self.bg_sess.take().unwrap().join();
            std::fs::remove_dir(&self.mount_path).unwrap();
            remove_file(&self.db_path).unwrap();
        }
    }

    fn crt_dummy_dir(parent: &Path, name: Option<&Path>) -> PathBuf {
        let dir_path: PathBuf = [parent, name.unwrap_or(Path::new("foo"))].iter().collect();
        create_dir(&dir_path).unwrap();
        dir_path
    }

    fn crt_dummy_file(parent: &Path, name: Option<&Path>) -> (PathBuf, File) {
        let path: PathBuf = [parent, name.unwrap_or(Path::new("bar"))].iter().collect();
        let file = File::create_new(&path).unwrap();

        (path, file)
    }

    struct Dummies {
        dir_path: PathBuf,
        file_path: PathBuf,
        file: File,
    }
    fn crt_dummies(parent: &PathBuf) -> Dummies {
        let dir_path = crt_dummy_dir(parent, None);
        let file = crt_dummy_file(&dir_path, None);

        Dummies {
            dir_path,
            file_path: file.0,
            file: file.1,
        }
    }

    /// Create dummy dir `foo` at `parent`, create file `bar` in it that is filled with `content`
    /// or `lorem ipsum` by default
    fn fill_dummy(parent: &PathBuf, content: Option<&[u8]>) -> Dummies {
        let mut dum = crt_dummies(parent);
        dum.file.write(content.unwrap_or(b"lorem ipsum")).unwrap();
        dum
    }

    #[ignore]
    #[test]
    fn mount_interactive() {
        let _stp = Setup::default();

        println!("press enter key to dismount...");
        let mut buf: [u8; 1] = [0];
        io::stdin().read_exact(&mut buf).unwrap();
    }

    #[test]
    fn mkdir() {
        task::block_on(async {
            let stp = Setup::default();

            let dir_name = "foo";
            let dir_path = crt_dummy_dir(&stp.mount_path, Some(Path::new(dir_name)));

            let dir_meta = metadata(&dir_path).unwrap();
            let tid = query_as::<_, (i64,)>("SELECT tid FROM associated_tags WHERE ino = ?")
                .bind(dir_meta.ino() as i64)
                .fetch_one(stp.pool)
                .await
                .unwrap()
                .0;

            assert!(dir_meta.is_dir());
            assert_eq!(dir_meta.uid(), unsafe { libc::geteuid() });
            assert_eq!(dir_meta.gid(), unsafe { libc::getegid() });
            // assert dir name
            assert!(
                query_as::<_, (String,)>("SELECT name FROM file_names WHERE ino = ?")
                    .bind(dir_meta.ino() as i64)
                    .fetch_one(stp.pool)
                    .await
                    .unwrap()
                    .0
                    .eq(dir_name)
            );
            // assert tag name
            assert!(
                query_as::<_, (String,)>("SELECT  name FROM tags WHERE tid = ? AND name = ?",)
                    .bind(tid)
                    .bind(dir_name)
                    .fetch_one(stp.pool)
                    .await
                    .unwrap()
                    .0
                    .eq(dir_name)
            );
        })
    }

    #[test]
    fn write() {
        task::block_on(async {
            let stp = Setup::default();

            let filepath: PathBuf = [stp.mount_path.to_str().unwrap(), "foo"].iter().collect();
            let mut file = File::create_new(filepath).unwrap();

            let content = b"lorem ipsum";
            file.write_all(content).unwrap();

            let db_content =
                query_as::<_, (Box<[u8]>,)>("SELECT content FROM file_contents WHERE ino = $1")
                    .bind(file.metadata().unwrap().ino() as i64)
                    .fetch_one(stp.pool)
                    .await
                    .unwrap()
                    .0;

            assert_eq!(content, db_content.as_ref());
        })
    }
}
