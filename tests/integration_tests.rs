#[cfg(test)]
mod integration_tests {
    use fuser::{BackgroundSession, spawn_mount2};
    use sqlx::{
        Pool, Sqlite, SqlitePool, migrate, query, query_as, query_scalar,
        sqlite::SqliteConnectOptions,
    };
    use std::{
        ffi::OsString,
        fs::{self, File, create_dir, remove_file},
        io::{self, Read, Write},
        os::unix::fs::{FileExt, MetadataExt},
        path::{Path, PathBuf},
        str::FromStr,
        thread::sleep,
        time::{Duration, SystemTime},
    };
    use tfs::TagFileSystem;
    use tokio::test;

    macro_rules! sleep {
        ($ms: expr) => {
            sleep(Duration::from_millis($ms));
        };
        () => {
            sleep!(10);
        };
    }

    const BASE_DIR: &str = env!("CARGO_MANIFEST_DIR");
    struct Setup {
        mount_path: PathBuf,
        db_path: PathBuf,
        pool: Pool<Sqlite>,
        bg_sess: Option<BackgroundSession>,
    }

    impl Setup {
        async fn new(mount_path: PathBuf, db_path: PathBuf) -> Self {
            tracing_subscriber::fmt::try_init().ok();

            // wait for delete from drop
            while let Err(e) = create_dir(&mount_path) {
                if e.kind() == io::ErrorKind::AlreadyExists {
                    sleep!();
                } else {
                    panic!("{e}");
                }
            }
            while let Err(e) = File::create_new(&db_path) {
                if e.kind() == io::ErrorKind::AlreadyExists {
                    sleep!();
                } else {
                    panic!("{e}");
                }
            }

            let pool = SqlitePool::connect_with(
                SqliteConnectOptions::from_str(
                    format!("sqlite:{}", db_path.to_str().unwrap()).as_str(),
                )
                .unwrap()
                .locking_mode(sqlx::sqlite::SqliteLockingMode::Normal)
                .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal),
            )
            .await
            .unwrap();

            migrate!().run(&pool).await.unwrap();

            let bg_sess = spawn_mount2(
                TagFileSystem {
                    pool: pool.clone(),
                    rt: tokio::runtime::Builder::new_current_thread()
                        .enable_time()
                        .build()
                        .unwrap(),
                    tag_prefix: String::from("#"),
                },
                &mount_path,
                &[],
            )
            .unwrap();

            // wait for initialization
            while query("SELECT 1 FROM file_attrs WHERE ino = 1")
                .fetch_one(&pool)
                .await
                .is_err()
            {
                sleep!();
            }

            Setup {
                mount_path,
                db_path,
                pool,
                bg_sess: Some(bg_sess), // TODO: unwrap Option
            }
        }

        async fn new_with(mount_path: Option<PathBuf>, db_path: Option<PathBuf>) -> Self {
            Self::new(
                mount_path.unwrap_or([BASE_DIR, "mountpoint"].iter().collect()),
                db_path.unwrap_or([BASE_DIR, "tfs_test.sqlite"].iter().collect()),
            )
            .await
        }

        async fn default() -> Self {
            Setup::new_with(None, None).await
        }
    }

    impl Drop for Setup {
        fn drop(&mut self) {
            self.bg_sess.take().unwrap().join();
            std::fs::remove_dir_all(&self.mount_path).unwrap();
            remove_file(&self.db_path).unwrap();
        }
    }

    fn crt_dummy_dir(parent: &Path, name: Option<&Path>) -> (PathBuf, File) {
        let dir_path: PathBuf = [parent, name.unwrap_or(Path::new("foo"))].iter().collect();
        fs::create_dir(&dir_path).unwrap();
        let dir_file = File::open(&dir_path).unwrap();

        (dir_path, dir_file)
    }

    fn crt_dummy_file(parent: &Path, name: Option<&Path>) -> (PathBuf, File) {
        let path: PathBuf = [parent, name.unwrap_or(Path::new("bar"))].iter().collect();
        let file = File::create_new(&path).unwrap();

        (path, file)
    }

    struct Dummies {
        dir_path: PathBuf,
        dir: File,
        file_path: PathBuf,
        file: File,
    }
    fn crt_dummies(parent: &PathBuf) -> Dummies {
        let (dir_path, dir) = crt_dummy_dir(parent, None);
        let (file_path, file) = crt_dummy_file(&dir_path, None);

        Dummies {
            dir,
            dir_path,
            file_path,
            file,
        }
    }

    /// Create dummy dir `foo` at `parent`, create file `bar` in it that is filled with `content`
    /// or `lorem ipsum` by default
    fn fill_dummies(parent: &PathBuf, content: Option<&[u8]>) -> Dummies {
        let mut dum = crt_dummies(parent);
        dum.file.write(content.unwrap_or(b"lorem ipsum")).unwrap();
        dum
    }

    #[test]
    #[ignore]
    async fn mount_interactive() {
        let _stp = Setup::default().await;

        println!("press enter key to dismount...");
        let mut buf: [u8; 1] = [0];
        io::stdin().read_exact(&mut buf).unwrap();
    }

    // TODO: test nested tags
    #[test]
    async fn mkdir_tagged() {
        let stp = Setup::default().await;

        let dir_name = "#foo";
        let (_, dir_file) = crt_dummy_dir(&stp.mount_path, Some(Path::new(dir_name)));
        let dir_meta = dir_file.metadata().unwrap();

        let tid = query_scalar::<_, i64>("SELECT tid FROM associated_tags WHERE ino = ?")
            .bind(dir_meta.ino() as i64)
            .fetch_one(&stp.pool)
            .await
            .unwrap();

        assert!(dir_meta.is_dir());
        assert_eq!(dir_meta.uid(), unsafe { libc::geteuid() });
        assert_eq!(dir_meta.gid(), unsafe { libc::getegid() });
        // assert dir name
        assert!(
            query_scalar::<_, String>("SELECT name FROM file_names WHERE ino = ?")
                .bind(dir_meta.ino() as i64)
                .fetch_one(&stp.pool)
                .await
                .unwrap()
                .eq(dir_name)
        );
        // assert tag name
        assert!(
            query_scalar::<_, String>("SELECT  name FROM tags WHERE tid = ? AND name = ?",)
                .bind(tid)
                .bind(dir_name)
                .fetch_one(&stp.pool)
                .await
                .unwrap()
                .eq(dir_name)
        );
    }

    #[test]
    async fn mkdir_untagged() {
        let stp = Setup::default().await;

        let dir_name = "foo";
        let (_, dir_file) = crt_dummy_dir(&stp.mount_path, Some(Path::new(dir_name)));
        let dir_meta = dir_file.metadata().unwrap();

        let tid = query_scalar::<_, i64>("SELECT tid FROM associated_tags WHERE ino = ?")
            .bind(dir_meta.ino() as i64)
            .fetch_optional(&stp.pool)
            .await
            .unwrap();

        assert!(dir_meta.is_dir());
        assert_eq!(dir_meta.uid(), unsafe { libc::geteuid() });
        assert_eq!(dir_meta.gid(), unsafe { libc::getegid() });
        // assert dir name
        assert!(
            query_scalar::<_, String>("SELECT name FROM file_names WHERE ino = ?")
                .bind(dir_meta.ino() as i64)
                .fetch_one(&stp.pool)
                .await
                .unwrap()
                .eq(dir_name)
        );
        assert!(tid == None);
    }

    #[test]
    // TODO:
    // - write with offset new file
    // - file larger than sql limit
    async fn write() {
        let stp = Setup::default().await;

        let dum = fill_dummies(&stp.mount_path, None);
        let file = dum.file;
        let ino = file.metadata().unwrap().ino();

        let db_content = async || {
            query_scalar::<_, Vec<u8>>("SELECT content FROM file_contents WHERE ino = $1")
                .bind(ino as i64)
                .fetch_one(&stp.pool)
                .await
                .unwrap()
        };

        let mut meta = file.metadata().unwrap();
        let mut mtime = meta.mtime();

        macro_rules! snyc_meta {
            () => {
                meta = file.metadata().unwrap();
            };
        }

        macro_rules! assert_mtime {
            () => {
                let prev_mtime = mtime;
                mtime = meta.mtime();
                assert!(mtime > prev_mtime);
            };
        }

        sleep!(1000); // ensure mtime has advanced by >= 1 sec
        file.write_all_at(b"lorem ipsum", 0).unwrap();
        snyc_meta!(); // HACK: this ensures write somehow
        assert_mtime!();
        assert_eq!(b"lorem ipsum", db_content().await.as_slice());
        assert_eq!(meta.size(), 11);

        file.write_all_at(b"hello world", 6).unwrap();
        snyc_meta!();
        assert_eq!(b"lorem hello world", db_content().await.as_slice());
        assert_eq!(meta.size(), 17);

        let offset = 1_000_000;
        file.write_all_at(b"x", offset).unwrap();
        snyc_meta!();
        assert_eq!(
            {
                let mut v = Vec::from(b"lorem hello world");
                v.extend(vec![0u8; offset as usize - 17]);
                v.push(b'x');
                v
            },
            db_content().await
        );
        assert_eq!(meta.size(), offset + 1);
    }

    #[test]
    async fn truncate() {
        let stp = Setup::default().await;

        let full_cnt = b"lorem ipsum";
        let dum = fill_dummies(&stp.mount_path, Some(full_cnt));
        let file = dum.file;
        let ino: i64 = file.metadata().unwrap().ino().try_into().unwrap();

        let resize = 5;
        file.set_len(resize).unwrap();

        // define variables to assert equal
        let expected_cnt = &full_cnt[..5];

        let (db_cnt, db_cnt_len) = query_as::<_, (Vec<u8>, u64)>(
            "SELECT content, LENGTH(content) FROM file_contents WHERE ino = $1",
        )
        .bind(ino)
        .fetch_one(&stp.pool)
        .await
        .unwrap();

        let db_attr_size = query_scalar::<_, u64>("SELECT size FROM file_attrs WHERE ino = $1")
            .bind(ino)
            .fetch_one(&stp.pool)
            .await
            .unwrap();

        assert_eq!(db_cnt_len, db_attr_size);
        assert_eq!(db_cnt, expected_cnt);
    }

    #[test]
    async fn read() {
        let stp = Setup::default().await;

        let full_cnt = b"lorem ipsum";
        let dum = fill_dummies(&stp.mount_path, Some(full_cnt));
        let prev_atime = dum.file.metadata().unwrap().atime();

        sleep(Duration::from_millis(1000));
        let read_cnt = fs::read_to_string(dum.file_path).unwrap();
        let atime = dum.file.metadata().unwrap().atime();
        assert_eq!(read_cnt.as_bytes(), full_cnt);
        assert!(atime > prev_atime);
    }

    #[test]
    async fn setattr() {
        let stp = Setup::default().await;

        let dum = fill_dummies(&stp.mount_path, None);
        let file = dum.file;

        let mut meta = file.metadata().unwrap();
        let prev_ctime = meta.ctime();

        sleep(Duration::from_millis(1000));
        file.set_modified(SystemTime::now()).unwrap();
        meta = file.metadata().unwrap();
        assert!(meta.ctime() > prev_ctime);
    }

    #[test]
    async fn rename_file() {
        let stp = Setup::default().await;

        let (dir1_path, _) = crt_dummy_dir(&stp.mount_path, Some(Path::new("dir1")));
        let (dir2_path, _) = crt_dummy_dir(&stp.mount_path, Some(Path::new("dir2")));

        let (child_path, child) = crt_dummy_file(&dir1_path, Some(Path::new("child")));
        let child_ino = child.metadata().unwrap().ino();

        let mut new_child_path = dir2_path.clone();
        new_child_path.push("new_child");

        fs::rename(&child_path, &new_child_path).unwrap();

        let new_child_ino = File::open(&new_child_path)
            .unwrap()
            .metadata()
            .unwrap()
            .ino();
        assert_eq!(child_ino, new_child_ino);

        let expected_err = File::open(&child_path).unwrap_err();
        assert_eq!(expected_err.kind(), std::io::ErrorKind::NotFound)
    }

    #[test]
    async fn rename_tagged_dir() {
        let stp = Setup::default().await;

        let (dir1_path, _) = crt_dummy_dir(&stp.mount_path, Some(Path::new("#dir1")));
        let (dir2_path, _) = crt_dummy_dir(&stp.mount_path, Some(Path::new("#dir2")));
        let (inner_dir_path, inner_dir_file) =
            crt_dummy_dir(&dir1_path, Some(Path::new("#inner_dir")));
        let inner_dir_ino = inner_dir_file.metadata().unwrap().ino();

        let (child_path, child) = crt_dummy_file(&inner_dir_path, Some(Path::new("child")));
        let child_ino = child.metadata().unwrap().ino();

        let mut new_inner_dir_path = dir2_path.clone();
        new_inner_dir_path.push("new_inner_dir");

        fs::rename(&inner_dir_path, &new_inner_dir_path).unwrap();

        let new_inner_dir_ino = File::open(&new_inner_dir_path)
            .unwrap()
            .metadata()
            .unwrap()
            .ino();
        assert_eq!(inner_dir_ino, new_inner_dir_ino);

        let mut new_child_path = new_inner_dir_path.clone();
        new_child_path.push("child");
        let new_child_ino = File::open(&new_child_path)
            .unwrap()
            .metadata()
            .unwrap()
            .ino();
        assert_eq!(child_ino, new_child_ino);

        let expected_err = File::open(&child_path).unwrap_err();
        assert_eq!(expected_err.kind(), std::io::ErrorKind::NotFound);

        let expected_err = File::open(&inner_dir_path).unwrap_err();
        assert_eq!(expected_err.kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    async fn unlink_tagged() {
        let stp = Setup::default().await;

        let (dir_path, _) = crt_dummy_dir(&stp.mount_path, Some(Path::new("#dir")));
        let (file_path, file) = crt_dummy_file(&dir_path, Some(Path::new("file")));
        let file_ino = file.metadata().unwrap().ino();

        remove_file(file_path).unwrap();

        let q = query("SELECT * FROM file_attrs WHERE ino = ?")
            .bind(file_ino as i64)
            .fetch_optional(&stp.pool)
            .await
            .unwrap();

        assert!(q.is_none());
    }

    #[test]
    async fn unlink_untagged() {
        let stp = Setup::default().await;

        let (dir_path, _) = crt_dummy_dir(&stp.mount_path, Some(Path::new("dir")));
        let (file_path, file) = crt_dummy_file(&dir_path, Some(Path::new("file")));
        let file_ino = file.metadata().unwrap().ino();

        remove_file(file_path).unwrap();

        let q = query("SELECT * FROM file_attrs WHERE ino = ?")
            .bind(file_ino as i64)
            .fetch_optional(&stp.pool)
            .await
            .unwrap();

        assert!(q.is_none());
    }

    #[test]
    async fn mknod_tagged() {
        let stp = Setup::default().await;
        let (dir_path, dir) = crt_dummy_dir(&stp.mount_path, Some(Path::new("#dir")));
        let dir_ino = dir.metadata().unwrap().ino();
        let (_, file) = crt_dummy_file(&dir_path, Some(Path::new("file")));
        let file_ino = file.metadata().unwrap().ino();
        let db_file_ino = query_scalar::<_, i64>("SELECT ino FROM file_attrs WHERE ino = ?")
            .bind(file_ino as i64)
            .fetch_one(&stp.pool)
            .await
            .unwrap();
        let dir_tid = query_scalar::<_, i64>("SELECT tid FROM associated_tags WHERE ino = ?")
            .bind(dir_ino as i64)
            .fetch_one(&stp.pool)
            .await
            .unwrap();
        let file_tid = query_scalar::<_, i64>("SELECT tid FROM associated_tags WHERE ino = ?")
            .bind(file_ino as i64)
            .fetch_one(&stp.pool)
            .await
            .unwrap();

        assert!(db_file_ino == file_ino as i64);
        assert!(dir_tid == file_tid);
    }

    #[test]
    async fn mknod_untagged() {
        let stp = Setup::default().await;
        let (dir_path, _) = crt_dummy_dir(&stp.mount_path, Some(Path::new("dir")));
        let (_, file) = crt_dummy_file(&dir_path, Some(Path::new("file")));
        let file_ino = file.metadata().unwrap().ino();
        let db_file_ino = query_scalar::<_, i64>("SELECT ino FROM file_attrs WHERE ino = ?")
            .bind(file_ino as i64)
            .fetch_one(&stp.pool)
            .await
            .unwrap();
        let file_tid = query_scalar::<_, i64>("SELECT tid FROM associated_tags WHERE ino = ?")
            .bind(file_ino as i64)
            .fetch_optional(&stp.pool)
            .await
            .unwrap();

        assert!(db_file_ino == file_ino as i64);
        assert!(file_tid.is_none());
    }
}
