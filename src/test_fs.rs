#[cfg(test)]
mod test {
    use std::{
        fs::{remove_file, File},
        io::{self, Read},
        path::PathBuf,
        str::FromStr,
    };

    use sqlx::{migrate, query, sqlite::SqliteConnectOptions, SqlitePool};

    use crate::*;

    struct Setup {
        mount_path: PathBuf,
        db_path: PathBuf,
        pool: &'static Pool<Sqlite>,
        bg_sess: Option<BackgroundSession>,
    }

    impl Default for Setup {
        fn default() -> Self {
            let mount_path = PathBuf::from("mountpoint");
            if let Err(e) = std::fs::create_dir(&mount_path) {
                panic!("{e}");
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

            if let Err(e) = std::fs::remove_dir(&self.mount_path) {
                panic!("{e}");
            }

            remove_file(&self.db_path).unwrap();
        }
    }

    #[ignore]
    #[test]
    fn mount_interactive() {
        let _stp = Setup::default();

        println!("press enter key to dismount...");
        let mut buf: [u8; 1] = [0];
        io::stdin().read_exact(&mut buf).unwrap();
    }
}
