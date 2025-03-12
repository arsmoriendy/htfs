#[cfg(test)]
mod test {
    use std::{
        fs::{remove_file, File},
        io::{self, Read},
        str::FromStr,
    };

    use sqlx::{migrate, query, sqlite::SqliteConnectOptions, SqlitePool};

    use crate::*;

    struct Setup<'a> {
        mount_path: &'a str,
        db_path: &'a str,
        pool: &'a Pool<Sqlite>,
        bg_sess: Option<BackgroundSession>,
    }

    impl Default for Setup<'static> {
        fn default() -> Self {
            let mount_path = "mountpoint";
            if let Err(e) = std::fs::create_dir(mount_path) {
                panic!("{e}");
            }

            let db_path = "tfs_test.sqlite";
            File::create(db_path).unwrap();

            let pool = task::block_on(async {
                let pool: &'static Pool<Sqlite> = Box::leak(Box::new(
                    SqlitePool::connect_with(
                        SqliteConnectOptions::from_str(format!("sqlite:{}", db_path).as_str())
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

            let bg_sess = spawn_mount2(TagFileSystem { pool }, mount_path, &[]).unwrap();

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

    impl Drop for Setup<'_> {
        fn drop(&mut self) {
            self.bg_sess.take().unwrap().join();

            if let Err(e) = std::fs::remove_dir(self.mount_path) {
                panic!("{e}");
            }

            remove_file(self.db_path).unwrap();
        }
    }

    #[ignore]
    #[test]
    fn mount_interactive() {
        task::block_on(async {
            Setup::default();

            println!("press enter key to dismount...");
            let mut buf: [u8; 1] = [0];
            io::stdin().read_exact(&mut buf).unwrap();
        });
    }
}
