#[macro_use]
mod macros;

pub use std::{
    fs::{File, create_dir, remove_dir, remove_file, rename},
    io::{ErrorKind as IoErrorKind, Result as IoResult, Write},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    str::FromStr,
};

pub use fuser::{BackgroundSession, spawn_mount2};
pub use htfs::HTFS;
pub use rand::prelude::*;
pub use sqlx::{SqlitePool, query, query_scalar, sqlite::SqliteConnectOptions};
pub use tokio::runtime::Runtime;

/// Alias to `File::create_new`
pub fn create_file<P: AsRef<Path>>(path: P) -> IoResult<File> {
    File::create_new(path)
}

pub struct Test {
    pub rt: Runtime,
    pub pool: &'static SqlitePool,
    pub bg_sess: BackgroundSession,
}

impl Test {
    pub fn new() -> Test {
        tracing_subscriber::fmt::try_init().ok();
        init_paths!();
        let rt = Runtime::new().unwrap();
        let pool = rt.block_on(async {
            Box::leak(Box::new(
                SqlitePool::connect_with(
                    SqliteConnectOptions::from_str(format!("sqlite:{}", DB_PATH).as_str())
                        .unwrap()
                        .locking_mode(sqlx::sqlite::SqliteLockingMode::Normal)
                        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal),
                )
                .await
                .unwrap(),
            ))
        });
        let bg_sess = init_sess!(rt, pool);
        Test { rt, pool, bg_sess }
    }

    pub fn cleanup(bg_sess: BackgroundSession) {
        // INFO: dropping instead of joining leaves behind sqlite shm and wal files, however
        // joining hangs when creating files (e.g., `let file = create_file("file").unwrap()`)
        drop(bg_sess);
        cleanup_paths!();
    }
}

pub const DB_PATH: &str = "test-db.sqlite";
pub const MP_PATH: &str = "test-mountpoint";
pub const PAGE_SIZE: usize = 4096;
