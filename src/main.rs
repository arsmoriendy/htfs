use std::{
    fs::{File, create_dir},
    io::ErrorKind as IoErrorKind,
    str::FromStr,
};

use clap::Parser;
use htfs::HTFS;
use sqlx::{SqlitePool, sqlite::SqliteConnectOptions};
use tokio::runtime::{Handle, Runtime};

fn main() {
    let Args {
        database,
        mountpoint,
        new,
        prefix,
    } = Args::parse();

    tracing_subscriber::fmt::try_init().ok();

    if new {
        let db_err = File::create_new(&database).err();
        let mp_err = create_dir(&mountpoint).err();

        for e in [db_err, mp_err] {
            if let Some(e) = e
                && e.kind() != IoErrorKind::AlreadyExists
            {
                panic!("{e}")
            }
        }
    }

    let rt = Runtime::new().unwrap();
    let fs = rt.block_on(async {
        let pool = Box::leak(Box::new(
            SqlitePool::connect_with(
                SqliteConnectOptions::from_str(format!("sqlite:{}", database).as_str())
                    .unwrap()
                    // disable caching
                    // .pragma("cache_size", "0")
                    // .statement_cache_capacity(0)
                    .locking_mode(sqlx::sqlite::SqliteLockingMode::Normal)
                    .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal),
            )
            .await
            .unwrap(),
        ));

        HTFS {
            pool: pool,
            runtime_handle: Handle::current(),
            tag_prefix: prefix,
        }
    });

    fuser::mount2(fs, mountpoint, &[]).unwrap();
}

#[derive(Parser)]
struct Args {
    database: String,
    mountpoint: String,
    #[arg(default_value_t = false, short, long)]
    new: bool,
    #[arg(default_value = "#", short, long)]
    prefix: String,
}
