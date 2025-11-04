use std::{fs::File, str::FromStr};

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

    if new {
        File::create_new(&database).unwrap();
    }

    let rt = Runtime::new().unwrap();
    let fs = rt.block_on(async {
        let pool = SqlitePool::connect_with(
            SqliteConnectOptions::from_str(format!("sqlite:{}", database).as_str())
                .unwrap()
                .locking_mode(sqlx::sqlite::SqliteLockingMode::Normal)
                .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal),
        )
        .await
        .unwrap();

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
