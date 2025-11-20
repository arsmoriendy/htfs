macro_rules! path {
        ($($p:expr),+) => {{
            let path: PathBuf = [$($p),+].iter().collect();
            path
        }};
        ($b:expr; $($p:expr),+) => {{
            let mut n = $b.clone();
            $(n.push($p);)+
            n
        }};
    }

macro_rules! init_pool {
    () => {
        SqlitePool::connect_with(
            SqliteConnectOptions::from_str(format!("sqlite:{}", DB_PATH).as_str())
                .unwrap()
                .locking_mode(sqlx::sqlite::SqliteLockingMode::Normal)
                .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal),
        )
    };
}

macro_rules! init_sess {
    ($rt:expr, $pool:expr) => {
        spawn_mount2(
            HTFS {
                runtime_handle: $rt.handle().clone(),
                tag_prefix: "#".to_string(),
                pool: $pool.clone(),
            },
            MP_PATH,
            &[],
        )
        .unwrap()
    };
}

macro_rules! init_paths {
    () => {
        create_dir(MP_PATH).unwrap();
        File::create_new(DB_PATH).unwrap();
    };
}

macro_rules! cleanup_paths {
    () => {
        remove_dir(MP_PATH).unwrap();
        remove_file(DB_PATH).unwrap();
    };
}

macro_rules! reg_method {
    ($m:ident) => {
        mod $m;
        use $m::*;
    };
}

macro_rules! load_prelude {
    () => {
        use super::prelude::*;
    };
}

macro_rules! read_file_query {
    () => {
        query_scalar(
            "SELECT GROUP_CONCAT(bytes, '') FROM (SELECT bytes FROM file_contents WHERE ino = ? \
             ORDER BY page)",
        )
    };
}
