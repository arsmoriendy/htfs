mod db_types;
mod test_db;
mod test_fs;

use async_std::task;
use db_types::{FileAttrRow, ReadDirRow};
use fuser::*;
use libc::c_int;
use sqlx::{query, query::QueryAs, query_as, Error, Pool, Sqlite};
use std::time::{Duration, SystemTime};

struct TagFileSystem {
    pool: Box<Pool<Sqlite>>,
}

impl TagFileSystem {
    async fn gen_inode(&self) -> u64 {
        let last_res: Option<(i64,)> =
            query_as("SELECT ino FROM file_attrs ORDER BY ino DESC LIMIT 1")
                .fetch_optional(self.pool.as_ref())
                .await
                .unwrap();

        if let Some(last) = last_res {
            if last.0 >= 2 {
                return (last.0 + 1) as u64;
            }
        }

        return 2;
    }
}

impl Filesystem for TagFileSystem {
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), c_int> {
        return Ok(());
    }

    fn destroy(&mut self) {
        task::block_on(self.pool.close());
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        let attr = match ino {
            1 => FileAttr {
                ino: 1,
                nlink: 1,
                rdev: 0,

                // TODO: size related
                size: 0,
                blocks: 0,

                // TODO: time related
                atime: SystemTime::UNIX_EPOCH,
                mtime: SystemTime::UNIX_EPOCH,
                ctime: SystemTime::UNIX_EPOCH,
                crtime: SystemTime::UNIX_EPOCH,

                kind: FileType::Directory,

                // TODO: permission related, sync with original dir mayhaps?
                perm: 0o755, // rwx r-x r-x

                // TODO: user/group related
                uid: _req.uid(),
                gid: _req.gid(),

                // TODO: misc
                blksize: 0,
                flags: 0,
            },
            _ => {
                let row: FileAttrRow = task::block_on(
                    query_as("SELECT * FROM file_attrs WHERE ino = ?")
                        .bind(ino as i64)
                        .fetch_one(self.pool.as_ref()),
                )
                .unwrap();

                row.into()
            }
        };

        reply.attr(&Duration::from_secs(1), &attr);
    }

    fn lookup(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: ReplyEntry,
    ) {
        let query: QueryAs<'_, _, FileAttrRow, _> = match parent {
            1 => query_as("SELECT * FROM readdir_rows WHERE name = ?").bind(name.to_str()),
            _ => todo!(),
        };

        let res = task::block_on(query.fetch_one(self.pool.as_ref()));
        match res {
            Ok(row) => {
                return reply.entry(&Duration::from_secs(1), &row.into(), 1);
            }
            Err(e) => match e {
                Error::RowNotFound => return reply.error(libc::ENOENT),
                _ => panic!("{e}"),
            },
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        // TODO: implement _ino
        let rows: Vec<ReadDirRow> = task::block_on(
            query_as("SELECT * FROM readdir_rows WHERE ino >= ?")
                .bind(offset)
                .fetch_all(self.pool.as_ref()),
        )
        .unwrap();

        for row in rows {
            let attr: FileAttr = row.attr.into();

            if reply.add(attr.ino, (attr.ino + 1) as i64, attr.kind, row.name) {
                break;
            };
        }

        reply.ok();
    }
}
