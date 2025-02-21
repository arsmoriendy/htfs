mod db_types;
mod test_db;
mod test_fs;

use async_std::task;
use db_types::ReadDirRow;
use fuser::*;
use libc::c_int;
use sqlx::{query_as, Pool, Sqlite};
use std::time::{Duration, SystemTime};

struct TagFileSystem {
    pool: Box<Pool<Sqlite>>,
}

impl Filesystem for TagFileSystem {
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), c_int> {
        return Ok(());
    }

    fn destroy(&mut self) {
        task::block_on(self.pool.close());
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        // inode 1 is the mountpoint directory itself
        if ino == 1 {
            return reply.attr(
                &Duration::from_secs(1),
                &FileAttr {
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
            );
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
        let res: Vec<ReadDirRow> = task::block_on(
            query_as("SELECT * FROM readdir_rows WHERE ino >= ?")
                .bind(offset)
                .fetch_all(self.pool.as_ref()),
        )
        .unwrap();

        for row in res {
            let row_attr: FileAttr = (&row).try_into().unwrap();

            if reply.add(
                row_attr.ino,
                (row_attr.ino + 1) as i64,
                row_attr.kind,
                row.name,
            ) {
                break;
            };
        }

        reply.ok();
    }
}
