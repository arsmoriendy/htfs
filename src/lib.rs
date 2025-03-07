mod db_types;
mod test_db;
mod test_fs;

use async_std::task;
use db_types::{from_filetype, from_systime, mode_to_filetype, FileAttrRow, ReadDirRow};
use fuser::*;
use libc::c_int;
use sqlx::{query, query::QueryAs, query_as, Error, Pool, Sqlite};
use std::time::{Duration, SystemTime};

struct TagFileSystem {
    pool: Box<Pool<Sqlite>>,
}

impl TagFileSystem {
    async fn get_ass_tags(&self, ino: u64) -> Vec<u64> {
        let ptags_res: Result<Vec<(u64,)>, Error> =
            query_as("SELECT tid FROM associated_tags WHERE ino = ?")
                .bind(ino as i64)
                .fetch_all(self.pool.as_ref())
                .await;

        match ptags_res {
            Ok(p) => p.iter().map(|r| r.0).collect(),
            Err(e) => panic!("{e}"),
        }
    }
}

impl Filesystem for TagFileSystem {
    fn init(&mut self, req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), c_int> {
        task::block_on(async {
            if let None = query("SELECT 1 FROM file_attrs WHERE ino = 1")
                .fetch_optional(self.pool.as_ref())
                .await
                .unwrap()
            {
                ins_attrs!(
                    query,
                    FileAttr {
                        ino: 0,
                        nlink: 1,
                        rdev: 0,

                        // TODO: size related
                        size: 0,
                        blocks: 0,

                        atime: SystemTime::now(),
                        mtime: SystemTime::now(),
                        ctime: SystemTime::now(),
                        crtime: SystemTime::now(),
                        kind: FileType::Directory,

                        // TODO: permission related, sync with original dir mayhaps?
                        perm: 0o777,

                        uid: req.uid(),
                        gid: req.gid(),

                        // TODO: misc
                        blksize: 0,
                        flags: 0,
                    }
                )
                .execute(self.pool.as_ref())
                .await
                .unwrap();
            };
        });
        return Ok(());
    }

    fn destroy(&mut self) {
        task::block_on(self.pool.close());
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        task::block_on(async {
            match query_as::<_, FileAttrRow>("SELECT * FROM file_attrs WHERE ino = ?")
                .bind(ino as i64)
                .fetch_one(self.pool.as_ref())
                .await
            {
                Ok(r) => reply.attr(&Duration::from_secs(1), &r.into()),
                Err(e) => match e {
                    Error::RowNotFound => reply.error(libc::ENOENT),
                    _ => panic!("{e}"),
                },
            };
        });
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

    fn mknod(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        task::block_on(async {
            // TODO: handle duplicates

            let kind = mode_to_filetype(mode).unwrap();

            if kind != FileType::RegularFile {
                eprintln!("tfs currently only supports regular files");
                reply.error(libc::ENOSYS);
                return;
            }

            let now = SystemTime::now();

            // TODO: figure out perm/mode S_ISUID/S_ISGID/S_ISVTX (inode(7))
            let f_attrs = FileAttr {
                ino: 0,
                size: 0,
                blocks: 0,
                atime: now,
                mtime: now,
                ctime: now,
                crtime: now,
                kind,
                perm: mode as u16,
                nlink: 1,
                uid: req.uid(),
                gid: req.gid(),
                rdev: 0,
                blksize: 0,
                flags: 0,
            };

            let ino: u64 = ins_attrs!(query_as::<_, (u64,)>, f_attrs, "RETURNING ino")
                .fetch_one(self.pool.as_ref())
                .await
                .unwrap()
                .0;

            query("INSERT INTO file_names VALUES (?, ?)")
                .bind(ino as i64)
                .bind(name.to_str())
                .execute(self.pool.as_ref())
                .await
                .unwrap();

            // associate created directory with parent tags
            for ptag in self.get_ass_tags(parent).await {
                query("INSERT INTO associated_tags VALUES (?, ?)")
                    .bind(ptag as i64)
                    .bind(ino as i64)
                    .execute(self.pool.as_ref())
                    .await
                    .unwrap();
            }

            reply.entry(&Duration::from_secs(1), &f_attrs, 0);
        });
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

    fn mkdir(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        task::block_on(async {
            // TODO: parent permissions, need impl mountpoint FileAttrs first (for if ino = 1)
            // TODO: update parent time attrs
            // TODO: handle duplicates

            let now = SystemTime::now();

            // TODO: size
            // TODO: figure out perm/mode S_ISUID/S_ISGID/S_ISVTX (inode(7))
            let f_attrs = FileAttr {
                ino: 0,
                size: 0,
                blocks: 0,
                atime: now,
                mtime: now,
                ctime: now,
                crtime: now,
                kind: FileType::Directory,
                perm: mode as u16,
                nlink: 1,
                uid: req.uid(),
                gid: req.gid(),
                rdev: 0,
                blksize: 0,
                flags: 0,
            };

            let ino: u64 = ins_attrs!(query_as::<_, (u64,)>, f_attrs, "RETURNING ino")
                .fetch_one(self.pool.as_ref())
                .await
                .unwrap()
                .0;

            query("INSERT INTO file_names VALUES (?, ?)")
                .bind(ino as i64)
                .bind(name.to_str())
                .execute(self.pool.as_ref())
                .await
                .unwrap();

            if parent != 1 {
                // insert into dir_contents
                if let Err(e) = query("INSERT INTO dir_contents VALUES (?, ?)")
                    .bind(parent as i64)
                    .bind(ino as i64)
                    .execute(self.pool.as_ref())
                    .await
                {
                    todo!("{e}")
                };
            }

            // create tag if doesn't exists
            let tid = match query_as::<_, (u64,)>("SELECT tid FROM tags WHERE name = ?")
                .bind(name.to_str())
                .fetch_optional(self.pool.as_ref())
                .await
                .unwrap()
            {
                Some(tid_row) => tid_row.0,
                None => {
                    query_as::<_, (u64,)>("INSERT INTO tags(name) VALUES (?) RETURNING tid")
                        .bind(name.to_str())
                        .fetch_one(self.pool.as_ref())
                        .await
                        .unwrap()
                        .0
                }
            };

            // associate created directory with the tid above
            query("INSERT INTO associated_tags VALUES (?, ?)")
                .bind(tid as i64)
                .bind(ino as i64)
                .execute(self.pool.as_ref())
                .await
                .unwrap();

            // associate created directory with parent tags
            for ptag in self.get_ass_tags(parent).await {
                query("INSERT INTO associated_tags VALUES (?, ?)")
                    .bind(ptag as i64)
                    .bind(ino as i64)
                    .execute(self.pool.as_ref())
                    .await
                    .unwrap();
            }

            reply.entry(&Duration::from_secs(1), &f_attrs, 1);
        });
    }
}
