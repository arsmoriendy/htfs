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

    async fn get_ass_tag(&self, ino: u64) -> Option<Vec<u64>> {
        let ptags_res: Result<Vec<(u64,)>, Error> =
            query_as("SELECT tid FROM associated_tags WHERE ino = ?")
                .bind(ino as i64)
                .fetch_all(self.pool.as_ref())
                .await;

        match ptags_res {
            Ok(p) => Some(p.iter().map(|r| r.0).collect()),
            Err(e) => match e {
                Error::RowNotFound => None,
                _ => panic!("{e}"),
            },
        }
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
            let kind = mode_to_filetype(mode).unwrap();

            if kind != FileType::RegularFile {
                println!("tfs currently only supports regular files");
                reply.error(libc::ENOSYS);
                return;
            }

            let ino = self.gen_inode().await;
            let now = SystemTime::now();

            // TODO: figure out perm/mode S_ISUID/S_ISGID/S_ISVTX (inode(7))
            let f_attrs = FileAttr {
                ino,
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

            let now_s = from_systime(now);

            query("INSERT INTO file_attrs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
                .bind(f_attrs.ino as i64) // ino INTEGER PRIMARY KEY,
                .bind(f_attrs.size as i64) // size INTEGER,
                .bind(f_attrs.blocks as i64) // blocks INTEGER,
                .bind(now_s as i64) // atime INTEGER,
                .bind(now_s as i64) // mtime INTEGER,
                .bind(now_s as i64) // ctime INTEGER,
                .bind(now_s as i64) // crtime INTEGER,
                .bind(from_filetype(f_attrs.kind)) // kind INTEGER,
                .bind(f_attrs.perm) // perm INTEGER,
                .bind(f_attrs.nlink) // nlink INTEGER,
                .bind(f_attrs.uid) // uid INTEGER,
                .bind(f_attrs.gid) // gid INTEGER,
                .bind(f_attrs.rdev) // rdev INTEGER,
                .bind(f_attrs.blksize) // blksize INTEGER,
                .bind(f_attrs.flags) // flags INTEGER,
                .execute(self.pool.as_ref())
                .await
                .unwrap();

            query("INSERT INTO file_names VALUES (?, ?)")
                .bind(ino as i64)
                .bind(name.to_str())
                .execute(self.pool.as_ref())
                .await
                .unwrap();

            // get parent tags
            if let Some(ptags) = self.get_ass_tag(parent).await {
                // associate created directory with parent tags
                for ptag in ptags {
                    query("INSERT INTO associated_tags VALUES (?, ?)")
                        .bind(ptag as i64)
                        .bind(ino as i64)
                        .execute(self.pool.as_ref())
                        .await
                        .unwrap();
                }
            };

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

            let ino = self.gen_inode().await;
            let now = SystemTime::now();

            // TODO: size
            // TODO: figure out perm/mode S_ISUID/S_ISGID/S_ISVTX (inode(7))
            let f_attrs = FileAttr {
                ino,
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

            let now_s = from_systime(now);

            query("INSERT INTO file_attrs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
                .bind(f_attrs.ino as i64) // ino INTEGER PRIMARY KEY,
                .bind(f_attrs.size as i64) // size INTEGER,
                .bind(f_attrs.blocks as i64) // blocks INTEGER,
                .bind(now_s as i64) // atime INTEGER,
                .bind(now_s as i64) // mtime INTEGER,
                .bind(now_s as i64) // ctime INTEGER,
                .bind(now_s as i64) // crtime INTEGER,
                .bind(from_filetype(f_attrs.kind)) // kind INTEGER,
                .bind(f_attrs.perm) // perm INTEGER,
                .bind(f_attrs.nlink) // nlink INTEGER,
                .bind(f_attrs.uid) // uid INTEGER,
                .bind(f_attrs.gid) // gid INTEGER,
                .bind(f_attrs.rdev) // rdev INTEGER,
                .bind(f_attrs.blksize) // blksize INTEGER,
                .bind(f_attrs.flags) // flags INTEGER,
                .execute(self.pool.as_ref())
                .await
                .unwrap();

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

            // get parent tags
            if let Some(ptags) = self.get_ass_tag(parent).await {
                // associate created directory with parent tags
                for ptag in ptags {
                    query("INSERT INTO associated_tags VALUES (?, ?)")
                        .bind(ptag as i64)
                        .bind(ino as i64)
                        .execute(self.pool.as_ref())
                        .await
                        .unwrap();
                }
            };

            reply.entry(&Duration::from_secs(1), &f_attrs, 1);
        });
    }
}
