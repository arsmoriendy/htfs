use crate::{
    db_helpers::{
        try_bind_attrs,
        types::{mode_to_filetype, to_filetype, DBError, FileAttrRow, ReadDirRow},
    },
    TagFileSystem,
};
use async_std::task;
use fuser::*;
use libc::c_int;
use sqlx::{query, query_as, QueryBuilder, Sqlite};
use std::{
    num::TryFromIntError,
    time::{Duration, SystemTime},
};

fn handle_from_int_err<T>(expr: Result<T, TryFromIntError>) -> Result<T, c_int> {
    expr.map_err(|e| {
        tracing::error!("{e}");
        libc::ERANGE
    })
}

macro_rules! handle_from_int_err {
    ($e: expr, $reply: expr) => {
        match handle_from_int_err($e) {
            Ok(v) => v,
            Err(e) => {
                $reply.error(e);
                return;
            }
        }
    };
}

fn handle_db_err<T, E>(expr: Result<T, E>) -> Result<T, c_int>
where
    E: Into<DBError>,
{
    expr.map_err(|e| {
        let db_err: DBError = e.into();
        let (code, s) = db_err.map_db_err();
        tracing::error!(s);
        code
    })
}

macro_rules! handle_db_err {
    ($e: expr, $reply: expr) => {
        match handle_db_err($e) {
            Ok(v) => v,
            Err(e) => {
                $reply.error(e);
                return;
            }
        }
    };
}

macro_rules! handle_auth_perm {
    ($self: expr, $ino: expr, $req: expr, $reply: expr, $rwx: expr) => {
        let has_perm = handle_db_err!($self.req_has_ino_perm($ino, $req, $rwx).await, $reply);
        if !has_perm {
            $reply.error(libc::EACCES);
            return;
        }
    };
}

impl Filesystem for TagFileSystem<'_> {
    #[tracing::instrument]
    fn init(&mut self, req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), c_int> {
        task::block_on(async {
            // create mountpoint attr if not exist
            let q = handle_db_err(try_bind_attrs(
                query("INSERT OR IGNORE INTO file_attrs VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)"),
                &FileAttr {
                    ino: 1,
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
                },
            ))?;
            handle_db_err(q.execute(self.pool).await)?;

            Ok(())
        })
    }

    #[tracing::instrument]
    fn destroy(&mut self) {
        task::block_on(self.pool.close());
    }

    #[tracing::instrument]
    fn getattr(&mut self, req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        task::block_on(async {
            handle_auth_perm!(self, ino, req, reply, 0b100);

            let attr_row = handle_db_err!(
                query_as::<_, FileAttrRow>("SELECT * FROM file_attrs WHERE ino = ?")
                    .bind(ino as i64)
                    .fetch_one(self.pool)
                    .await,
                reply
            );

            let attr = handle_db_err!(FileAttr::try_from(attr_row), reply);

            reply.attr(&Duration::from_secs(1), &attr);
        });
    }

    #[tracing::instrument]
    fn lookup(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: ReplyEntry,
    ) {
        task::block_on(async {
            handle_auth_perm!(self, parent, req, reply, 0b100);

            let mut query_builder =
                QueryBuilder::<Sqlite>::new("SELECT * FROM readdir_rows WHERE (ino IN (");

            let ptags = handle_db_err!(self.get_ass_tags(parent).await, reply);
            for ptag in ptags.iter().enumerate() {
                query_builder
                    .push("SELECT ino FROM associated_tags WHERE tid = ")
                    .push_bind(*ptag.1 as i64);
                if ptag.0 != ptags.len() - 1 {
                    query_builder.push(" AND ino IN (");
                }
            }
            for _ in ptags.iter().skip(1) {
                query_builder.push(")");
            }

            query_builder
                .push(
                    ") AND kind != 3 OR ino IN (SELECT cnt_ino FROM dir_contents WHERE dir_ino = ",
                )
                .push_bind(parent as i64)
                .push(")) AND ino != ")
                .push_bind(parent as i64)
                .push(" AND name = ")
                .push_bind(name.to_str());

            let row = handle_db_err!(
                query_builder
                    .build_query_as::<ReadDirRow>()
                    .fetch_one(self.pool)
                    .await,
                reply
            );
            let attr = handle_db_err!(FileAttr::try_from(row.attr), reply);
            reply.entry(&Duration::from_secs(1), &attr, 0);
        });
    }

    #[tracing::instrument]
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
            handle_auth_perm!(self, parent, req, reply, 0b010);

            // TODO: handle duplicates

            let kind = handle_db_err!(mode_to_filetype(mode), reply);

            if kind != FileType::RegularFile {
                tracing::error!("tfs currently only supports regular files");
                reply.error(libc::ENOSYS);
                return;
            }

            let now = SystemTime::now();

            // TODO: figure out perm/mode S_ISUID/S_ISGID/S_ISVTX (inode(7))
            let mut f_attrs = FileAttr {
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

            f_attrs.ino = handle_db_err!(self.ins_attrs(&f_attrs).await, reply);

            handle_db_err!(
                query("INSERT INTO file_names VALUES (?, ?)")
                    .bind(f_attrs.ino as i64)
                    .bind(name.to_str())
                    .execute(self.pool)
                    .await,
                reply
            );

            // associate created directory with parent tags
            for ptag in handle_db_err!(self.get_ass_tags(parent).await, reply) {
                handle_db_err!(
                    query("INSERT INTO associated_tags VALUES (?, ?)")
                        .bind(ptag as i64)
                        .bind(f_attrs.ino as i64)
                        .execute(self.pool)
                        .await,
                    reply
                );
            }

            handle_db_err!(self.sync_mtime(parent).await, reply);

            reply.entry(&Duration::from_secs(1), &f_attrs, 0);
        });
    }

    #[tracing::instrument]
    fn readdir(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        task::block_on(async {
            handle_auth_perm!(self, ino, req, reply, 0b100);

            let mut query_builder =
                QueryBuilder::<Sqlite>::new("SELECT * FROM readdir_rows WHERE (ino IN (");

            let ptags = handle_db_err!(self.get_ass_tags(ino).await, reply);
            for ptag in ptags.iter().enumerate() {
                query_builder
                    .push("SELECT ino FROM associated_tags WHERE tid = ")
                    .push_bind(*ptag.1 as i64);
                if ptag.0 != ptags.len() - 1 {
                    query_builder.push(" AND ino IN (");
                }
            }
            for _ in ptags.iter().skip(1) {
                query_builder.push(")");
            }

            query_builder
                .push(
                    ") AND kind != 3 OR ino IN (SELECT cnt_ino FROM dir_contents WHERE dir_ino = ",
                )
                .push_bind(ino as i64)
                .push(")) AND ino != ")
                .push_bind(ino as i64)
                .push(" ORDER BY ino LIMIT -1 OFFSET ")
                .push_bind(offset);

            let rows = handle_db_err!(
                query_builder
                    .build_query_as::<ReadDirRow>()
                    .fetch_all(self.pool)
                    .await,
                reply
            );

            for row in rows.iter().enumerate() {
                let attr = &row.1.attr;
                let name = &row.1.name;
                let ftyp = handle_db_err!(to_filetype(attr.kind), reply);

                if reply.add(attr.ino, offset + row.0 as i64 + 1, ftyp, name) {
                    break;
                };
            }
            handle_db_err!(self.sync_atime(ino).await, reply);
            reply.ok();
        });
    }

    #[tracing::instrument]
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
            handle_auth_perm!(self, parent, req, reply, 0b010);

            // TODO: handle duplicates

            let now = SystemTime::now();

            // TODO: size
            // TODO: figure out perm/mode S_ISUID/S_ISGID/S_ISVTX (inode(7))
            let mut f_attrs = FileAttr {
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

            f_attrs.ino = handle_db_err!(self.ins_attrs(&f_attrs).await, reply);

            handle_db_err!(
                query("INSERT INTO file_names VALUES (?, ?)")
                    .bind(f_attrs.ino as i64)
                    .bind(name.to_str())
                    .execute(self.pool)
                    .await,
                reply
            );

            handle_db_err!(
                query("INSERT INTO dir_contents VALUES (?, ?)")
                    .bind(parent as i64)
                    .bind(f_attrs.ino as i64)
                    .execute(self.pool)
                    .await,
                reply
            );

            // create tag if doesn't exists
            let tid = match handle_db_err!(
                query_as::<_, (u64,)>("SELECT tid FROM tags WHERE name = ?")
                    .bind(name.to_str())
                    .fetch_optional(self.pool)
                    .await,
                reply
            ) {
                Some(tid_row) => tid_row.0,
                None => {
                    handle_db_err!(
                        query_as::<_, (u64,)>("INSERT INTO tags(name) VALUES (?) RETURNING tid")
                            .bind(name.to_str())
                            .fetch_one(self.pool)
                            .await,
                        reply
                    )
                    .0
                }
            };

            // associate created directory with the tid above
            handle_db_err!(
                query("INSERT INTO associated_tags VALUES (?, ?)")
                    .bind(tid as i64)
                    .bind(f_attrs.ino as i64)
                    .execute(self.pool)
                    .await,
                reply
            );

            // associate created directory with parent tags
            for ptag in handle_db_err!(self.get_ass_tags(parent).await, reply) {
                handle_db_err!(
                    query("INSERT INTO associated_tags VALUES (?, ?)")
                        .bind(ptag as i64)
                        .bind(f_attrs.ino as i64)
                        .execute(self.pool)
                        .await,
                    reply
                );
            }

            handle_db_err!(self.sync_mtime(parent).await, reply);

            reply.entry(&Duration::from_secs(1), &f_attrs, 1);
        });
    }

    #[tracing::instrument]
    fn rmdir(&mut self, req: &Request<'_>, parent: u64, name: &std::ffi::OsStr, reply: ReplyEmpty) {
        task::block_on(async {
            handle_auth_perm!(self, parent, req, reply, 0b010);

            let (ino,) = handle_db_err!(query_as::<_,(i64,)>("SELECT cnt_ino FROM dir_contents INNER JOIN file_names ON file_names.ino = dir_contents.cnt_ino WHERE dir_ino = ? AND name = ?")
                .bind(parent as i64)
                .bind(name.to_str().unwrap())
                .fetch_one(self.pool)
                .await, reply);

            handle_db_err!(
                query("DELETE FROM file_attrs WHERE ino = ?")
                    .bind(ino)
                    .execute(self.pool)
                    .await,
                reply
            );

            reply.ok();
        })
    }

    #[tracing::instrument]
    fn unlink(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: ReplyEmpty,
    ) {
        task::block_on(async {
            handle_auth_perm!(self, parent, req, reply, 0b010);

            let mut query_builder =
                QueryBuilder::<Sqlite>::new("SELECT * FROM readdir_rows WHERE (ino IN (");

            let ptags = handle_db_err!(self.get_ass_tags(parent).await, reply);
            for ptag in ptags.iter().enumerate() {
                query_builder
                    .push("SELECT ino FROM associated_tags WHERE tid = ")
                    .push_bind(*ptag.1 as i64);
                if ptag.0 != ptags.len() - 1 {
                    query_builder.push(" AND ino IN (");
                }
            }
            for _ in ptags.iter().skip(1) {
                query_builder.push(")");
            }

            query_builder
                .push(") OR ino IN (SELECT cnt_ino FROM dir_contents WHERE dir_ino = ")
                .push_bind(parent as i64)
                .push(")) AND ino != ")
                .push_bind(parent as i64)
                .push(" AND name = ")
                .push_bind(name.to_str());

            let f_attrs = handle_db_err!(
                query_builder
                    .build_query_as::<FileAttrRow>()
                    .fetch_one(self.pool)
                    .await,
                reply
            );

            handle_auth_perm!(self, f_attrs.ino, req, reply, 0b010);

            handle_db_err!(
                query("DELETE FROM file_attrs WHERE ino = ?")
                    .bind(f_attrs.ino as i64)
                    .execute(self.pool)
                    .await,
                reply
            );

            reply.ok();
        });
    }

    #[tracing::instrument]
    fn setattr(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        _fh: Option<u64>,
        crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        task::block_on(async {
            handle_auth_perm!(self, ino, req, reply, 0b010);

            let row = handle_db_err!(
                query_as::<_, FileAttrRow>("SELECT * FROM file_attrs WHERE ino = $1")
                    .bind(ino as i64)
                    .fetch_one(self.pool)
                    .await,
                reply
            );

            let mut attr: FileAttr = handle_db_err!(FileAttr::try_from(row), reply).into();

            attr.size = match size {
                Some(s) => {
                    handle_db_err!(query("UPDATE file_contents SET content = CAST(SUBSTR(content, 1, $1) AS BLOB) WHERE ino = $2")
                        .bind(s as i64)
                        .bind(ino as i64)
                        .execute(self.pool)
                        .await, reply);
                    s
                }
                None => attr.size,
            };
            attr.atime = atime.map_or(attr.atime, |tn| match tn {
                TimeOrNow::Now => SystemTime::now(),
                TimeOrNow::SpecificTime(t) => t,
            });
            attr.mtime = mtime.map_or(attr.mtime, |tn| match tn {
                TimeOrNow::Now => SystemTime::now(),
                TimeOrNow::SpecificTime(t) => t,
            });
            attr.ctime = ctime.unwrap_or(SystemTime::now());
            attr.crtime = crtime.unwrap_or(attr.crtime);
            attr.flags = flags.unwrap_or(attr.flags);
            // TODO: handle change mode filetype case?
            attr.perm = mode.map_or(attr.perm, |m| m as u16);
            attr.uid = uid.unwrap_or(attr.uid);
            attr.gid = gid.unwrap_or(attr.gid);

            handle_db_err!(self.upd_attrs(&attr).await, reply);

            reply.attr(&Duration::from_secs(1), &attr);
        })
    }

    #[tracing::instrument]
    fn write(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        task::block_on(async {
            handle_auth_perm!(self, ino, req, reply, 0b010);

            let dat_len = i64::try_from(data.len()).unwrap();

            let cnt_len = handle_db_err!(
                query_as::<_, (i64,)>("SELECT LENGTH(content) FROM file_contents WHERE ino = $1")
                    .bind(ino as i64)
                    .fetch_optional(self.pool)
                    .await,
                reply
            );

            let pad_len: Option<i64> = match cnt_len {
                Some((l,)) => {
                    if offset > l {
                        Some(offset - l)
                    } else {
                        None
                    }
                }
                None => None,
            };

            // cast to BLOB because sqlite converts all concat (||) expressions to TEXT
            // https://stackoverflow.com/questions/55301281/update-query-to-append-zeroes-into-blob-field-with-sqlitestudio
            handle_db_err!(query("INSERT INTO file_contents VALUES ($4, CAST(ZEROBLOB($5) || $2 AS BLOB)) ON CONFLICT(ino) DO UPDATE SET content = CAST(SUBSTR(content, 1, $1) || ZEROBLOB($5) || $2 || SUBSTR(content, $3) AS BLOB) WHERE ino = $4")
                .bind(offset)
                .bind(data)
                .bind(offset + 1 + dat_len )
                .bind(ino as i64)
                .bind(pad_len.unwrap_or(0))
                .execute(self.pool)
                .await, reply);

            handle_db_err!(query("UPDATE file_attrs SET size = (SELECT LENGTH(content) FROM file_contents WHERE ino = $1) WHERE ino = $1")
                .bind(ino as i64)
                .execute(self.pool)
                .await, reply);

            handle_db_err!(self.sync_mtime(ino).await, reply);

            reply.written(dat_len.try_into().unwrap());
        });
    }

    #[tracing::instrument]
    fn read(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        task::block_on(async {
            handle_auth_perm!(self, ino, req, reply, 0b100);

            let data = handle_db_err!(
                query_as::<_, (Box<[u8]>,)>(
                    "SELECT SUBSTR(content, $1, $2) FROM file_contents WHERE ino = $3",
                )
                .bind(offset)
                .bind(size)
                .bind(ino as i64)
                .fetch_one(self.pool)
                .await,
                reply
            )
            .0;

            handle_db_err!(self.sync_atime(ino).await, reply);
            reply.data(Box::leak(data));
        });
    }
}
