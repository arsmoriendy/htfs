use crate::{
    TagFileSystem,
    db_helpers::{
        chain_tagged_inos, try_bind_attrs,
        types::{FileAttrRow, ReadDirRow, mode_to_filetype, to_filetype},
    },
    handle_db_err, handle_from_int_err,
};
use fuser::*;
use libc::c_int;
use sqlx::{QueryBuilder, Sqlite, migrate, query, query_as, query_scalar};
use std::time::{Duration, SystemTime};

impl Filesystem for TagFileSystem<Sqlite> {
    #[tracing::instrument]
    fn init(&mut self, req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), c_int> {
        self.rt.block_on(async {
            migrate!().run(&self.pool).await.unwrap();

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
            handle_db_err(q.execute(&self.pool).await)?;

            Ok(())
        })
    }

    #[tracing::instrument]
    fn destroy(&mut self) {
        // TODO: delete shm and wal files
        self.rt.block_on(async {
            let _c = &self.pool.close().await;
        });
    }

    #[tracing::instrument]
    fn getattr(&mut self, req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        self.rt.block_on(async {
            handle_auth_perm!(self, ino, req, reply, 0b100);

            let attr_row = handle_db_err!(
                query_as::<_, FileAttrRow>("SELECT * FROM file_attrs WHERE ino = ?")
                    .bind(to_i64!(ino, reply))
                    .fetch_one(&self.pool)
                    .await,
                reply
            );

            let attr = handle_db_err!(FileAttr::try_from(&attr_row), reply);

            reply.attr(&Duration::from_secs(1), &attr);
        });
    }

    // TODO: prefix
    #[tracing::instrument]
    fn lookup(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: ReplyEntry,
    ) {
        self.rt.block_on(async {
            handle_auth_perm!(self, parent, req, reply, 0b100);

            let mut query_builder =
                QueryBuilder::<Sqlite>::new("SELECT * FROM readdir_rows WHERE (ino IN (");

            let ptags = handle_db_err!(self.get_ass_tags(parent).await, reply);
            handle_db_err!(chain_tagged_inos(&mut query_builder, &ptags), reply);

            query_builder
                .push(
                    ") AND kind != 3 OR ino IN (SELECT cnt_ino FROM dir_contents WHERE dir_ino = ",
                )
                .push_bind(to_i64!(parent, reply))
                .push(")) AND name = ")
                .push_bind(name.to_str());

            let row = handle_db_err!(
                query_builder
                    .build_query_as::<ReadDirRow>()
                    .fetch_one(&self.pool)
                    .await,
                reply
            );
            let attr = handle_db_err!(FileAttr::try_from(&row.attr), reply);
            reply.entry(&Duration::from_secs(1), &attr, 0);
        });
    }

    // TODO: prefix
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
        self.rt.block_on(async {
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
                    .bind(to_i64!(f_attrs.ino, reply))
                    .bind(name.to_str())
                    .execute(&self.pool)
                    .await,
                reply
            );

            let parent_name = handle_db_err!(
                query_scalar::<_, String>("SELECT name FROM file_names WHERE ino = ?")
                    .bind(to_i64!(parent, reply))
                    .fetch_one(&self.pool)
                    .await,
                reply
            );

            if self.is_prefixed(parent_name.as_str()) {
                // associate created directory with parent tags
                for ptag in handle_db_err!(self.get_ass_tags(parent).await, reply) {
                    handle_db_err!(
                        query("INSERT INTO associated_tags VALUES (?, ?)")
                            .bind(to_i64!(ptag, reply))
                            .bind(to_i64!(f_attrs.ino, reply))
                            .execute(&self.pool)
                            .await,
                        reply
                    );
                }
            } else {
                handle_db_err!(
                    query("INSERT INTO dir_contents (dir_ino, cnt_ino) VALUES (?, ?)")
                        .bind(to_i64!(parent, reply))
                        .bind(to_i64!(f_attrs.ino, reply))
                        .execute(&self.pool)
                        .await,
                    reply
                );
            }

            handle_db_err!(self.sync_mtime(parent).await, reply);

            reply.entry(&Duration::from_secs(1), &f_attrs, 0);
        });
    }

    // TODO: prefix
    #[tracing::instrument]
    fn readdir(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        self.rt.block_on(async {
            handle_auth_perm!(self, ino, req, reply, 0b100);

            let mut query_builder =
                QueryBuilder::<Sqlite>::new("SELECT * FROM readdir_rows WHERE (ino IN (");

            let ptags = handle_db_err!(self.get_ass_tags(ino).await, reply);
            handle_db_err!(chain_tagged_inos(&mut query_builder, &ptags), reply);

            query_builder
                .push(
                    ") AND kind != 3 OR ino IN (SELECT cnt_ino FROM dir_contents WHERE dir_ino = ",
                )
                .push_bind(to_i64!(ino, reply))
                .push(")) ORDER BY ino LIMIT -1 OFFSET ")
                .push_bind(offset);

            let rows = handle_db_err!(
                query_builder
                    .build_query_as::<ReadDirRow>()
                    .fetch_all(&self.pool)
                    .await,
                reply
            );

            for row in rows.iter().enumerate() {
                let attr = &row.1.attr;
                let name = &row.1.name;
                let ftyp = handle_db_err!(to_filetype(attr.kind), reply);

                if reply.add(attr.ino, offset + to_i64!(row.0, reply) + 1, ftyp, name) {
                    break;
                };
            }
            handle_db_err!(self.sync_atime(ino).await, reply);
            reply.ok();
        });
    }

    // TODO: prefix
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
        self.rt.block_on(async {
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
                    .bind(to_i64!(f_attrs.ino, reply))
                    .bind(name.to_str())
                    .execute(&self.pool)
                    .await,
                reply
            );

            handle_db_err!(
                query("INSERT INTO dir_contents VALUES (?, ?)")
                    .bind(to_i64!(parent, reply))
                    .bind(to_i64!(f_attrs.ino, reply))
                    .execute(&self.pool)
                    .await,
                reply
            );

            if self.is_prefixed(name.to_str().unwrap()) {
                // create tag if doesn't exists
                let tid = match handle_db_err!(
                    query_scalar::<_, u64>("SELECT tid FROM tags WHERE name = ?")
                        .bind(name.to_str())
                        .fetch_optional(&self.pool)
                        .await,
                    reply
                ) {
                    Some(tid_row) => tid_row,
                    None => {
                        handle_db_err!(
                            query_scalar::<_, u64>(
                                "INSERT INTO tags(name) VALUES (?) RETURNING tid"
                            )
                            .bind(name.to_str())
                            .fetch_one(&self.pool)
                            .await,
                            reply
                        )
                    }
                };

                // associate created directory with the tid above
                handle_db_err!(
                    query("INSERT INTO associated_tags VALUES (?, ?)")
                        .bind(to_i64!(tid, reply))
                        .bind(to_i64!(f_attrs.ino, reply))
                        .execute(&self.pool)
                        .await,
                    reply
                );

                // associate created directory with parent tags
                for ptag in handle_db_err!(self.get_ass_tags(parent).await, reply) {
                    handle_db_err!(
                        query("INSERT INTO associated_tags VALUES (?, ?)")
                            .bind(to_i64!(ptag, reply))
                            .bind(to_i64!(f_attrs.ino, reply))
                            .execute(&self.pool)
                            .await,
                        reply
                    );
                }
            }

            handle_db_err!(self.sync_mtime(parent).await, reply);

            reply.entry(&Duration::from_secs(1), &f_attrs, 1);
        });
    }

    #[tracing::instrument]
    fn rmdir(&mut self, req: &Request<'_>, parent: u64, name: &std::ffi::OsStr, reply: ReplyEmpty) {
        self.rt.block_on(async {
            handle_auth_perm!(self, parent, req, reply, 0b010);

            let ino = handle_db_err!(query_scalar::<_,i64>("SELECT cnt_ino FROM dir_contents INNER JOIN file_names ON file_names.ino = dir_contents.cnt_ino WHERE dir_ino = ? AND name = ?")
                .bind(to_i64!(parent,reply))
                .bind(name.to_str().unwrap())
                .fetch_one(&self.pool)
                .await, reply);

            handle_db_err!(
                query("DELETE FROM file_attrs WHERE ino = ?")
                    .bind(ino)
                    .execute(&self.pool)
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
        self.rt.block_on(async {
            handle_auth_perm!(self, parent, req, reply, 0b010);

            let mut query_builder =
                QueryBuilder::<Sqlite>::new("SELECT * FROM readdir_rows WHERE (ino IN (");

            let ptags = handle_db_err!(self.get_ass_tags(parent).await, reply);
            handle_db_err!(chain_tagged_inos(&mut query_builder, &ptags), reply);

            query_builder
                .push(") OR ino IN (SELECT cnt_ino FROM dir_contents WHERE dir_ino = ")
                .push_bind(to_i64!(parent, reply))
                .push(")) AND name = ")
                .push_bind(name.to_str());

            let f_attrs = handle_db_err!(
                query_builder
                    .build_query_as::<FileAttrRow>()
                    .fetch_one(&self.pool)
                    .await,
                reply
            );

            handle_auth_perm!(self, f_attrs.ino, req, reply, 0b010);

            handle_db_err!(
                query("DELETE FROM file_attrs WHERE ino = ?")
                    .bind(to_i64!(f_attrs.ino, reply))
                    .execute(&self.pool)
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
        self.rt.block_on(async {
            handle_auth_perm!(self, ino, req, reply, 0b010);

            let row = handle_db_err!(
                query_as::<_, FileAttrRow>("SELECT * FROM file_attrs WHERE ino = $1")
                    .bind(to_i64!(ino, reply))
                    .fetch_one(&self.pool)
                    .await,
                reply
            );

            let mut attr: FileAttr = handle_db_err!(FileAttr::try_from(&row), reply).into();

            attr.size = match size {
                Some(s) => {
                    handle_db_err!(query("UPDATE file_contents SET content = CAST(SUBSTR(content, 1, $1) AS BLOB) WHERE ino = $2")
                        .bind(to_i64!(s,reply))
                        .bind(to_i64!(ino,reply))
                        .execute(&self.pool)
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
        self.rt.block_on(async {
            handle_auth_perm!(self, ino, req, reply, 0b010);

            let data_len = to_i64!(data.len(), reply);

            let cnt_len = handle_db_err!(
                query_scalar::<_, i64>("SELECT LENGTH(content) FROM file_contents WHERE ino = $1")
                    .bind(to_i64!(ino, reply))
                    .fetch_optional(&self.pool)
                    .await,
                reply
            );

            let pad_len: Option<i64> = match cnt_len {
                Some(l) => {
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
                .bind(offset + 1 + data_len )
                .bind(to_i64!(ino,reply))
                .bind(pad_len.unwrap_or(0))
                .execute(&self.pool)
                .await, reply);

            handle_db_err!(query("UPDATE file_attrs SET size = (SELECT LENGTH(content) FROM file_contents WHERE ino = $1) WHERE ino = $1")
                .bind(to_i64!(ino,reply))
                .execute(&self.pool)
                .await, reply);

            handle_db_err!(self.sync_mtime(ino).await, reply);

            let dat_len_32 = handle_from_int_err!(u32::try_from(data_len), reply);
            reply.written(dat_len_32);
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
        self.rt.block_on(async {
            handle_auth_perm!(self, ino, req, reply, 0b100);

            let data = handle_db_err!(
                query_scalar::<_, Box<[u8]>>(
                    "SELECT SUBSTR(content, $1, $2) FROM file_contents WHERE ino = $3",
                )
                .bind(offset)
                .bind(size)
                .bind(to_i64!(ino, reply))
                .fetch_one(&self.pool)
                .await,
                reply
            );

            handle_db_err!(self.sync_atime(ino).await, reply);
            reply.data(Box::leak(data));
        });
    }

    #[tracing::instrument]
    fn rename(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        newparent: u64,
        newname: &std::ffi::OsStr,
        _flags: u32, // TODO: what is this for?
        reply: ReplyEmpty,
    ) {
        self.rt.block_on(async {
            // check permissions on each parents
            handle_auth_perm!(self, parent, req, reply, 0b100);
            handle_auth_perm!(self, newparent, req, reply, 0b010);

            // get file attributes
            let mut query_builder =
                QueryBuilder::<Sqlite>::new("SELECT ino, kind FROM readdir_rows WHERE (ino IN (");
            let parent_tags = handle_db_err!(self.get_ass_tags(parent).await, reply);
            handle_db_err!(chain_tagged_inos(&mut query_builder, &parent_tags), reply);
            query_builder.push(") OR ino IN (SELECT cnt_ino FROM dir_contents WHERE dir_ino = ?)");
            query_builder.push(") AND name = ? LIMIT 2");

            let q = query_builder
                .build_query_as::<(u64, u64)>()
                .bind(to_i64!(parent, reply))
                .bind(name.to_str());

            let rows = handle_db_err!(q.fetch_all(&self.pool).await, reply);
            if rows.len() != 1 {
                if rows.len() > 1 {
                    tracing::error!("found duplicates")
                }
                reply.error(libc::ENOENT);
                return;
            }

            let (ino, kind) = rows[0];

            let filetype = handle_db_err!(
                to_filetype(handle_from_int_err!(u8::try_from(kind), reply)),
                reply
            );

            // check write permission on file
            handle_auth_perm!(self, ino, req, reply, 0b010);

            match filetype {
                FileType::Directory => {
                    // get children baesd on old tags
                    let old_tags = handle_db_err!(self.get_ass_tags(ino).await, reply);
                    let mut query_builder =
                        QueryBuilder::<Sqlite>::new("SELECT ino FROM file_attrs WHERE ino IN (");
                    handle_db_err!(chain_tagged_inos(&mut query_builder, &old_tags), reply);
                    query_builder
                        .push(") OR ino IN (SELECT cnt_ino FROM dir_contents WHERE dir_ino = ?)");
                    let children = handle_db_err!(
                        query_builder
                            .build_query_scalar::<u64>()
                            .bind(to_i64!(ino, reply))
                            .fetch_all(&self.pool)
                            .await,
                        reply
                    );

                    // remove all children associations
                    for child_ino in &children {
                        handle_db_err!(
                            query("DELETE FROM associated_tags WHERE ino = $1")
                                .bind(to_i64!(*child_ino, reply))
                                .execute(&self.pool)
                                .await,
                            reply
                        );
                    }

                    // remove all directory's associations
                    handle_db_err!(
                        query("DELETE from associated_tags WHERE ino = $1")
                            .bind(to_i64!(ino, reply))
                            .execute(&self.pool)
                            .await,
                        reply
                    );

                    let new_tags = handle_db_err!(self.get_ass_tags(newparent).await, reply);

                    // associate children with newparent's tags
                    for child_ino in &children {
                        for new_tid in &new_tags {
                            handle_db_err!(
                                query("INSERT INTO associated_tags (ino, tid) VALUES ($1, $2)")
                                    .bind(to_i64!(*child_ino, reply))
                                    .bind(to_i64!(*new_tid, reply))
                                    .execute(&self.pool)
                                    .await,
                                reply
                            );
                        }
                    }

                    // associate directory with newparent's tags
                    for new_tid in &new_tags {
                        handle_db_err!(
                            query("INSERT INTO associated_tags (ino, tid) VALUES ($1, $2)")
                                .bind(to_i64!(ino, reply))
                                .bind(to_i64!(*new_tid, reply))
                                .execute(&self.pool)
                                .await,
                            reply
                        );
                    }

                    // create new tag with the directory's new name if it doesn't yet exist
                    let new_tid = match handle_db_err!(
                        query_scalar::<_, u64>("SELECT tid FROM tags WHERE name = $1")
                            .bind(newname.to_str())
                            .fetch_optional(&self.pool)
                            .await,
                        reply
                    ) {
                        Some(tid_row) => tid_row,
                        None => {
                            handle_db_err!(
                                query_scalar::<_, u64>(
                                    "INSERT INTO tags (name) VALUES ($1) RETURNING tid"
                                )
                                .bind(newname.to_str())
                                .fetch_one(&self.pool)
                                .await,
                                reply
                            )
                        }
                    };

                    // associate children with the tag with the new directory's name
                    for child_ino in &children {
                        handle_db_err!(
                            query("INSERT INTO associated_tags (tid, ino) VALUES ($1, $2)")
                                .bind(to_i64!(new_tid, reply))
                                .bind(to_i64!(*child_ino, reply))
                                .execute(&self.pool)
                                .await,
                            reply
                        );
                    }

                    // delete entire tag if there are no other associations
                    let old_tid = handle_db_err!(
                        query_scalar::<_, u64>("SELECT tid FROM tags WHERE name = $1")
                            .bind(name.to_str())
                            .fetch_one(&self.pool)
                            .await,
                        reply
                    );
                    let associated_old_tags_count = handle_db_err!(
                        query_scalar::<_, u64>(
                            "SELECT COUNT(*) FROM associated_tags WHERE tid = $1"
                        )
                        .bind(to_i64!(old_tid, reply))
                        .fetch_one(&self.pool)
                        .await,
                        reply
                    );
                    if associated_old_tags_count == 0 {
                        handle_db_err!(
                            query("DELETE FROM tags WHERE tid = $1")
                                .bind(to_i64!(old_tid, reply))
                                .execute(&self.pool)
                                .await,
                            reply
                        );
                    }

                    // remove directory from parent
                    handle_db_err!(
                        query("DELETE FROM dir_contents WHERE cnt_ino = $1 AND dir_ino = $2")
                            .bind(to_i64!(ino, reply))
                            .bind(to_i64!(parent, reply))
                            .execute(&self.pool)
                            .await,
                        reply
                    );

                    // add directory to new parent
                    handle_db_err!(
                        query("INSERT INTO dir_contents (cnt_ino, dir_ino) VALUES ($1, $2)")
                            .bind(to_i64!(ino, reply))
                            .bind(to_i64!(newparent, reply))
                            .execute(&self.pool)
                            .await,
                        reply
                    );
                }
                FileType::RegularFile => {
                    // remove all file's associations
                    handle_db_err!(
                        query("DELETE from associated_tags WHERE ino = $1")
                            .bind(to_i64!(ino, reply))
                            .execute(&self.pool)
                            .await,
                        reply
                    );

                    // associate file with newparent's tags
                    let new_tags = handle_db_err!(self.get_ass_tags(newparent).await, reply);
                    for new_tid in new_tags {
                        handle_db_err!(
                            query("INSERT INTO associated_tags (ino, tid) VALUES ($1, $2)")
                                .bind(to_i64!(ino, reply))
                                .bind(to_i64!(new_tid, reply))
                                .execute(&self.pool)
                                .await,
                            reply
                        );
                    }
                }
                _ => {
                    tracing::error!("tfs currently only supports regular files and directories");
                    reply.error(libc::ENOSYS);
                    return;
                }
            }

            // update file_names table
            handle_db_err!(
                query("UPDATE file_names SET name = $1 WHERE ino = $2")
                    .bind(newname.to_str())
                    .bind(to_i64!(ino, reply))
                    .execute(&self.pool)
                    .await,
                reply
            );

            reply.ok();
        })
    }
}
