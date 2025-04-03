mod db_types;
mod test_db;

use async_std::task;
use db_types::{
    from_filetype, from_systime, mode_to_filetype, to_filetype, FileAttrRow, ReadDirRow,
};
use fuser::*;
use libc::c_int;
use sqlx::{query, query_as, Error, Pool, QueryBuilder, Sqlite};
use std::time::{Duration, SystemTime};

pub struct TagFileSystem<'a> {
    pub pool: &'a Pool<Sqlite>,
}

impl TagFileSystem<'_> {
    async fn ins_attrs(&self, attr: &FileAttr) -> u64 {
        bind_attrs!(query_as::<_, (u64,)>( "INSERT INTO file_attrs VALUES (NULL, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) RETURNING ino"), attr)
            .fetch_one(self.pool)
            .await
            .unwrap()
            .0
    }

    async fn upd_attrs(&self, ino: u64, attr: &FileAttr) {
        bind_attrs!(query("UPDATE file_attrs SET size = ?, blocks = ?, atime = ?, mtime = ?, ctime = ?, crtime = ?, kind = ?, perm = ?, nlink = ?, uid = ?, gid = ?, rdev = ?, blksize = ?, flags = ? WHERE ino = ?"), attr)
            .bind(ino as i64)
            .execute(self.pool)
            .await
            .unwrap();
    }

    async fn get_ass_tags(&self, ino: u64) -> Vec<u64> {
        let ptags_res: Result<Vec<(u64,)>, Error> =
            query_as("SELECT tid FROM associated_tags WHERE ino = ?")
                .bind(ino as i64)
                .fetch_all(self.pool)
                .await;

        match ptags_res {
            Ok(p) => p.iter().map(|r| r.0).collect(),
            Err(e) => panic!("{e}"),
        }
    }

    async fn sync_mtime(&self, ino: u64) -> Result<(), Error> {
        match query("UPDATE file_attrs SET mtime = ? WHERE ino = ?")
            .bind(from_systime(SystemTime::now()) as i64)
            .bind(ino as i64)
            .execute(self.pool)
            .await
        {
            Err(e) => Err(e),
            Ok(_) => Ok(()),
        }
    }

    async fn sync_atime(&self, ino: u64) -> Result<(), Error> {
        match query("UPDATE file_attrs SET atime = ? WHERE ino = ?")
            .bind(from_systime(SystemTime::now()) as i64)
            .bind(ino as i64)
            .execute(self.pool)
            .await
        {
            Err(e) => Err(e),
            Ok(_) => Ok(()),
        }
    }

    fn has_perm(&self, f_uid: u32, f_gid: u32, f_perm: u16, uid: u32, gid: u32, rwx: u16) -> bool {
        if uid == 0 {
            return true;
        }

        if f_uid == uid {
            if f_perm >> 6 & rwx == rwx {
                return true;
            }

            return false;
        }

        if f_gid == gid {
            if f_perm >> 3 & rwx == rwx {
                return true;
            }

            return false;
        };

        // permission for others
        if f_perm & rwx == rwx {
            return true;
        }

        return false;
    }

    async fn has_ino_pern(&self, ino: u64, uid: u32, gid: u32, rwx: u16) -> bool {
        let p_attrs = query_as::<_, FileAttrRow>("SELECT * FROM file_attrs WHERE ino = ?")
            .bind(ino as i64)
            .fetch_one(self.pool)
            .await
            .unwrap();

        self.has_perm(p_attrs.uid, p_attrs.gid, p_attrs.perm, uid, gid, rwx)
    }
}

impl Filesystem for TagFileSystem<'_> {
    fn init(&mut self, req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), c_int> {
        task::block_on(async {
            if let None = query("SELECT 1 FROM file_attrs WHERE ino = 1")
                .fetch_optional(self.pool)
                .await
                .unwrap()
            {
                self.ins_attrs(&FileAttr {
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
                })
                .await;
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
                .fetch_one(self.pool)
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
        req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: ReplyEntry,
    ) {
        task::block_on(async {
            if !self.has_ino_pern(parent, req.uid(), req.gid(), 0b100).await {
                return reply.error(libc::EACCES);
            }

            let mut query_builder =
                QueryBuilder::<Sqlite>::new("SELECT * FROM readdir_rows WHERE (ino IN (");

            let ptags = self.get_ass_tags(parent).await;
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

            match query_builder
                .build_query_as::<ReadDirRow>()
                .fetch_optional(self.pool)
                .await
                .unwrap()
            {
                Some(r) => {
                    let attr: FileAttr = r.attr.into();
                    reply.entry(&Duration::from_secs(1), &attr, 0);
                }
                None => reply.error(libc::ENOENT),
            };
        });
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
            if !self.has_ino_pern(parent, req.uid(), req.gid(), 0b010).await {
                return reply.error(libc::EACCES);
            }

            // TODO: handle duplicates

            let kind = mode_to_filetype(mode).unwrap();

            if kind != FileType::RegularFile {
                eprintln!("tfs currently only supports regular files");
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

            f_attrs.ino = self.ins_attrs(&f_attrs).await;

            query("INSERT INTO file_names VALUES (?, ?)")
                .bind(f_attrs.ino as i64)
                .bind(name.to_str())
                .execute(self.pool)
                .await
                .unwrap();

            // associate created directory with parent tags
            for ptag in self.get_ass_tags(parent).await {
                query("INSERT INTO associated_tags VALUES (?, ?)")
                    .bind(ptag as i64)
                    .bind(f_attrs.ino as i64)
                    .execute(self.pool)
                    .await
                    .unwrap();
            }

            self.sync_mtime(parent).await.unwrap();

            reply.entry(&Duration::from_secs(1), &f_attrs, 0);
        });
    }

    fn readdir(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        task::block_on(async {
            if !self.has_ino_pern(ino, req.uid(), req.gid(), 0b100).await {
                return reply.error(libc::EACCES);
            }

            let mut query_builder =
                QueryBuilder::<Sqlite>::new("SELECT * FROM readdir_rows WHERE (ino IN (");

            let ptags = self.get_ass_tags(ino).await;
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

            match query_builder
                .build_query_as::<ReadDirRow>()
                .fetch_all(self.pool)
                .await
            {
                Ok(rows) => {
                    for row in rows.iter().enumerate() {
                        let attr = &row.1.attr;
                        let name = &row.1.name;

                        if reply.add(
                            attr.ino,
                            offset + row.0 as i64 + 1,
                            to_filetype(attr.kind).unwrap(),
                            name,
                        ) {
                            break;
                        };
                    }
                    reply.ok();
                    self.sync_atime(ino).await.unwrap();
                }
                Err(e) => panic!("{e}"),
            };
        });
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
            if !self.has_ino_pern(parent, req.uid(), req.gid(), 0b010).await {
                return reply.error(libc::EACCES);
            }

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

            f_attrs.ino = self.ins_attrs(&f_attrs).await;

            query("INSERT INTO file_names VALUES (?, ?)")
                .bind(f_attrs.ino as i64)
                .bind(name.to_str())
                .execute(self.pool)
                .await
                .unwrap();

            if let Err(e) = query("INSERT INTO dir_contents VALUES (?, ?)")
                .bind(parent as i64)
                .bind(f_attrs.ino as i64)
                .execute(self.pool)
                .await
            {
                panic!("{e}")
            };

            // create tag if doesn't exists
            let tid = match query_as::<_, (u64,)>("SELECT tid FROM tags WHERE name = ?")
                .bind(name.to_str())
                .fetch_optional(self.pool)
                .await
                .unwrap()
            {
                Some(tid_row) => tid_row.0,
                None => {
                    query_as::<_, (u64,)>("INSERT INTO tags(name) VALUES (?) RETURNING tid")
                        .bind(name.to_str())
                        .fetch_one(self.pool)
                        .await
                        .unwrap()
                        .0
                }
            };

            // associate created directory with the tid above
            query("INSERT INTO associated_tags VALUES (?, ?)")
                .bind(tid as i64)
                .bind(f_attrs.ino as i64)
                .execute(self.pool)
                .await
                .unwrap();

            // associate created directory with parent tags
            for ptag in self.get_ass_tags(parent).await {
                query("INSERT INTO associated_tags VALUES (?, ?)")
                    .bind(ptag as i64)
                    .bind(f_attrs.ino as i64)
                    .execute(self.pool)
                    .await
                    .unwrap();
            }

            self.sync_mtime(parent).await.unwrap();

            reply.entry(&Duration::from_secs(1), &f_attrs, 1);
        });
    }

    fn rmdir(&mut self, req: &Request<'_>, parent: u64, name: &std::ffi::OsStr, reply: ReplyEmpty) {
        task::block_on(async {
            if !self.has_ino_pern(parent, req.uid(), req.gid(), 0b010).await {
                return reply.error(libc::EACCES);
            }

            match  query_as::<_,(i64,)>("SELECT cnt_ino FROM dir_contents INNER JOIN file_names ON file_names.ino = dir_contents.cnt_ino WHERE dir_ino = ? AND name = ?").bind(parent as i64).bind(name.to_str().unwrap()).fetch_optional(self.pool).await.unwrap(){
                Some(r)=>{
                    if let Err(e) = query("DELETE FROM file_attrs WHERE ino = ?")
                        .bind(r.0)
                        .execute(self.pool)
                        .await
                    {
                        panic!("{e}");
                    };
                    reply.ok();

                },
                None=>reply.error(libc::ENOENT),
            };
        })
    }

    fn unlink(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: ReplyEmpty,
    ) {
        task::block_on(async {
            if !self.has_ino_pern(parent, req.uid(), req.gid(), 0b010).await {
                return reply.error(libc::EACCES);
            }

            let mut query_builder =
                QueryBuilder::<Sqlite>::new("SELECT * FROM readdir_rows WHERE (ino IN (");

            let ptags = self.get_ass_tags(parent).await;
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

            match query_builder
                .build_query_as::<FileAttrRow>()
                .fetch_optional(self.pool)
                .await
                .unwrap()
            {
                Some(r) => {
                    if !self.has_ino_pern(r.ino, req.uid(), req.gid(), 0b010).await {
                        return reply.error(libc::EACCES);
                    }

                    if let Err(e) = query("DELETE FROM file_attrs WHERE ino = ?")
                        .bind(r.ino as i64)
                        .execute(self.pool)
                        .await
                    {
                        panic!("{e}");
                    };
                    reply.ok();
                }
                None => reply.error(libc::ENOENT),
            };
        });
    }

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
            if !self.has_ino_pern(ino, req.uid(), req.gid(), 0b010).await {
                return reply.error(libc::EACCES);
            }

            let mut attr: FileAttr =
                match query_as::<_, FileAttrRow>("SELECT * FROM file_attrs WHERE ino = $1")
                    .bind(ino as i64)
                    .fetch_optional(self.pool)
                    .await
                    .unwrap()
                {
                    Some(row) => row.into(),
                    None => return reply.error(libc::ENOENT),
                };

            attr.size = size.unwrap_or(attr.size);
            attr.atime = atime.map_or(attr.atime, |tn| match tn {
                TimeOrNow::Now => SystemTime::now(),
                TimeOrNow::SpecificTime(t) => t,
            });
            attr.mtime = mtime.map_or(attr.mtime, |tn| match tn {
                TimeOrNow::Now => SystemTime::now(),
                TimeOrNow::SpecificTime(t) => t,
            });
            attr.ctime = ctime.unwrap_or(attr.ctime);
            attr.crtime = crtime.unwrap_or(attr.crtime);
            attr.flags = flags.unwrap_or(attr.flags);
            // TODO: handle change mode filetype case?
            attr.perm = mode.map_or(attr.perm, |m| m as u16);
            attr.uid = uid.unwrap_or(attr.uid);
            attr.gid = gid.unwrap_or(attr.gid);

            self.upd_attrs(attr.ino, &attr).await;

            reply.attr(&Duration::from_secs(1), &attr);
        })
    }

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
            if !self.has_ino_pern(ino, req.uid(), req.gid(), 0b010).await {
                return reply.error(libc::EACCES);
            }

            let cnt_len =
                query_as::<_, (i64,)>("SELECT LENGTH(content) FROM file_contents WHERE ino = $1")
                    .bind(ino as i64)
                    .fetch_optional(self.pool)
                    .await
                    .unwrap();

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
            query("INSERT INTO file_contents VALUES ($4, CAST(ZEROBLOB($5) || $2 AS BLOB)) ON CONFLICT(ino) DO UPDATE SET content = CAST(SUBSTR(content, 1, $1) || ZEROBLOB($5) || $2 || SUBSTR(content, $3) AS BLOB) WHERE ino = $4")
                .bind(offset)
                .bind(data)
                .bind(data.len() as i64 + 1 + offset)
                .bind(ino as i64)
                .bind(pad_len.unwrap_or(0))
                .execute(self.pool)
                .await.unwrap();

            query("UPDATE file_attrs SET size = (SELECT LENGTH(content) FROM file_contents WHERE ino = $1) WHERE ino = $1")
                .bind(ino as i64)
                .execute(self.pool)
                .await
                .unwrap();

            reply.written(data.len().try_into().unwrap());
        });
    }

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
            if !self.has_ino_pern(ino, req.uid(), req.gid(), 0b100).await {
                return reply.error(libc::EACCES);
            }

            let data = query_as::<_, (Box<[u8]>,)>(
                "SELECT SUBSTR(content, $1, $2) FROM file_contents WHERE ino = $3",
            )
            .bind(offset)
            .bind(size)
            .bind(ino as i64)
            .fetch_one(self.pool)
            .await
            .unwrap()
            .0;

            reply.data(Box::leak(data));
        });
    }
}
