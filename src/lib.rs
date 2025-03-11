mod db_types;
mod test_db;
mod test_fs;

use async_std::task;
use db_types::{
    from_filetype, from_systime, mode_to_filetype, to_filetype, FileAttrRow, ReadDirRow,
};
use fuser::*;
use libc::c_int;
use sqlx::{query, query_as, Error, Pool, QueryBuilder, Sqlite};
use std::time::{Duration, SystemTime};

struct TagFileSystem<'a> {
    pool: &'a Pool<Sqlite>,
}

impl TagFileSystem<'_> {
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
}

impl Filesystem for TagFileSystem<'_> {
    fn init(&mut self, req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), c_int> {
        task::block_on(async {
            if let None = query("SELECT 1 FROM file_attrs WHERE ino = 1")
                .fetch_optional(self.pool)
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
                .execute(self.pool)
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
        _req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: ReplyEntry,
    ) {
        task::block_on(async {
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
                .fetch_one(self.pool)
                .await
                .unwrap()
                .0;

            query("INSERT INTO file_names VALUES (?, ?)")
                .bind(ino as i64)
                .bind(name.to_str())
                .execute(self.pool)
                .await
                .unwrap();

            // associate created directory with parent tags
            for ptag in self.get_ass_tags(parent).await {
                query("INSERT INTO associated_tags VALUES (?, ?)")
                    .bind(ptag as i64)
                    .bind(ino as i64)
                    .execute(self.pool)
                    .await
                    .unwrap();
            }

            reply.entry(&Duration::from_secs(1), &f_attrs, 0);
        });
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        task::block_on(async {
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
                    // println!("{ino}\t{:?}", rows);
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
                .fetch_one(self.pool)
                .await
                .unwrap()
                .0;

            query("INSERT INTO file_names VALUES (?, ?)")
                .bind(ino as i64)
                .bind(name.to_str())
                .execute(self.pool)
                .await
                .unwrap();

            if let Err(e) = query("INSERT INTO dir_contents VALUES (?, ?)")
                .bind(parent as i64)
                .bind(ino as i64)
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
                .bind(ino as i64)
                .execute(self.pool)
                .await
                .unwrap();

            // associate created directory with parent tags
            for ptag in self.get_ass_tags(parent).await {
                query("INSERT INTO associated_tags VALUES (?, ?)")
                    .bind(ptag as i64)
                    .bind(ino as i64)
                    .execute(self.pool)
                    .await
                    .unwrap();
            }

            reply.entry(&Duration::from_secs(1), &f_attrs, 1);
        });
    }

    fn rmdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: ReplyEmpty,
    ) {
        task::block_on(async {
            match  query_as::<_,(i64,)>("SELECT cnt_ino FROM dir_contents INNER JOIN file_names ON file_names.ino = dir_contents.cnt_ino WHERE dir_ino = ? AND name = ?").bind(parent as i64).bind(name.to_str().unwrap()).fetch_optional(self.pool).await.unwrap(){
                Some(r)=>{
                    if let Err(e) = query("DELETE FROM file_attrs WHERE ino = ?")
                        .bind(r.0)
                        .execute(self.pool)
                        .await
                    {
                        panic!("{e}");
                    };

                },
                None=>reply.error(libc::ENOENT),
            };
        })
    }

    fn unlink(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: ReplyEmpty,
    ) {
        task::block_on(async {
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
                    if let Err(e) = query("DELETE FROM file_attrs WHERE ino = ?")
                        .bind(r.ino as i64)
                        .execute(self.pool)
                        .await
                    {
                        panic!("{e}");
                    };
                }
                None => reply.error(libc::ENOENT),
            };
        });
    }
}
