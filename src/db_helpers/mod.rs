pub mod types;

use crate::{bind_attrs, TagFileSystem};
use fuser::{FileAttr, Request};
use sqlx::{query, query_as};
use std::time::SystemTime;
use types::{from_filetype, from_systime, FileAttrRow};

#[macro_export]
macro_rules! bind_attrs {
    ($q: expr, $a: expr) => {
        $q.bind($a.size as i64) // size INTEGER,
            .bind($a.blocks as i64) // blocks INTEGER,
            .bind(from_systime($a.atime) as i64) // atime INTEGER,
            .bind(from_systime($a.mtime) as i64) // mtime INTEGER,
            .bind(from_systime($a.ctime) as i64) // ctime INTEGER,
            .bind(from_systime($a.crtime) as i64) // crtime INTEGER,
            .bind(from_filetype($a.kind)) // kind INTEGER,
            .bind($a.perm) // perm INTEGER,
            .bind($a.nlink) // nlink INTEGER,
            .bind($a.uid) // uid INTEGER,
            .bind($a.gid) // gid INTEGER,
            .bind($a.rdev) // rdev INTEGER,
            .bind($a.blksize) // blksize INTEGER,
            .bind($a.flags) // flags INTEGER,
    };
}

#[macro_export]
macro_rules! bind_attrs_ino {
    ($q: expr, $a: expr) => {
        bind_attrs!($q.bind($a.ino as i64), $a)
    };
}

impl TagFileSystem<'_> {
    pub async fn ins_attrs(&self, attr: &FileAttr) -> Result<u64, sqlx::Error> {
        Ok(bind_attrs!(query_as::<_, (u64,)>( "INSERT INTO file_attrs VALUES (NULL, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) RETURNING ino"), attr)
            .fetch_one(self.pool)
            .await?
            .0)
    }

    pub async fn upd_attrs(&self, ino: u64, attr: &FileAttr) -> Result<(), sqlx::Error> {
        bind_attrs!(query("UPDATE file_attrs SET size = ?, blocks = ?, atime = ?, mtime = ?, ctime = ?, crtime = ?, kind = ?, perm = ?, nlink = ?, uid = ?, gid = ?, rdev = ?, blksize = ?, flags = ? WHERE ino = ?"), attr)
            .bind(ino as i64)
            .execute(self.pool)
            .await?;
        Ok(())
    }

    pub async fn get_ass_tags(&self, ino: u64) -> Result<Vec<u64>, sqlx::Error> {
        Ok(
            query_as::<_, (u64,)>("SELECT tid FROM associated_tags WHERE ino = ?")
                .bind(ino as i64)
                .fetch_all(self.pool)
                .await?
                .iter()
                .map(|r| r.0)
                .collect::<Vec<_>>(),
        )
    }

    pub async fn sync_mtime(&self, ino: u64) -> Result<(), sqlx::Error> {
        query("UPDATE file_attrs SET mtime = ? WHERE ino = ?")
            .bind(from_systime(SystemTime::now()) as i64)
            .bind(ino as i64)
            .execute(self.pool)
            .await?;
        Ok(())
    }

    pub async fn sync_atime(&self, ino: u64) -> Result<(), sqlx::Error> {
        query("UPDATE file_attrs SET atime = ? WHERE ino = ?")
            .bind(from_systime(SystemTime::now()) as i64)
            .bind(ino as i64)
            .execute(self.pool)
            .await?;
        Ok(())
    }

    async fn has_ino_perm(
        &self,
        ino: u64,
        uid: u32,
        gid: u32,
        rwx: u16,
    ) -> Result<bool, sqlx::Error> {
        let p_attrs = query_as::<_, FileAttrRow>("SELECT * FROM file_attrs WHERE ino = ?")
            .bind(ino as i64)
            .fetch_one(self.pool)
            .await?;

        Ok(TagFileSystem::has_perm(
            p_attrs.uid,
            p_attrs.gid,
            p_attrs.perm,
            uid,
            gid,
            rwx,
        ))
    }

    pub async fn req_has_ino_perm(
        &self,
        ino: u64,
        req: &Request<'_>,
        rwx: u16,
    ) -> Result<bool, sqlx::Error> {
        Ok(self.has_ino_perm(ino, req.uid(), req.gid(), rwx).await?)
    }
}
