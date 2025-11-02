#[macro_use]
mod macros;
mod db_helpers;
mod fs;
mod test_db;
use db_helpers::{
    try_bind_attrs,
    types::{Bindable, DBError, FileAttrRow, from_systime},
};
use fuser::{FileAttr, Request};
use libc::c_int;
use sqlx::{Database, Pool, Sqlite, query, query_as, query_scalar};
use std::{num::TryFromIntError, time::SystemTime};
use tokio::runtime::Runtime;

#[derive(Debug)]
pub struct TagFileSystem<DB: Database> {
    pub pool: Pool<DB>,
    pub rt: Runtime,
    pub tag_prefix: String,
}

impl TagFileSystem<Sqlite> {
    async fn ins_attrs(&self, attr: &FileAttr) -> Result<u64, DBError> {
        let q = query_scalar::<_, u64>(
            "INSERT INTO file_attrs VALUES (NULL, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, \
             $13, $14, $15) RETURNING ino",
        );
        Ok(try_bind_attrs(q, attr)?
            .inner()
            .fetch_one(&self.pool)
            .await?)
    }

    async fn upd_attrs(&self, attr: &FileAttr) -> Result<(), DBError> {
        let q = query(
            "UPDATE file_attrs SET size = $2, blocks = $3, atime = $4, mtime = $5, ctime = $6, \
             crtime = $7, kind = $8, perm = $9, nlink = $10, uid = $11, gid = $12, rdev = $13, \
             blksize = $14, flags = $15 WHERE ino = $1",
        );
        try_bind_attrs(q, attr)?.execute(&self.pool).await?;
        Ok(())
    }

    async fn get_ass_tags(&self, ino: u64) -> Result<Vec<u64>, DBError> {
        Ok(
            query_scalar::<_, u64>("SELECT tid FROM associated_tags WHERE ino = ?")
                .bind(i64::try_from(ino)?)
                .fetch_all(&self.pool)
                .await?,
        )
    }

    async fn sync_mtime(&self, ino: u64) -> Result<(), DBError> {
        query("UPDATE file_attrs SET mtime = ? WHERE ino = ?")
            .bind(i64::try_from(from_systime(SystemTime::now())?)?)
            .bind(i64::try_from(ino)?)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn sync_atime(&self, ino: u64) -> Result<(), DBError> {
        query("UPDATE file_attrs SET atime = ? WHERE ino = ?")
            .bind(i64::try_from(from_systime(SystemTime::now())?)?)
            .bind(i64::try_from(ino)?)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn req_has_ino_perm(
        &self,
        ino: u64,
        req: &Request<'_>,
        rwx: u16,
    ) -> Result<bool, DBError> {
        Ok(self.has_ino_perm(ino, req.uid(), req.gid(), rwx).await?)
    }

    async fn has_ino_perm(&self, ino: u64, uid: u32, gid: u32, rwx: u16) -> Result<bool, DBError> {
        let p_attrs = query_as::<_, FileAttrRow>("SELECT * FROM file_attrs WHERE ino = ?")
            .bind(i64::try_from(ino)?)
            .fetch_one(&self.pool)
            .await?;

        Ok(has_perm(
            p_attrs.uid,
            p_attrs.gid,
            p_attrs.perm,
            uid,
            gid,
            rwx,
        ))
    }

    fn is_prefixed(&self, filename: &str) -> bool {
        let prefix_position = filename.as_bytes().get(0..self.tag_prefix.len());
        match prefix_position {
            Some(oss) => oss == self.tag_prefix.as_bytes(),
            None => false,
        }
    }
}

fn has_perm(f_uid: u32, f_gid: u32, f_perm: u16, uid: u32, gid: u32, rwx: u16) -> bool {
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

fn handle_from_int_err<T>(expr: Result<T, TryFromIntError>) -> Result<T, c_int> {
    expr.map_err(|e| {
        tracing::error!("{e}");
        libc::ERANGE
    })
}

fn handle_db_err<T, E>(expr: Result<T, E>) -> Result<T, c_int>
where
    E: Into<DBError>,
{
    expr.map_err(|e| {
        let db_err: DBError = e.into();
        let (code, s) = db_err.map_db_err();
        tracing::error!("{s}");
        code
    })
}
