mod db_types;
mod fs;
mod test_db;

use db_types::{from_filetype, from_systime, FileAttrRow};
use fuser::*;
use sqlx::{query, query_as, Pool, Sqlite};
use std::time::SystemTime;

#[derive(Debug)]
pub struct TagFileSystem<'a> {
    pub pool: &'a Pool<Sqlite>,
}

impl TagFileSystem<'_> {
    async fn ins_attrs(&self, attr: &FileAttr) -> Result<u64, sqlx::Error> {
        Ok(bind_attrs!(query_as::<_, (u64,)>( "INSERT INTO file_attrs VALUES (NULL, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) RETURNING ino"), attr)
            .fetch_one(self.pool)
            .await?
            .0)
    }

    async fn upd_attrs(&self, ino: u64, attr: &FileAttr) -> Result<(), sqlx::Error> {
        bind_attrs!(query("UPDATE file_attrs SET size = ?, blocks = ?, atime = ?, mtime = ?, ctime = ?, crtime = ?, kind = ?, perm = ?, nlink = ?, uid = ?, gid = ?, rdev = ?, blksize = ?, flags = ? WHERE ino = ?"), attr)
            .bind(ino as i64)
            .execute(self.pool)
            .await?;
        Ok(())
    }

    async fn get_ass_tags(&self, ino: u64) -> Result<Vec<u64>, sqlx::Error> {
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

    async fn sync_mtime(&self, ino: u64) -> Result<(), sqlx::Error> {
        query("UPDATE file_attrs SET mtime = ? WHERE ino = ?")
            .bind(from_systime(SystemTime::now()) as i64)
            .bind(ino as i64)
            .execute(self.pool)
            .await?;
        Ok(())
    }

    async fn sync_atime(&self, ino: u64) -> Result<(), sqlx::Error> {
        query("UPDATE file_attrs SET atime = ? WHERE ino = ?")
            .bind(from_systime(SystemTime::now()) as i64)
            .bind(ino as i64)
            .execute(self.pool)
            .await?;
        Ok(())
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

        Ok(self.has_perm(p_attrs.uid, p_attrs.gid, p_attrs.perm, uid, gid, rwx))
    }

    async fn req_has_ino_perm(
        &self,
        ino: u64,
        req: &Request<'_>,
        rwx: u16,
    ) -> Result<bool, sqlx::Error> {
        Ok(self.has_ino_perm(ino, req.uid(), req.gid(), rwx).await?)
    }
}
