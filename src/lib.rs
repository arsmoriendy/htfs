#[macro_use]
mod macros;
mod db_helpers;
mod fs;
mod test_db;
use db_helpers::{
    try_bind_attrs,
    types::{Bindable, DBError, FileAttrRow, from_systime},
};
use fuser::FileAttr;
use libc::c_int;
use sqlx::{Database, Pool, SqlitePool, query, query_as, query_scalar};
use std::{num::TryFromIntError, time::SystemTime};
use tokio::runtime::Handle;

#[derive(Debug)]
pub struct HTFS<DB: Database> {
    pub pool: &'static Pool<DB>,
    pub runtime_handle: Handle,
    pub tag_prefix: String,
}

pub fn is_prefixed(prefix: &str, filename: &str) -> bool {
    let prefix_position = filename.as_bytes().get(0..prefix.len());
    match prefix_position {
        Some(oss) => oss == prefix.as_bytes(),
        None => false,
    }
}

pub async fn ins_attrs(pool: &SqlitePool, attr: &FileAttr) -> Result<u64, DBError> {
    let q = query_scalar::<_, u64>(
        "INSERT INTO file_attrs VALUES (NULL, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, \
         $14, $15) RETURNING ino",
    );
    Ok(try_bind_attrs(q, attr)?.inner().fetch_one(pool).await?)
}

pub async fn upd_attrs(pool: &SqlitePool, attr: &FileAttr) -> Result<(), DBError> {
    let q = query(
        "UPDATE file_attrs SET size = $2, blocks = $3, atime = $4, mtime = $5, ctime = $6, crtime \
         = $7, kind = $8, perm = $9, nlink = $10, uid = $11, gid = $12, rdev = $13, blksize = \
         $14, flags = $15 WHERE ino = $1",
    );
    try_bind_attrs(q, attr)?.execute(pool).await?;
    Ok(())
}

pub async fn get_ass_tags(pool: &SqlitePool, ino: u64) -> Result<Vec<u64>, DBError> {
    Ok(
        query_scalar::<_, u64>("SELECT tid FROM associated_tags WHERE ino = ?")
            .bind(i64::try_from(ino)?)
            .fetch_all(pool)
            .await?,
    )
}

pub async fn sync_mtime(pool: &SqlitePool, ino: u64) -> Result<(), DBError> {
    query("UPDATE file_attrs SET mtime = ? WHERE ino = ?")
        .bind(i64::try_from(from_systime(SystemTime::now())?)?)
        .bind(i64::try_from(ino)?)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn sync_atime(pool: &SqlitePool, ino: u64) -> Result<(), DBError> {
    query("UPDATE file_attrs SET atime = ? WHERE ino = ?")
        .bind(i64::try_from(from_systime(SystemTime::now())?)?)
        .bind(i64::try_from(ino)?)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn has_ino_perm(
    pool: &SqlitePool,
    ino: u64,
    uid: u32,
    gid: u32,
    rwx: u16,
) -> Result<bool, DBError> {
    let p_attrs = query_as::<_, FileAttrRow>("SELECT * FROM file_attrs WHERE ino = ?")
        .bind(i64::try_from(ino)?)
        .fetch_one(pool)
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

pub async fn get_ino_name(pool: &SqlitePool, ino: i64) -> Result<String, DBError> {
    query_scalar("SELECT name FROM file_names WHERE ino = ?")
        .bind(ino)
        .fetch_one(pool)
        .await
        .map_err(|e| DBError::from(e))
}

pub async fn get_db_page_size(pool: &SqlitePool) -> Result<u64, DBError> {
    query_scalar("PRAGMA page_size")
        .fetch_one(pool)
        .await
        .map_err(|e| DBError::from(e))
}

pub async fn change_file_size(pool: &SqlitePool, ino: u64, new_size: u64) -> Result<(), DBError> {
    let ino: i64 = ino.try_into()?;
    let new_size: i64 = new_size.try_into()?;
    let page_size: i64 = get_db_page_size(pool).await?.try_into()?;
    let usize_page_size = page_size.try_into()?;
    let new_last_page = new_size / page_size;
    let new_last_page_size = new_size % page_size;

    let create_new_last_page = || {
        query("INSERT INTO file_contents (ino, page, bytes) VALUES (?,?,ZEROBLOB(?))")
            .bind(ino)
            .bind(new_last_page)
            .bind(new_last_page_size)
            .execute(pool)
    };
    let truncate_new_last_page = || {
        query(
            "UPDATE file_contents SET bytes = CAST(SUBSTR(bytes, 1, ?) AS BLOB) WHERE ino = ? and \
             page = ?",
        )
        .bind(new_last_page_size)
        .bind(ino)
        .bind(new_last_page)
        .execute(pool)
    };

    // check file has content
    let old_last_page_query: Option<(i64, Vec<u8>)> =
        query_as("SELECT page, bytes FROM file_contents WHERE ino = ? ORDER BY page DESC LIMIT 1")
            .bind(ino)
            .fetch_optional(pool)
            .await?;
    if let Some((old_last_page, mut old_last_page_bytes)) = old_last_page_query {
        if new_last_page > old_last_page {
            // rpad old last page
            old_last_page_bytes.resize(usize_page_size, 0);
            query("UPDATE file_contents SET bytes = ? WHERE ino = ? and page = ?")
                .bind(old_last_page_bytes)
                .bind(ino)
                .bind(old_last_page)
                .execute(pool)
                .await?;

            create_new_last_page().await?;
        } else if new_last_page < old_last_page {
            // delete pages > new_last_page
            query("DELETE FROM file_contents WHERE ino = ? AND page > ?")
                .bind(ino)
                .bind(new_last_page)
                .execute(pool)
                .await?;
            // check if new_last_page exists
            match query("SELECT 1 FROM file_contents WHERE ino = ? AND page = ?")
                .bind(ino)
                .bind(new_last_page)
                .fetch_optional(pool)
                .await?
            {
                Some(_) => truncate_new_last_page().await?,
                None => create_new_last_page().await?,
            };
        } else if new_last_page == old_last_page {
            truncate_new_last_page().await?;
        };
    } else {
        create_new_last_page().await?;
    };
    Ok(())
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
        tracing::debug!("{e}");
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
        tracing::debug!("{s}");
        code
    })
}
