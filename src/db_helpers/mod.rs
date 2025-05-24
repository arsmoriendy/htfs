pub mod types;

use fuser::FileAttr;
use sqlx::{QueryBuilder, Sqlite};
use types::{from_filetype, from_systime, Bindable, ConvError, DBError};

pub fn try_bind_attrs<'q, Q, B>(b: B, a: &FileAttr) -> Result<Q, ConvError>
where
    B: Bindable<'q, Sqlite, Q>,
{
    Ok(
        b.gbind(i64::try_from(a.ino)?) // ino INTEGER
            .gbind(i64::try_from(a.size)?) // size INTEGER,
            .gbind(i64::try_from(a.blocks)?) // blocks INTEGER,
            .gbind(i64::try_from(from_systime(a.atime)?)?) // atime INTEGER,
            .gbind(i64::try_from(from_systime(a.mtime)?)?) // mtime INTEGER,
            .gbind(i64::try_from(from_systime(a.ctime)?)?) // ctime INTEGER,
            .gbind(i64::try_from(from_systime(a.crtime)?)?) // crtime INTEGER,
            .gbind(from_filetype(a.kind)) // kind INTEGER,
            .gbind(a.perm) // perm INTEGER,
            .gbind(a.nlink) // nlink INTEGER,
            .gbind(a.uid) // uid INTEGER,
            .gbind(a.gid) // gid INTEGER,
            .gbind(a.rdev) // rdev INTEGER,
            .gbind(a.blksize) // blksize INTEGER,
            .gbind(a.flags)
            .inner(), // flags INTEGER,
    )
}

/// Chain `qb` with a `SELECT` query of inodes that has been tagged with all of `tags`
pub fn chain_tagged_inos(qb: &mut QueryBuilder<Sqlite>, tags: &Vec<u64>) -> Result<(), DBError> {
    for (i, t) in tags.iter().enumerate() {
        qb.push("SELECT ino FROM associated_tags WHERE tid = ")
            .push_bind(i64::try_from(*t)?);
        if i != tags.len() - 1 {
            qb.push(" AND ino IN (");
        }
    }
    for _ in tags.iter().skip(1) {
        qb.push(")");
    }

    Ok(())
}
