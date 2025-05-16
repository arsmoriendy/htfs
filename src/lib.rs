mod db_helpers;
mod fs;
mod test_db;
use db_helpers::types::DBError;
use libc::c_int;
use sqlx::{Pool, Sqlite};
use std::num::TryFromIntError;

#[derive(Debug)]
pub struct TagFileSystem<'a> {
    pub pool: &'a Pool<Sqlite>,
}

impl TagFileSystem<'_> {
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
}

pub fn handle_from_int_err<T>(expr: Result<T, TryFromIntError>) -> Result<T, c_int> {
    expr.map_err(|e| {
        tracing::error!("{e}");
        libc::ERANGE
    })
}

#[macro_export]
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

#[macro_export]
macro_rules! to_i64 {
    ($e: expr, $reply: expr) => {
        handle_from_int_err!(i64::try_from($e), $reply)
    };
}

pub fn handle_db_err<T, E>(expr: Result<T, E>) -> Result<T, c_int>
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

#[macro_export]
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

#[macro_export]
macro_rules! handle_auth_perm {
    ($self: expr, $ino: expr, $req: expr, $reply: expr, $rwx: expr) => {
        let has_perm = handle_db_err!($self.req_has_ino_perm($ino, $req, $rwx).await, $reply);
        if !has_perm {
            $reply.error(libc::EACCES);
            return;
        }
    };
}
