mod db_helpers;
mod fs;
mod test_db;

use sqlx::{Pool, Sqlite};

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
