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

macro_rules! handle_auth_perm {
    ($pool: expr, $ino: expr, $uid: expr, $gid: expr, $reply: expr, $rwx: expr) => {
        let has_perm = handle_db_err!(has_ino_perm($pool, $ino, $uid, $gid, $rwx).await, $reply);
        if !has_perm {
            $reply.error(libc::EACCES);
            return;
        }
    };
}

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

macro_rules! to_i64 {
    ($e: expr, $reply: expr) => {
        handle_from_int_err!(i64::try_from($e), $reply)
    };
}
