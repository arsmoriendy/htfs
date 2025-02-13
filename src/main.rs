use async_std::task;
use libc::c_int;

use fuser::*;
use sqlx::{Pool, Sqlite};

struct TagFileSystem {
    pool: Box<Pool<Sqlite>>,
}

impl Filesystem for TagFileSystem {
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), c_int> {
        return Ok(());
    }

    fn destroy(&mut self) {
        task::block_on(self.pool.close());
    }
}

fn main() {}

#[cfg(test)]
mod test {
    use sqlx::SqlitePool;

    use super::*;

    struct Setup<'a> {
        monut_path: &'a str,
    }

    impl Default for Setup<'_> {
        fn default() -> Self {
            let ret = Setup {
                monut_path: "mountpoint",
            };
            if let Err(e) = std::fs::create_dir(ret.monut_path) {
                panic!("{e}");
            }
            return ret;
        }
    }

    impl Drop for Setup<'_> {
        fn drop(&mut self) {
            if let Err(e) = std::fs::remove_dir_all(self.monut_path) {
                panic!("{e}");
            }
        }
    }

    #[test]
    fn mount_unmount() {
        let stp = Setup::default();

        let pool = Box::new(SqlitePool::connect_lazy("sqlite::memory").unwrap());

        let sess = spawn_mount2(TagFileSystem { pool }, stp.monut_path, &[]).unwrap();
        sess.join();
    }
}
