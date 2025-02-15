#[cfg(test)]
mod test {
    use sqlx::SqlitePool;

    use crate::*;

    struct Setup<'a> {
        monut_path: &'a str,
    }

    impl Setup<'_> {
        fn init(&self) {
            if let Err(e) = std::fs::create_dir(self.monut_path) {
                panic!("{e}");
            }
        }
    }

    impl Default for Setup<'_> {
        fn default() -> Self {
            let ret = Setup {
                monut_path: "mountpoint",
            };
            ret.init();
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

        let pool = Box::new(SqlitePool::connect_lazy("sqlite::memory:").unwrap());

        let sess = spawn_mount2(TagFileSystem { pool }, stp.monut_path, &[]).unwrap();
        sess.join();
    }

    #[ignore]
    #[test]
    fn mount_interactive() {
        let stp = Setup::default();

        let pool = Box::new(SqlitePool::connect_lazy("sqlite::memory:").unwrap());

        mount2(TagFileSystem { pool }, stp.monut_path, &[]).unwrap();
    }
}
