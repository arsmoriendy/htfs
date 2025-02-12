use libc::c_int;

use fuser::*;

struct TagFileSystem;

impl Filesystem for TagFileSystem {
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), c_int> {
        return Ok(());
    }
}

fn main() {
    TagFileSystem {};
}

#[cfg(test)]
mod test {
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

        let sess = spawn_mount2(TagFileSystem {}, stp.monut_path, &[]).unwrap();
        sess.join();
    }
}
