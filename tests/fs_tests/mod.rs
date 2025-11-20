#[macro_use]
mod prelude;
reg_method!(rename);
reg_method!(write);

pub fn test_fs() {
    test_rename();
    test_write();
}
