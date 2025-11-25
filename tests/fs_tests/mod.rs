#[macro_use]
mod prelude;
reg_method!(rename);
reg_method!(write);
reg_method!(setattr);

pub fn test_fs() {
    test_rename();
    test_write();
    test_setattr();
}
