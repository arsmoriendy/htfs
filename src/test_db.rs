#[cfg(test)]
mod test {
    use sqlx::{migrate, SqlitePool};

    use crate::*;

    #[test]
    fn migrate() {
        let pool = Box::new(SqlitePool::connect_lazy("sqlite::memory:").unwrap());

        task::block_on(migrate!().run(pool.as_ref())).unwrap();

        task::block_on(pool.close());
    }
}
