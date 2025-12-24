use sqlx::Postgres;
use sqlx::query_builder::Separated;

pub trait BatchInsertRow {
    fn table(&self, exchange: &str) -> String;
    const COLUMNS: &'static [&'static str];

    fn push_binds(&self, b: &mut Separated<'_, '_, Postgres, &'static str>);
}

