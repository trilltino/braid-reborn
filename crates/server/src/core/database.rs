//! The application's SQLite boundary. No database macros, network driver or TLS
//! feature is needed for these runtime parameterized queries.
pub use sqlx_core::{query::query, query_as::query_as};
pub use sqlx_sqlite::{self as sqlite, SqlitePool};

pub async fn open(path: &std::path::Path) -> anyhow::Result<SqlitePool> {
    let options = sqlite::SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(sqlite::SqliteJournalMode::Wal)
        .synchronous(sqlite::SqliteSynchronous::Full)
        .busy_timeout(std::time::Duration::from_secs(5));
    Ok(sqlite::SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?)
}
