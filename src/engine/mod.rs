pub mod mysql;

use anyhow::Result;
use async_trait::async_trait;
use mysql_common::value::Value;

/// Stream of rows from a database query
pub type RowStream = std::pin::Pin<Box<dyn futures::Stream<Item = Result<Vec<Value>>> + Send>>;

/// Database engine trait for provider abstraction
#[async_trait]
pub trait DbEngine: Send + Sync {
    /// Connect to a database using the provider's URL format
    async fn connect(&self, url: &str) -> Result<Box<dyn DbSession>>;
}

/// Active database session for executing queries
#[async_trait]
pub trait DbSession: Send {
    /// Start a consistent snapshot transaction (REPEATABLE READ)
    async fn start_consistent_snapshot(&mut self) -> Result<()>;

    /// List all tables matching include/exclude filters
    /// Empty include = all tables; exclude list is applied after
    async fn list_tables(&mut self, include: &[String], exclude: &[String]) -> Result<Vec<String>>;

    /// Get CREATE TABLE statement for a table (minified to single line)
    async fn show_create_table(&mut self, table: &str) -> Result<String>;

    /// Stream all rows from a table
    /// Returns rows as Vec<Value> in column order
    async fn stream_rows(&mut self, table: &str) -> Result<(Vec<String>, RowStream)>;

    /// Get approximate row count for a table (for progress indication)
    async fn approximate_row_count(&mut self, table: &str) -> Result<u64>;

    /// Insert a batch of rows into a table
    async fn insert_batch(
        &mut self,
        table: &str,
        column_names: &[String],
        rows: &[Vec<Value>],
    ) -> Result<()>;

    /// Disable foreign key checks
    async fn disable_constraints(&mut self) -> Result<()>;

    /// Enable foreign key checks
    async fn enable_constraints(&mut self) -> Result<()>;

    /// Execute a raw SQL statement
    async fn execute(&mut self, sql: &str) -> Result<()>;

    /// Commit current transaction
    async fn commit(&mut self) -> Result<()>;
}

/// Factory for creating database engines
pub fn create_engine(provider: &str) -> Result<Box<dyn DbEngine>> {
    match provider.to_lowercase().as_str() {
        "mysql" => Ok(Box::new(mysql::MysqlEngine)),
        _ => Err(anyhow::anyhow!("Unsupported database provider: {}", provider)),
    }
}
