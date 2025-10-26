use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "migrasquiel")]
#[command(
    about = "Database migration tool for MySQL, PostgreSQL, and SQL Server",
    long_about = None
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Dump database to SQL file
    Dump {
        /// Source database URL (mysql://, postgres://, or mssql://)
        #[arg(short, long)]
        source: Option<String>,

        /// Environment variable containing source URL
        #[arg(long)]
        source_env: Option<String>,

        /// Output file path (.sql or .sql.gz)
        #[arg(short, long)]
        output: String,

        /// Database provider (mysql|postgres|sqlserver)
        #[arg(long, default_value = "mysql", value_parser = ["mysql", "postgres", "sqlserver"])]
        provider: String,

        /// Tables to include (comma-separated)
        #[arg(long, value_delimiter = ',')]
        tables: Vec<String>,

        /// Tables to exclude (comma-separated)
        #[arg(long, value_delimiter = ',')]
        exclude: Vec<String>,

        /// Dump schema only (no data)
        #[arg(long)]
        schema_only: bool,

        /// Dump data only (no schema)
        #[arg(long)]
        data_only: bool,

        /// Rows per INSERT batch
        #[arg(long, default_value = "1000")]
        batch_rows: usize,

        /// Use consistent snapshot (REPEATABLE READ transaction)
        #[arg(long)]
        consistent_snapshot: bool,

        /// Compress output with gzip
        #[arg(long)]
        gzip: bool,
    },

    /// Restore database from SQL file
    Restore {
        /// Destination database URL (mysql://, postgres://, or mssql://)
        #[arg(short, long)]
        destination: Option<String>,

        /// Environment variable containing destination URL
        #[arg(long)]
        destination_env: Option<String>,

        /// Input file path (.sql or .sql.gz)
        #[arg(short, long)]
        input: String,

        /// Database provider (mysql|postgres|sqlserver)
        #[arg(long, default_value = "mysql", value_parser = ["mysql", "postgres", "sqlserver"])]
        provider: String,

        /// Disable foreign key checks during restore
        #[arg(long, default_value = "true")]
        disable_fk_checks: bool,
    },

    /// Migrate database directly from source to destination
    Migrate {
        /// Source database URL (mysql://, postgres://, or mssql://)
        #[arg(short, long)]
        source: Option<String>,

        /// Environment variable containing source URL
        #[arg(long)]
        source_env: Option<String>,

        /// Destination database URL (mysql://, postgres://, or mssql://)
        #[arg(short, long)]
        destination: Option<String>,

        /// Environment variable containing destination URL
        #[arg(long)]
        destination_env: Option<String>,

        /// Database provider (mysql|postgres|sqlserver)
        #[arg(long, default_value = "mysql", value_parser = ["mysql", "postgres", "sqlserver"])]
        provider: String,

        /// Tables to include (comma-separated)
        #[arg(long, value_delimiter = ',')]
        tables: Vec<String>,

        /// Tables to exclude (comma-separated)
        #[arg(long, value_delimiter = ',')]
        exclude: Vec<String>,

        /// Migrate schema only (no data)
        #[arg(long)]
        schema_only: bool,

        /// Migrate data only (no schema)
        #[arg(long)]
        data_only: bool,

        /// Rows per INSERT batch
        #[arg(long, default_value = "1000")]
        batch_rows: usize,

        /// Use consistent snapshot (REPEATABLE READ transaction)
        #[arg(long)]
        consistent_snapshot: bool,

        /// Disable foreign key checks during migration
        #[arg(long, default_value = "true")]
        disable_fk_checks: bool,
    },
}

impl Commands {
    /// Get database URL from either direct argument or environment variable
    pub fn get_url(direct: &Option<String>, env_var: &Option<String>, url_type: &str) -> anyhow::Result<String> {
        if let Some(url) = direct {
            Ok(url.clone())
        } else if let Some(env) = env_var {
            std::env::var(env)
                .map_err(|_| anyhow::anyhow!("Environment variable {} not found", env))
        } else {
            Err(anyhow::anyhow!("Either --{} or --{}-env must be provided", url_type, url_type))
        }
    }

    /// Redact password from URL for logging
    pub fn redact_url(url: &str) -> String {
        if let Some(at_pos) = url.find('@') {
            if let Some(colon_pos) = url[..at_pos].rfind(':') {
                let mut redacted = url.to_string();
                redacted.replace_range(colon_pos + 1..at_pos, "***");
                return redacted;
            }
        }
        url.to_string()
    }
}
