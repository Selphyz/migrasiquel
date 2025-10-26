use super::{DbEngine, DbSession, RowStream};
use crate::engine::dialect::SqlDialect;
use crate::engine::value::SqlValue;
use crate::util::dialects::mysql::MYSQL_DIALECT;
use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::stream;
use mysql_async::{prelude::*, Conn, Pool, Row};
use mysql_common::value::Value;

pub struct MysqlEngine;

#[async_trait]
impl DbEngine for MysqlEngine {
    async fn connect(&self, url: &str) -> Result<Box<dyn DbSession>> {
        let pool = Pool::new(url);
        let conn = pool
            .get_conn()
            .await
            .context("Failed to connect to MySQL database")?;

        Ok(Box::new(MysqlSession {
            conn,
            pool,
            in_transaction: false,
        }))
    }
}

pub struct MysqlSession {
    conn: Conn,
    #[allow(dead_code)]
    pool: Pool,
    in_transaction: bool,
}

#[async_trait]
impl DbSession for MysqlSession {
    fn dialect(&self) -> &'static dyn SqlDialect {
        &MYSQL_DIALECT
    }

    async fn start_consistent_snapshot(&mut self) -> Result<()> {
        self.conn
            .query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .await?;
        self.conn
            .query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            .await?;
        self.in_transaction = true;
        Ok(())
    }

    async fn list_tables(&mut self, include: &[String], exclude: &[String]) -> Result<Vec<String>> {
        let mut tables: Vec<String> = self
            .conn
            .query("SHOW TABLES")
            .await
            .context("Failed to list tables")?;

        if !include.is_empty() {
            tables.retain(|t| include.contains(t));
        }

        if !exclude.is_empty() {
            tables.retain(|t| !exclude.contains(t));
        }

        Ok(tables)
    }

    async fn show_create_table(&mut self, table: &str) -> Result<String> {
        let query = format!("SHOW CREATE TABLE `{}`", table.replace('`', "``"));
        let row: Option<Row> = self.conn.query_first(&query).await?;

        let row = row.context("No CREATE TABLE result")?;
        let create_stmt: String = row.get(1).context("Missing CREATE TABLE statement")?;

        let minified = minify_create_table(&create_stmt);
        Ok(minified)
    }

    async fn stream_rows(&mut self, table: &str) -> Result<(Vec<String>, RowStream)> {
        let query = format!(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{}' \
             ORDER BY ORDINAL_POSITION",
            table.replace('\'', "''")
        );
        let columns: Vec<String> = self.conn.query(&query).await?;

        let query = format!("SELECT * FROM `{}`", table.replace('`', "``"));
        let rows: Vec<Row> = self.conn.query(&query).await?;

        let value_rows: Vec<Result<Vec<SqlValue>>> = rows
            .into_iter()
            .map(|row| {
                let mut values = Vec::with_capacity(row.len());
                for i in 0..row.len() {
                    let mysql_value: Value = row.get(i).unwrap_or(Value::NULL);
                    values.push(convert_value(mysql_value));
                }
                Ok(values)
            })
            .collect();

        let row_stream = stream::iter(value_rows);

        Ok((columns, Box::pin(row_stream)))
    }

    async fn approximate_row_count(&mut self, table: &str) -> Result<u64> {
        let query = format!(
            "SELECT TABLE_ROWS FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{}'",
            table.replace('\'', "''")
        );

        let count: Option<u64> = self.conn.query_first(&query).await?;
        Ok(count.unwrap_or(0))
    }

    async fn insert_batch(
        &mut self,
        table: &str,
        column_names: &[String],
        rows: &[Vec<SqlValue>],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let sql = MYSQL_DIALECT.insert_values_sql(table, column_names, rows);
        self.conn
            .query_drop(sql)
            .await
            .with_context(|| format!("Failed to insert batch into table '{}'", table))?;

        Ok(())
    }

    async fn disable_constraints(&mut self) -> Result<()> {
        self.conn.query_drop("SET FOREIGN_KEY_CHECKS=0").await?;
        self.conn.query_drop("SET UNIQUE_CHECKS=0").await?;
        Ok(())
    }

    async fn enable_constraints(&mut self) -> Result<()> {
        self.conn.query_drop("SET FOREIGN_KEY_CHECKS=1").await?;
        self.conn.query_drop("SET UNIQUE_CHECKS=1").await?;
        Ok(())
    }

    async fn execute(&mut self, sql: &str) -> Result<()> {
        self.conn
            .query_drop(sql)
            .await
            .context("Failed to execute SQL statement")?;
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        if self.in_transaction {
            self.conn.query_drop("COMMIT").await?;
            self.in_transaction = false;
        }
        Ok(())
    }
}

fn convert_value(value: Value) -> SqlValue {
    match value {
        Value::NULL => SqlValue::Null,
        Value::Bytes(bytes) => match String::from_utf8(bytes.clone()) {
            Ok(s) => SqlValue::String(s),
            Err(_) => SqlValue::Bytes(bytes),
        },
        Value::Int(v) => SqlValue::Int(v),
        Value::UInt(v) => {
            if v <= i64::MAX as u64 {
                SqlValue::Int(v as i64)
            } else {
                SqlValue::Decimal(v.to_string())
            }
        }
        Value::Float(v) => SqlValue::Float(v as f64),
        Value::Double(v) => SqlValue::Float(v),
        Value::Date(year, month, day, hour, minute, second, micro) => {
            if hour == 0 && minute == 0 && second == 0 && micro == 0 {
                SqlValue::Date {
                    y: i32::from(year),
                    m: u32::from(month),
                    d: u32::from(day),
                }
            } else {
                SqlValue::Timestamp {
                    y: i32::from(year),
                    m: u32::from(month),
                    d: u32::from(day),
                    hh: u32::from(hour),
                    mm: u32::from(minute),
                    ss: u32::from(second),
                    us: micro,
                }
            }
        }
        Value::Time(neg, days, hours, minutes, seconds, micros) => {
            let total_hours = days * 24 + u32::from(hours);
            SqlValue::Time {
                neg,
                h: total_hours,
                m: minutes as u32,
                s: seconds as u32,
                us: micros,
            }
        }
    }
}

/// Minify CREATE TABLE statement to single line and add IF NOT EXISTS
fn minify_create_table(create_stmt: &str) -> String {
    let single_line = create_stmt
        .lines()
        .map(|l| l.trim())
        .collect::<Vec<_>>()
        .join(" ");

    if single_line.starts_with("CREATE TABLE") {
        single_line.replacen("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
    } else {
        single_line
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_minify_create_table() {
        let input = r#"CREATE TABLE `users` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"#;

        let output = minify_create_table(input);
        assert!(output.contains("CREATE TABLE IF NOT EXISTS"));
        assert!(!output.contains('\n'));
    }
}
