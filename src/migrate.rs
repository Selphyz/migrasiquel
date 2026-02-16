use crate::engine::dialect::SqlDialect;
use crate::engine::value::SqlValue;
use crate::engine::{DbEngine, DbSession};
use anyhow::{bail, Context, Result};
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};

pub struct MigrateOptions {
    pub tables: Vec<String>,
    pub exclude: Vec<String>,
    pub schema_only: bool,
    pub data_only: bool,
    pub batch_rows: usize,
    pub consistent_snapshot: bool,
    pub disable_fk_checks: bool,
    pub skip_errors: bool,
}

pub async fn migrate(
    engine: &dyn DbEngine,
    source_url: &str,
    destination_url: &str,
    opts: MigrateOptions,
) -> Result<()> {
    println!("Starting database migration...");

    // Connect to source and destination
    println!("Connecting to source database...");
    let mut source = engine
        .connect(source_url)
        .await
        .context("Failed to connect to source database")?;

    println!("Connecting to destination database...");
    let mut dest = engine
        .connect(destination_url)
        .await
        .context("Failed to connect to destination database")?;

    let src_dialect = source.dialect();
    let dest_dialect = dest.dialect();

    if src_dialect.name() != dest_dialect.name() {
        bail!("Cross-engine migrations are not supported in this release");
    }

    // Start consistent snapshot on source if requested
    if opts.consistent_snapshot {
        println!("Starting consistent snapshot on source...");
        source.start_consistent_snapshot().await?;
    }

    // Disable constraints on destination if requested
    if opts.disable_fk_checks {
        println!("Disabling foreign key checks on destination...");
        dest.disable_constraints().await?;
    }

    // Get list of tables from source
    let tables = source.list_tables(&opts.tables, &opts.exclude).await?;
    println!("Found {} table(s) to migrate", tables.len());

    // Migrate each table
    for (idx, table) in tables.iter().enumerate() {
        println!(
            "\n[{}/{}] Migrating table '{}'...",
            idx + 1,
            tables.len(),
            table
        );

        migrate_table(&mut *source, &mut *dest, table, dest_dialect, &opts)
            .await
            .with_context(|| format!("Failed to migrate table '{}'", table))?;
    }

    // Re-enable constraints on destination
    if opts.disable_fk_checks {
        println!("\nRe-enabling foreign key checks on destination...");
        dest.enable_constraints().await?;
    }

    // Commit both sessions
    println!("Committing transactions...");
    source.commit().await?;
    dest.commit().await?;

    println!("\nMigration completed successfully!");

    Ok(())
}

async fn migrate_table(
    source: &mut dyn DbSession,
    dest: &mut dyn DbSession,
    table: &str,
    _dest_dialect: &dyn SqlDialect,
    opts: &MigrateOptions,
) -> Result<()> {
    // Migrate schema
    if !opts.data_only {
        println!("  Creating table schema...");
        let create_stmt = source.show_create_table(table).await?;

        // Drop table first if it exists
        let drop_stmt = format!("DROP TABLE IF EXISTS `{}`;", table);
        dest.execute(&drop_stmt).await?;

        // Create table
        let normalized_create = create_stmt.trim_end_matches(';');
        dest.execute(normalized_create).await?;
    }

    // Migrate data
    if !opts.schema_only {
        println!("  Migrating data...");

        // Get approximate row count for progress
        let approx_count = source.approximate_row_count(table).await?;

        // Create progress bar
        let pb = if approx_count > 0 {
            let pb = ProgressBar::new(approx_count);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("  {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} rows ({per_sec})")
                    .unwrap()
                    .progress_chars("#>-"),
            );
            Some(pb)
        } else {
            None
        };

        // Stream rows from source
        let (columns, mut row_stream) = source.stream_rows(table).await?;

        let mut batch: Vec<(u64, Vec<SqlValue>)> = Vec::with_capacity(opts.batch_rows);
        let mut total_rows = 0u64;
        let mut failed_rows: Vec<(u64, String)> = Vec::new();
        let mut source_row_number = 0u64;

        while let Some(row_result) = row_stream.next().await {
            let row = row_result?;
            source_row_number += 1;
            batch.push((source_row_number, row));

            // Insert batch when full
            if batch.len() >= opts.batch_rows {
                let inserted = insert_batch_with_fallback(
                    dest,
                    table,
                    &columns,
                    &batch,
                    opts,
                    &mut failed_rows,
                )
                .await?;
                total_rows += inserted;

                if let Some(pb) = &pb {
                    pb.set_position(total_rows);
                }

                batch.clear();
            }
        }

        // Insert remaining rows
        if !batch.is_empty() {
            let inserted =
                insert_batch_with_fallback(dest, table, &columns, &batch, opts, &mut failed_rows)
                    .await?;
            total_rows += inserted;
        }

        if let Some(pb) = &pb {
            pb.finish_with_message(format!("Migrated {} rows", total_rows));
        } else {
            println!("  Migrated {} rows", total_rows);
        }

        if !failed_rows.is_empty() {
            println!("  Failed to insert {} row(s)", failed_rows.len());
            for (row_number, err) in failed_rows.iter().take(10) {
                println!("    Source row {}: {}", row_number, err);
            }
            if failed_rows.len() > 10 {
                println!("    ... and {} more errors", failed_rows.len() - 10);
            }
        }
    }

    Ok(())
}

async fn insert_batch_with_fallback(
    dest: &mut dyn DbSession,
    table: &str,
    columns: &[String],
    batch: &[(u64, Vec<SqlValue>)],
    opts: &MigrateOptions,
    failed_rows: &mut Vec<(u64, String)>,
) -> Result<u64> {
    let rows: Vec<Vec<SqlValue>> = batch.iter().map(|(_, row)| row.clone()).collect();

    match dest.insert_batch(table, columns, &rows).await {
        Ok(()) => Ok(batch.len() as u64),
        Err(_batch_error) => {
            let mut inserted = 0u64;

            for (row_number, row) in batch {
                let single_row = vec![row.clone()];
                match dest.insert_batch(table, columns, &single_row).await {
                    Ok(()) => inserted += 1,
                    Err(row_error) => {
                        let record = summarize_record(row);
                        let error_message =
                            format!("insert failed ({}) | record: {}", row_error, record);

                        if opts.skip_errors {
                            failed_rows.push((*row_number, error_message));
                            continue;
                        }

                        bail!(
                            "insert error on source row {} in table '{}': {}",
                            row_number,
                            table,
                            error_message
                        );
                    }
                }
            }

            Ok(inserted)
        }
    }
}

fn summarize_record(row: &[SqlValue]) -> String {
    const MAX_LEN: usize = 200;
    let full = format!("{:?}", row);

    if full.len() <= MAX_LEN {
        full
    } else {
        format!("{}...", &full[..MAX_LEN])
    }
}
