use crate::engine::{DbEngine, DbSession};
use anyhow::{Context, Result};
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use mysql_common::value::Value;

pub struct MigrateOptions {
    pub tables: Vec<String>,
    pub exclude: Vec<String>,
    pub schema_only: bool,
    pub data_only: bool,
    pub batch_rows: usize,
    pub consistent_snapshot: bool,
    pub disable_fk_checks: bool,
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
    let mut source = engine.connect(source_url).await
        .context("Failed to connect to source database")?;
    
    println!("Connecting to destination database...");
    let mut dest = engine.connect(destination_url).await
        .context("Failed to connect to destination database")?;
    
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
        println!("\n[{}/{}] Migrating table '{}'...", idx + 1, tables.len(), table);
        
        migrate_table(&mut *source, &mut *dest, table, &opts).await
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
    opts: &MigrateOptions,
) -> Result<()> {
    // Migrate schema
    if !opts.data_only {
        println!("  Creating table schema...");
        let create_stmt = source.show_create_table(table).await?;
        
        // Drop table first if it exists
        let drop_stmt = format!("DROP TABLE IF EXISTS `{}`", table.replace('`', "``"));
        dest.execute(&drop_stmt).await?;
        
        // Create table
        dest.execute(&create_stmt).await?;
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
        
        let mut batch: Vec<Vec<Value>> = Vec::with_capacity(opts.batch_rows);
        let mut total_rows = 0u64;
        
        while let Some(row_result) = row_stream.next().await {
            let row = row_result?;
            batch.push(row);
            
            // Insert batch when full
            if batch.len() >= opts.batch_rows {
                dest.insert_batch(table, &columns, &batch).await?;
                total_rows += batch.len() as u64;
                
                if let Some(pb) = &pb {
                    pb.set_position(total_rows);
                }
                
                batch.clear();
            }
        }
        
        // Insert remaining rows
        if !batch.is_empty() {
            dest.insert_batch(table, &columns, &batch).await?;
            total_rows += batch.len() as u64;
        }
        
        if let Some(pb) = &pb {
            pb.finish_with_message(format!("Migrated {} rows", total_rows));
        } else {
            println!("  Migrated {} rows", total_rows);
        }
    }
    
    Ok(())
}
