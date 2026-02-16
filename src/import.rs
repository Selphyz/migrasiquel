use crate::engine::value::SqlValue;
use crate::engine::{DbEngine, DbSession};
use anyhow::{bail, Context, Result};
use chrono::Datelike;
use csv::ReaderBuilder;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

pub struct ImportOptions {
    pub input: String,
    pub table: String,
    pub batch_rows: usize,
    pub disable_fk_checks: bool,
    pub skip_errors: bool,
    pub column_mapping: Option<HashMap<String, String>>,
}

pub async fn import(engine: &dyn DbEngine, url: &str, options: ImportOptions) -> Result<()> {
    println!("Starting CSV import...");

    // Connect to database
    println!("Connecting to database...");
    let mut session = engine
        .connect(url)
        .await
        .context("Failed to connect to database")?;

    // Check if input file exists
    if !Path::new(&options.input).exists() {
        bail!("Input file not found: {}", options.input);
    }

    // Read CSV header
    println!("Reading CSV header...");
    let file = File::open(&options.input).context("Failed to open input file")?;
    let mut csv_reader = ReaderBuilder::new().from_reader(file);

    let headers = csv_reader.headers().context("Failed to read CSV headers")?;
    let csv_columns: Vec<String> = headers.iter().map(|h| h.to_string()).collect();

    if csv_columns.is_empty() {
        bail!("CSV file has no columns");
    }

    // Get column mapping
    let column_mapping = options
        .column_mapping
        .clone()
        .unwrap_or_else(|| csv_columns.iter().map(|c| (c.clone(), c.clone())).collect());

    let db_columns: Vec<String> = csv_columns
        .iter()
        .map(|csv_col| {
            column_mapping
                .get(csv_col)
                .cloned()
                .unwrap_or_else(|| csv_col.clone())
        })
        .collect();

    // Infer column types from first 100 rows
    println!("Inferring column types...");
    let file = File::open(&options.input).context("Failed to open input file")?;
    let mut csv_reader = ReaderBuilder::new().from_reader(file);

    let inferred_types = infer_column_types(&mut csv_reader, &csv_columns, 100)?;

    // Check if table exists
    let tables = session
        .list_tables(&[options.table.clone()], &[])
        .await
        .context("Failed to list tables")?;

    let table_exists = !tables.is_empty();

    // Create table if it doesn't exist
    if !table_exists {
        println!("Creating table '{}'...", options.table);
        session
            .create_table_from_columns(&options.table, &db_columns, &inferred_types)
            .await
            .context("Failed to create table")?;
    } else {
        println!(
            "Table '{}' already exists, inserting data...",
            options.table
        );
    }

    // Disable constraints if requested
    if options.disable_fk_checks {
        println!("Disabling foreign key checks...");
        session
            .disable_constraints()
            .await
            .context("Failed to disable constraints")?;
    }

    // Process and insert rows
    println!("Importing data...");
    let file = File::open(&options.input).context("Failed to open input file")?;
    let mut csv_reader = ReaderBuilder::new().from_reader(file);

    // Skip header
    let _headers = csv_reader.headers().context("Failed to read CSV headers")?;

    let mut batch: Vec<(usize, Vec<SqlValue>)> = Vec::new();
    let mut error_rows: Vec<(usize, String)> = Vec::new();
    let mut row_number = 1; // Header is row 1
    let mut total_inserted = 0u64;

    let progress = ProgressBar::new_spinner();
    progress.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap(),
    );
    progress.set_message("Processing rows...");

    for result in csv_reader.deserialize::<Vec<String>>() {
        row_number += 1;

        match result {
            Ok(row) => match parse_row(&row, &csv_columns, &db_columns, &inferred_types) {
                Ok(values) => {
                    batch.push((row_number, values));

                    if batch.len() >= options.batch_rows {
                        total_inserted += insert_batch_with_row_tracking(
                            &mut *session,
                            &options.table,
                            &db_columns,
                            &batch,
                            options.skip_errors,
                            &mut error_rows,
                        )
                        .await
                        .context("Failed to insert batch")?;
                        progress.set_message(format!("Inserted {} rows...", total_inserted));
                        batch.clear();
                    }
                }
                Err(e) => {
                    error_rows.push((row_number, e.to_string()));
                    if !options.skip_errors {
                        bail!("Error at row {}: {}", row_number, e);
                    }
                }
            },
            Err(e) => {
                error_rows.push((row_number, format!("CSV parse error: {}", e)));
                if !options.skip_errors {
                    bail!("CSV parse error at row {}: {}", row_number, e);
                }
            }
        }
    }

    // Insert remaining batch
    if !batch.is_empty() {
        total_inserted += insert_batch_with_row_tracking(
            &mut *session,
            &options.table,
            &db_columns,
            &batch,
            options.skip_errors,
            &mut error_rows,
        )
        .await
        .context("Failed to insert final batch")?;
    }

    // Re-enable constraints
    if options.disable_fk_checks {
        println!("Re-enabling foreign key checks...");
        session
            .enable_constraints()
            .await
            .context("Failed to enable constraints")?;
    }

    // Commit transaction
    println!("Committing transaction...");
    session
        .commit()
        .await
        .context("Failed to commit transaction")?;

    progress.finish_and_clear();

    // Print summary
    println!("\n═══════════════════════════════════════");
    println!("CSV Import Summary");
    println!("═══════════════════════════════════════");
    println!("Source:        {}", options.input);
    println!("Table:         {}", options.table);
    println!("Total rows:    {} (including header)", row_number);
    println!("Inserted:      {} rows ✓", total_inserted);
    println!("Failed:        {} rows ✗", error_rows.len());
    println!("═══════════════════════════════════════");

    // Show failed rows
    if !error_rows.is_empty() {
        println!("\nFailed rows:");
        for (line, err) in error_rows.iter().take(10) {
            println!("  Line {}: {}", line, err);
        }
        if error_rows.len() > 10 {
            println!("  ... and {} more errors", error_rows.len() - 10);
        }
    }

    Ok(())
}

async fn insert_batch_with_row_tracking(
    session: &mut dyn DbSession,
    table: &str,
    columns: &[String],
    batch: &[(usize, Vec<SqlValue>)],
    skip_errors: bool,
    error_rows: &mut Vec<(usize, String)>,
) -> Result<u64> {
    let rows: Vec<Vec<SqlValue>> = batch.iter().map(|(_, row)| row.clone()).collect();

    match session.insert_batch(table, columns, &rows).await {
        Ok(()) => Ok(batch.len() as u64),
        Err(_) => {
            let mut inserted = 0u64;

            for (row_number, row) in batch {
                let single = vec![row.clone()];
                match session.insert_batch(table, columns, &single).await {
                    Ok(()) => inserted += 1,
                    Err(err) => {
                        let details =
                            format!("Insert error: {} | record: {}", err, summarize_record(row));

                        if skip_errors {
                            error_rows.push((*row_number, details));
                            continue;
                        }

                        bail!("Error at row {}: {}", row_number, details);
                    }
                }
            }

            Ok(inserted)
        }
    }
}

fn summarize_record(row: &[SqlValue]) -> String {
    const MAX_LEN: usize = 200;
    let value = format!("{:?}", row);

    if value.len() <= MAX_LEN {
        value
    } else {
        format!("{}...", &value[..MAX_LEN])
    }
}

/// Infer column types from CSV data
fn infer_column_types(
    csv_reader: &mut csv::Reader<File>,
    csv_columns: &[String],
    sample_size: usize,
) -> Result<Vec<SqlValue>> {
    let mut type_scores: Vec<HashMap<String, usize>> = vec![HashMap::new(); csv_columns.len()];

    let mut row_count = 0;
    for result in csv_reader.deserialize::<Vec<String>>() {
        if row_count >= sample_size {
            break;
        }

        match result {
            Ok(row) => {
                for (col_idx, value) in row.iter().enumerate() {
                    if col_idx >= csv_columns.len() {
                        break;
                    }

                    let detected_type = detect_value_type(value);
                    *type_scores[col_idx].entry(detected_type).or_insert(0) += 1;
                }
                row_count += 1;
            }
            Err(_) => {
                // Skip malformed rows during type inference
                continue;
            }
        }
    }

    // Determine final type for each column
    let mut inferred_types = Vec::new();
    for scores in type_scores {
        let final_type = if let Some((type_name, _)) = scores.iter().max_by_key(|(_, &count)| count)
        {
            match type_name.as_str() {
                "int" => SqlValue::Int(0),
                "float" => SqlValue::Float(0.0),
                "bool" => SqlValue::Bool(false),
                "timestamp" => SqlValue::Timestamp {
                    y: 2024,
                    m: 1,
                    d: 1,
                    hh: 0,
                    mm: 0,
                    ss: 0,
                    us: 0,
                },
                "date" => SqlValue::Date {
                    y: 2024,
                    m: 1,
                    d: 1,
                },
                _ => SqlValue::String(String::new()),
            }
        } else {
            SqlValue::String(String::new())
        };

        inferred_types.push(final_type);
    }

    Ok(inferred_types)
}

/// Detect the type of a value
fn detect_value_type(value: &str) -> String {
    use chrono::NaiveDate;

    let trimmed = value.trim();

    // Check for empty/null
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("null")
        || trimmed.eq_ignore_ascii_case("none")
    {
        return "string".to_string();
    }

    // Check for boolean
    if trimmed.eq_ignore_ascii_case("true")
        || trimmed.eq_ignore_ascii_case("false")
        || trimmed.eq_ignore_ascii_case("yes")
        || trimmed.eq_ignore_ascii_case("no")
        || trimmed == "1"
        || trimmed == "0"
    {
        if trimmed == "1" || trimmed == "0" {
            // Could be int or bool, prefer int
            return "int".to_string();
        }
        return "bool".to_string();
    }

    // Check for timestamp (with time)
    if trimmed.matches(':').count() >= 2 && trimmed.contains('-') {
        return "timestamp".to_string();
    }

    // Check for date (without time)
    if let Ok(_) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        return "date".to_string();
    }

    // Check for float
    if let Ok(_) = trimmed.parse::<f64>() {
        if trimmed.contains('.') {
            return "float".to_string();
        }
    }

    // Check for integer
    if let Ok(_) = trimmed.parse::<i64>() {
        return "int".to_string();
    }

    // Default to string
    "string".to_string()
}

/// Parse a CSV row into SqlValues
fn parse_row(
    row: &[String],
    _csv_columns: &[String],
    db_columns: &[String],
    types: &[SqlValue],
) -> Result<Vec<SqlValue>> {
    use chrono::NaiveDate;

    let mut values = Vec::new();

    for (col_idx, db_col) in db_columns.iter().enumerate() {
        let value = if col_idx < row.len() {
            row[col_idx].trim()
        } else {
            ""
        };

        let sql_value = if value.is_empty()
            || value.eq_ignore_ascii_case("null")
            || value.eq_ignore_ascii_case("none")
        {
            SqlValue::Null
        } else {
            match &types[col_idx] {
                SqlValue::Int(_) => {
                    let int_val = value.parse::<i64>().context(format!(
                        "Failed to parse '{}' as integer for column '{}'",
                        value, db_col
                    ))?;
                    SqlValue::Int(int_val)
                }
                SqlValue::Float(_) => {
                    let float_val = value.parse::<f64>().context(format!(
                        "Failed to parse '{}' as float for column '{}'",
                        value, db_col
                    ))?;
                    SqlValue::Float(float_val)
                }
                SqlValue::Bool(_) => {
                    let bool_val = match value.to_lowercase().as_str() {
                        "true" | "yes" | "1" => true,
                        "false" | "no" | "0" => false,
                        _ => bail!(
                            "Failed to parse '{}' as boolean for column '{}'",
                            value,
                            db_col
                        ),
                    };
                    SqlValue::Bool(bool_val)
                }
                SqlValue::Date { .. } => {
                    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d").context(format!(
                        "Failed to parse '{}' as date (YYYY-MM-DD) for column '{}'",
                        value, db_col
                    ))?;
                    SqlValue::Date {
                        y: date.year(),
                        m: date.month(),
                        d: date.day(),
                    }
                }
                SqlValue::Timestamp { .. } => {
                    let parts: Vec<&str> = value.split(' ').collect();
                    if parts.len() < 2 {
                        bail!(
                            "Failed to parse '{}' as timestamp for column '{}': invalid format",
                            value,
                            db_col
                        );
                    }

                    let date_parts: Vec<&str> = parts[0].split('-').collect();
                    let time_parts: Vec<&str> = parts[1].split(':').collect();

                    if date_parts.len() != 3 || time_parts.len() < 2 {
                        bail!(
                            "Failed to parse '{}' as timestamp for column '{}'",
                            value,
                            db_col
                        );
                    }

                    let y = date_parts[0]
                        .parse::<i32>()
                        .context("Invalid year in timestamp")?;
                    let m = date_parts[1]
                        .parse::<u32>()
                        .context("Invalid month in timestamp")?;
                    let d = date_parts[2]
                        .parse::<u32>()
                        .context("Invalid day in timestamp")?;
                    let hh = time_parts[0]
                        .parse::<u32>()
                        .context("Invalid hour in timestamp")?;
                    let mm = time_parts[1]
                        .parse::<u32>()
                        .context("Invalid minute in timestamp")?;
                    let ss = if time_parts.len() > 2 {
                        time_parts[2]
                            .parse::<u32>()
                            .context("Invalid second in timestamp")?
                    } else {
                        0
                    };

                    SqlValue::Timestamp {
                        y,
                        m,
                        d,
                        hh,
                        mm,
                        ss,
                        us: 0,
                    }
                }
                _ => SqlValue::String(value.to_string()),
            }
        };

        values.push(sql_value);
    }

    Ok(values)
}

/// Parse column mapping from string format: "col1:db_col1,col2:db_col2"
pub fn parse_column_mapping(mapping: &str) -> Result<HashMap<String, String>> {
    let mut result = HashMap::new();

    for pair in mapping.split(',') {
        let parts: Vec<&str> = pair.trim().split(':').collect();
        if parts.len() != 2 {
            bail!("Invalid column mapping format. Expected 'csv_col:db_col,csv_col2:db_col2'");
        }

        result.insert(parts[0].to_string(), parts[1].to_string());
    }

    Ok(result)
}
