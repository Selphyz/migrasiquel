mod cli;
mod dump;
mod engine;
mod migrate;
mod restore;
mod util;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Commands};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Dump {
            source,
            source_env,
            output,
            provider,
            tables,
            exclude,
            schema_only,
            data_only,
            batch_rows,
            consistent_snapshot,
            gzip,
        } => {
            let source_url = Commands::get_url(&source, &source_env, "source")?;
            
            println!("Connecting to: {}", Commands::redact_url(&source_url));
            
            let engine = engine::create_engine(&provider)?;
            
            let opts = dump::DumpOptions {
                tables,
                exclude,
                schema_only,
                data_only,
                batch_rows,
                consistent_snapshot,
                gzip,
            };
            
            dump::dump(&*engine, &source_url, &output, opts).await?;
        }

        Commands::Restore {
            destination,
            destination_env,
            input,
            provider,
            disable_fk_checks,
        } => {
            let dest_url = Commands::get_url(&destination, &destination_env, "destination")?;
            
            println!("Connecting to: {}", Commands::redact_url(&dest_url));
            
            let engine = engine::create_engine(&provider)?;
            
            let opts = restore::RestoreOptions {
                disable_fk_checks,
            };
            
            restore::restore(&*engine, &dest_url, &input, opts).await?;
        }

        Commands::Migrate {
            source,
            source_env,
            destination,
            destination_env,
            provider,
            tables,
            exclude,
            schema_only,
            data_only,
            batch_rows,
            consistent_snapshot,
            disable_fk_checks,
        } => {
            let source_url = Commands::get_url(&source, &source_env, "source")?;
            let dest_url = Commands::get_url(&destination, &destination_env, "destination")?;
            
            println!("Source: {}", Commands::redact_url(&source_url));
            println!("Destination: {}", Commands::redact_url(&dest_url));
            
            let engine = engine::create_engine(&provider)?;
            
            let opts = migrate::MigrateOptions {
                tables,
                exclude,
                schema_only,
                data_only,
                batch_rows,
                consistent_snapshot,
                disable_fk_checks,
            };
            
            migrate::migrate(&*engine, &source_url, &dest_url, opts).await?;
        }
    }

    Ok(())
}
