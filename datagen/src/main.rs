//! TPC-DS High-Performance Data Generator
//!
//! This application generates TPC-DS benchmark data in Parquet format
//! and uploads it to GCS in parallel. It uses two thread pools:
//! - Generator threads: Create data files locally
//! - Uploader threads: Upload completed files to GCS
//!
//! Usage:
//!   tpcds-datagen --config ../conf.yaml

mod config;
mod generator;
mod schema;
mod uploader;

use std::path::PathBuf;
use std::thread;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::Parser;
use crossbeam::channel::bounded;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::config::Config;
use crate::generator::Generator;
use crate::uploader::Uploader;

/// TPC-DS Data Generator CLI
#[derive(Parser, Debug)]
#[command(name = "tpcds-datagen")]
#[command(author, version, about = "High-performance TPC-DS data generator with parallel GCS upload")]
struct Cli {
    /// Path to configuration file (conf.yaml)
    #[arg(short, long, default_value = "conf.yaml")]
    config: PathBuf,

    /// Override scale factor from config
    #[arg(short, long)]
    scale_factor: Option<u32>,

    /// Override number of generator threads
    #[arg(long)]
    generator_threads: Option<usize>,

    /// Override number of uploader threads
    #[arg(long)]
    uploader_threads: Option<usize>,

    /// Override file size in MB
    #[arg(long)]
    file_size_mb: Option<usize>,

    /// Dry run - generate locally but don't upload
    #[arg(long)]
    dry_run: bool,

    /// Generate only specified tables (comma-separated)
    #[arg(long)]
    tables: Option<String>,

    /// Verbose output
    #[arg(short, long)]
    verbose: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    let filter = if cli.verbose {
        EnvFilter::new("debug")
    } else {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    // Load configuration
    let mut config = Config::load(&cli.config)
        .with_context(|| format!("Failed to load config: {:?}", cli.config))?;

    // Apply CLI overrides
    if let Some(sf) = cli.scale_factor {
        config.benchmark.scale_factor = sf;
    }
    if let Some(threads) = cli.generator_threads {
        config.datagen.generator_threads = threads;
    }
    if let Some(threads) = cli.uploader_threads {
        config.datagen.uploader_threads = threads;
    }
    if let Some(size) = cli.file_size_mb {
        config.datagen.file_size_mb = size;
    }

    info!("TPC-DS Data Generator");
    info!("=====================");
    info!("Scale Factor: {}", config.benchmark.scale_factor);
    info!("Data Format: {}", config.benchmark.data_format);
    info!("Compression: {}", config.benchmark.format_compression);
    info!("File Size: {} MB", config.datagen.file_size_mb);
    info!("Generator Threads: {}", config.datagen.generator_threads);
    info!("Uploader Threads: {}", config.datagen.uploader_threads);
    info!("Temp Directory: {}", config.datagen.temp_dir);
    info!("Target: gs://{}/{}", config.gcs_bucket(), config.data_prefix());
    info!("Dry Run: {}", cli.dry_run);

    let start = Instant::now();

    if cli.dry_run {
        // Dry run: generate locally without uploading
        run_dry(config)?;
    } else {
        // Full run: generate and upload in parallel
        run_full(config)?;
    }

    let elapsed = start.elapsed();
    info!("Total time: {:?}", elapsed);

    Ok(())
}

/// Run full generation with parallel upload
fn run_full(config: Config) -> Result<()> {
    // Create bounded channel between generators and uploaders
    // Larger buffer to prevent generator thread blocking
    // Buffer size = max(100, uploader_threads * 16) for better throughput
    let channel_size = std::cmp::max(100, config.datagen.uploader_threads * 16);
    let (sender, receiver) = bounded(channel_size);

    // Create generator and uploader
    let generator = Generator::new(config.clone(), sender);
    let uploader = Uploader::new(config.clone(), receiver);

    let gen_stats = generator.stats();
    let up_stats = uploader.stats();

    // Start uploader in a separate thread
    let uploader_handle = thread::spawn(move || {
        if let Err(e) = uploader.run() {
            tracing::error!("Uploader error: {}", e);
        }
    });

    // Run generators in main thread
    generator.generate_all()?;

    // Wait for uploader to finish
    uploader_handle.join().map_err(|_| anyhow::anyhow!("Uploader thread panicked"))?;

    // Print final statistics
    info!("=== Generation Statistics ===");
    info!(
        "Files Generated: {}",
        gen_stats.files_generated.load(std::sync::atomic::Ordering::Relaxed)
    );
    info!(
        "Rows Generated: {}",
        gen_stats.rows_generated.load(std::sync::atomic::Ordering::Relaxed)
    );
    info!(
        "Bytes Written: {} MB",
        gen_stats.bytes_written.load(std::sync::atomic::Ordering::Relaxed) / 1024 / 1024
    );

    info!("=== Upload Statistics ===");
    info!(
        "Files Uploaded: {}",
        up_stats.files_uploaded.load(std::sync::atomic::Ordering::Relaxed)
    );
    info!(
        "Bytes Uploaded: {} MB",
        up_stats.bytes_uploaded.load(std::sync::atomic::Ordering::Relaxed) / 1024 / 1024
    );
    info!(
        "Upload Errors: {}",
        up_stats.upload_errors.load(std::sync::atomic::Ordering::Relaxed)
    );

    Ok(())
}

/// Run in dry-run mode (generate locally, no upload)
fn run_dry(config: Config) -> Result<()> {
    // Create a channel but don't start uploader - just drain it
    let (sender, receiver) = bounded(1000);

    let generator = Generator::new(config.clone(), sender);
    let gen_stats = generator.stats();

    // Start a thread to drain the receiver
    let drain_handle = thread::spawn(move || {
        let mut count = 0;
        for file in receiver.iter() {
            count += 1;
            info!(
                "Generated (dry-run): {}/{} - {} rows, {} bytes",
                file.table_name,
                file.local_path.file_name().unwrap_or_default().to_string_lossy(),
                file.row_count,
                file.file_size
            );
        }
        count
    });

    // Run generators
    generator.generate_all()?;
    drop(generator); // Close the sender

    // Wait for drain to complete
    let file_count = drain_handle.join().map_err(|_| anyhow::anyhow!("Drain thread panicked"))?;

    info!("=== Dry Run Statistics ===");
    info!("Files Generated: {}", file_count);
    info!(
        "Rows Generated: {}",
        gen_stats.rows_generated.load(std::sync::atomic::Ordering::Relaxed)
    );
    info!(
        "Bytes Written: {} MB",
        gen_stats.bytes_written.load(std::sync::atomic::Ordering::Relaxed) / 1024 / 1024
    );
    info!("Note: Files saved to {} (not uploaded)", config.datagen.temp_dir);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_parsing() {
        // Test default values
        let cli = Cli::try_parse_from(["tpcds-datagen"]).unwrap();
        assert_eq!(cli.config, PathBuf::from("conf.yaml"));
        assert!(!cli.dry_run);
        assert!(!cli.verbose);
    }

    #[test]
    fn test_cli_with_options() {
        let cli = Cli::try_parse_from([
            "tpcds-datagen",
            "--config",
            "custom.yaml",
            "--scale-factor",
            "10",
            "--dry-run",
            "--verbose",
        ])
        .unwrap();

        assert_eq!(cli.config, PathBuf::from("custom.yaml"));
        assert_eq!(cli.scale_factor, Some(10));
        assert!(cli.dry_run);
        assert!(cli.verbose);
    }
}
