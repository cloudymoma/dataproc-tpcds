//! Multi-threaded TPC-DS data generator module.
//!
//! This module implements parallel data generation for all TPC-DS tables using
//! Arrow RecordBatches and Parquet file writing. Each table is processed by
//! generator threads that produce files up to the configured size limit.

mod data_factory;
mod row_generator;

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::datatypes::Schema;
use crossbeam::channel::Sender;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::schema::{all_tables, TableSpec};

pub use row_generator::RowGenerator;

/// Message sent from generators to uploaders
#[derive(Debug, Clone)]
pub struct GeneratedFile {
    /// Local file path
    pub local_path: PathBuf,
    /// Table name
    pub table_name: String,
    /// File number within the table (for debugging/logging)
    #[allow(dead_code)]
    pub file_number: usize,
    /// Number of rows in the file
    pub row_count: u64,
    /// File size in bytes
    pub file_size: u64,
}

/// Statistics for generation progress
#[derive(Debug, Default)]
pub struct GenerationStats {
    pub files_generated: AtomicUsize,
    pub rows_generated: AtomicU64,
    pub bytes_written: AtomicU64,
}

impl GenerationStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_file(&self, rows: u64, bytes: u64) {
        self.files_generated.fetch_add(1, Ordering::AcqRel);
        self.rows_generated.fetch_add(rows, Ordering::AcqRel);
        self.bytes_written.fetch_add(bytes, Ordering::AcqRel);
    }
}

/// Table generation task
struct TableTask {
    spec: TableSpec,
    scale_factor: u32,
    file_size_bytes: usize,
    output_dir: PathBuf,
    compression: Compression,
}

/// Multi-threaded data generator
pub struct Generator {
    config: Config,
    stats: Arc<GenerationStats>,
    file_sender: Sender<GeneratedFile>,
}

impl Generator {
    pub fn new(config: Config, file_sender: Sender<GeneratedFile>) -> Self {
        Self {
            config,
            stats: Arc::new(GenerationStats::new()),
            file_sender,
        }
    }

    pub fn stats(&self) -> Arc<GenerationStats> {
        Arc::clone(&self.stats)
    }

    /// Generate all TPC-DS tables in parallel
    pub fn generate_all(&self) -> Result<()> {
        let tables = all_tables();
        let scale_factor = self.config.benchmark.scale_factor;
        let file_size_bytes = self.config.file_size_bytes();
        let temp_dir = PathBuf::from(&self.config.datagen.temp_dir);
        let compression = self.parse_compression();

        info!(
            "Starting data generation: {} tables, SF={}, file_size={}MB",
            tables.len(),
            scale_factor,
            self.config.datagen.file_size_mb
        );

        // Ensure temp directory exists
        std::fs::create_dir_all(&temp_dir)
            .with_context(|| format!("Failed to create temp directory: {:?}", temp_dir))?;

        // Create tasks for each table
        let tasks: Vec<TableTask> = tables
            .into_iter()
            .map(|spec| {
                let table_dir = temp_dir.join(&spec.name);
                std::fs::create_dir_all(&table_dir).ok();
                TableTask {
                    spec,
                    scale_factor,
                    file_size_bytes,
                    output_dir: table_dir,
                    compression,
                }
            })
            .collect();

        // Configure rayon thread pool
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.config.datagen.generator_threads)
            .build()
            .context("Failed to build generator thread pool")?;

        // Process tables in parallel
        let stats = Arc::clone(&self.stats);
        let sender = self.file_sender.clone();
        let batch_size = self.config.datagen.batch_size;

        pool.install(|| {
            tasks.par_iter().for_each(|task| {
                if let Err(e) = Self::generate_table(task, &stats, &sender, batch_size) {
                    warn!("Failed to generate table {}: {}", task.spec.name, e);
                }
            });
        });

        info!(
            "Generation complete: {} files, {} rows, {} bytes",
            self.stats.files_generated.load(Ordering::Relaxed),
            self.stats.rows_generated.load(Ordering::Relaxed),
            self.stats.bytes_written.load(Ordering::Relaxed)
        );

        Ok(())
    }

    /// Generate a single table's data
    fn generate_table(
        task: &TableTask,
        stats: &Arc<GenerationStats>,
        sender: &Sender<GeneratedFile>,
        batch_size: usize,
    ) -> Result<()> {
        let total_rows = task.spec.rows_for_scale(task.scale_factor);
        let table_name = task.spec.name;
        let schema = Arc::clone(&task.spec.schema);

        info!(
            "Generating table '{}': {} total rows",
            table_name, total_rows
        );

        let mut file_number = 0;
        let mut rows_remaining = total_rows;
        let row_generator = RowGenerator::new(Arc::clone(&schema), table_name);

        while rows_remaining > 0 {
            let file_path = task.output_dir.join(format!(
                "{}_{:05}.parquet",
                table_name, file_number
            ));

            let (rows_written, file_size) = Self::write_parquet_file(
                &file_path,
                &schema,
                &row_generator,
                task.file_size_bytes,
                batch_size,
                rows_remaining,
                task.compression,
            )?;

            // Update stats
            stats.add_file(rows_written, file_size);

            // Send to uploader
            let generated_file = GeneratedFile {
                local_path: file_path,
                table_name: table_name.to_string(),
                file_number,
                row_count: rows_written,
                file_size,
            };

            if sender.send(generated_file).is_err() {
                warn!("Upload channel closed, stopping generation for {}", table_name);
                break;
            }

            rows_remaining = rows_remaining.saturating_sub(rows_written);
            file_number += 1;

            debug!(
                "Table '{}' file {} complete: {} rows, {} bytes, {} remaining",
                table_name, file_number - 1, rows_written, file_size, rows_remaining
            );
        }

        info!(
            "Table '{}' complete: {} files generated",
            table_name, file_number
        );
        Ok(())
    }

    /// Write a single Parquet file up to the size limit
    fn write_parquet_file(
        file_path: &PathBuf,
        schema: &Arc<Schema>,
        row_generator: &RowGenerator,
        max_file_size: usize,
        batch_size: usize,
        max_rows: u64,
        compression: Compression,
    ) -> Result<(u64, u64)> {
        let file = std::fs::File::create(file_path)
            .with_context(|| format!("Failed to create file: {:?}", file_path))?;

        let props = WriterProperties::builder()
            .set_compression(compression)
            .set_max_row_group_size(batch_size.min(50000))  // Optimal row group size
            .set_write_batch_size(1024)  // Better memory usage
            .set_data_page_size_limit(1024 * 1024)  // 1MB data pages
            .build();

        let mut writer = ArrowWriter::try_new(file, Arc::clone(schema), Some(props))
            .context("Failed to create Arrow writer")?;

        let mut total_rows: u64 = 0;
        let mut estimated_size: usize = 0;

        while estimated_size < max_file_size && total_rows < max_rows {
            let rows_to_generate = batch_size.min((max_rows - total_rows) as usize);
            if rows_to_generate == 0 {
                break;
            }

            let batch = row_generator.generate_batch(rows_to_generate, total_rows)?;
            let batch_bytes = batch.get_array_memory_size();

            writer.write(&batch).context("Failed to write batch")?;

            total_rows += rows_to_generate as u64;
            // Estimate compressed size (compression ratio ~3-4x for typical data)
            estimated_size += batch_bytes / 3;
        }

        writer.close().context("Failed to close Parquet writer")?;

        let file_size = std::fs::metadata(file_path)
            .map(|m| m.len())
            .unwrap_or(0);

        Ok((total_rows, file_size))
    }

    fn parse_compression(&self) -> Compression {
        match self.config.benchmark.format_compression.to_lowercase().as_str() {
            "snappy" => Compression::SNAPPY,
            "zstd" => Compression::ZSTD(parquet::basic::ZstdLevel::try_new(3).unwrap()), // Level 3 for speed
            "lz4" => Compression::LZ4,
            "gzip" => Compression::GZIP(parquet::basic::GzipLevel::try_new(6).unwrap()), // Level 6 for balance
            "none" | "uncompressed" => Compression::UNCOMPRESSED,
            _ => {
                warn!(
                    "Unknown compression '{}', defaulting to SNAPPY",
                    self.config.benchmark.format_compression
                );
                Compression::SNAPPY
            }
        }
    }
}
