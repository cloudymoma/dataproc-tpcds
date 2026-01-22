//! Configuration module for TPC-DS data generator.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

/// Main configuration structure matching conf.yaml
#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub gcp: GcpConfig,
    pub benchmark: BenchmarkConfig,
    #[serde(default)]
    pub datagen: DatagenConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct GcpConfig {
    #[allow(dead_code)]
    pub project_id: String,
    #[allow(dead_code)]
    pub region: String,
    pub staging_bucket: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct BenchmarkConfig {
    pub scale_factor: u32,
    pub data_format: String,
    #[serde(default = "default_compression")]
    pub format_compression: String,
    pub data_path: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatagenConfig {
    /// Number of generator threads (default: number of CPUs)
    #[serde(default = "default_generator_threads")]
    pub generator_threads: usize,

    /// Number of uploader threads (default: 4)
    #[serde(default = "default_uploader_threads")]
    pub uploader_threads: usize,

    /// Target file size in MB (default: 128)
    #[serde(default = "default_file_size_mb")]
    pub file_size_mb: usize,

    /// Local temp directory for generated files
    #[serde(default = "default_temp_dir")]
    pub temp_dir: String,

    /// Batch size for row generation (default: 100000)
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Whether to delete local files after upload (default: true)
    #[serde(default = "default_cleanup")]
    pub cleanup_after_upload: bool,
}

fn default_compression() -> String {
    "snappy".to_string()
}

fn default_generator_threads() -> usize {
    num_cpus::get()
}

fn default_uploader_threads() -> usize {
    4
}

fn default_file_size_mb() -> usize {
    128
}

fn default_temp_dir() -> String {
    "/tmp/tpcds-datagen".to_string()
}

fn default_batch_size() -> usize {
    50_000  // Optimal for memory and Parquet row group size
}

fn default_cleanup() -> bool {
    true
}

impl Default for DatagenConfig {
    fn default() -> Self {
        Self {
            generator_threads: default_generator_threads(),
            uploader_threads: default_uploader_threads(),
            file_size_mb: default_file_size_mb(),
            temp_dir: default_temp_dir(),
            batch_size: default_batch_size(),
            cleanup_after_upload: default_cleanup(),
        }
    }
}

impl Config {
    /// Load configuration from YAML file
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = std::fs::read_to_string(path.as_ref())
            .with_context(|| format!("Failed to read config file: {:?}", path.as_ref()))?;

        let config: Config = serde_yaml::from_str(&content)
            .with_context(|| "Failed to parse config YAML")?;

        Ok(config)
    }

    /// Get the GCS bucket name (without gs:// prefix)
    pub fn gcs_bucket(&self) -> &str {
        self.gcp
            .staging_bucket
            .strip_prefix("gs://")
            .unwrap_or(&self.gcp.staging_bucket)
    }

    /// Get the data path prefix (without gs://bucket/)
    pub fn data_prefix(&self) -> String {
        let path = self.benchmark.data_path.strip_prefix("gs://").unwrap_or(&self.benchmark.data_path);
        // Remove bucket name from path
        if let Some(pos) = path.find('/') {
            path[pos + 1..].to_string()
        } else {
            String::new()
        }
    }

    /// Get target file size in bytes
    pub fn file_size_bytes(&self) -> usize {
        self.datagen.file_size_mb * 1024 * 1024
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = DatagenConfig::default();
        assert_eq!(config.file_size_mb, 128);
        assert_eq!(config.uploader_threads, 4);
        assert!(config.cleanup_after_upload);
    }
}
