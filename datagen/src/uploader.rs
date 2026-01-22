//! GCS uploader module with parallel file upload.
//!
//! This module implements a multi-threaded uploader that receives generated files
//! via a channel and uploads them to GCS in parallel, optionally cleaning up
//! local files after successful upload.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bytes::Bytes;
use crossbeam::channel::Receiver;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use tokio::runtime::Runtime;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::generator::GeneratedFile;

/// Upload statistics
#[derive(Debug, Default)]
pub struct UploadStats {
    pub files_uploaded: AtomicUsize,
    pub bytes_uploaded: AtomicU64,
    pub upload_errors: AtomicUsize,
}

impl UploadStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_success(&self, bytes: u64) {
        self.files_uploaded.fetch_add(1, Ordering::AcqRel);
        self.bytes_uploaded.fetch_add(bytes, Ordering::AcqRel);
    }

    pub fn add_error(&self) {
        self.upload_errors.fetch_add(1, Ordering::AcqRel);
    }
}

/// GCS uploader with parallel upload capability
pub struct Uploader {
    config: Config,
    receiver: Receiver<GeneratedFile>,
    stats: Arc<UploadStats>,
}

impl Uploader {
    pub fn new(config: Config, receiver: Receiver<GeneratedFile>) -> Self {
        Self {
            config,
            receiver,
            stats: Arc::new(UploadStats::new()),
        }
    }

    pub fn stats(&self) -> Arc<UploadStats> {
        Arc::clone(&self.stats)
    }

    /// Start the upload workers
    pub fn run(&self) -> Result<()> {
        let num_threads = self.config.datagen.uploader_threads;
        let bucket = self.config.gcs_bucket().to_string();
        let prefix = self.config.data_prefix();
        let cleanup = self.config.datagen.cleanup_after_upload;
        let stats = Arc::clone(&self.stats);

        info!(
            "Starting {} uploader threads, target: gs://{}/{}",
            num_threads, bucket, prefix
        );

        // Create tokio runtime for async GCS operations
        let rt = Runtime::new().context("Failed to create Tokio runtime")?;

        // Create object store client (coerce to trait object)
        let store: Arc<dyn ObjectStore> = rt.block_on(async {
            let gcs = GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(&bucket)
                .build()
                .context("Failed to create GCS client")?;
            Ok::<_, anyhow::Error>(Arc::new(gcs) as Arc<dyn ObjectStore>)
        })?;

        // Use rayon for parallel processing of received files
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .context("Failed to build uploader thread pool")?;

        // Process files as they arrive
        pool.install(|| {
            // Collect files into batches for parallel processing
            let mut batch: Vec<GeneratedFile> = Vec::with_capacity(num_threads * 8);
            let batch_timeout = Duration::from_millis(500);  // Longer timeout for better batching
            let max_batch_size = num_threads * 8;  // Larger batches for better throughput

            loop {
                match self.receiver.recv_timeout(batch_timeout) {
                    Ok(file) => {
                        batch.push(file);

                        // Process batch when it reaches max size
                        if batch.len() >= max_batch_size {
                            self.process_batch(&rt, &store, &batch, &prefix, cleanup, &stats);
                            batch.clear();
                        }
                    }
                    Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                        // Process any pending files
                        if !batch.is_empty() {
                            self.process_batch(&rt, &store, &batch, &prefix, cleanup, &stats);
                            batch.clear();
                        }
                    }
                    Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                        // Channel closed, process remaining files and exit
                        if !batch.is_empty() {
                            self.process_batch(&rt, &store, &batch, &prefix, cleanup, &stats);
                        }
                        break;
                    }
                }
            }
        });

        info!(
            "Upload complete: {} files, {} bytes, {} errors",
            self.stats.files_uploaded.load(Ordering::Relaxed),
            self.stats.bytes_uploaded.load(Ordering::Relaxed),
            self.stats.upload_errors.load(Ordering::Relaxed)
        );

        Ok(())
    }

    fn process_batch(
        &self,
        rt: &Runtime,
        store: &Arc<dyn ObjectStore>,
        batch: &[GeneratedFile],
        prefix: &str,
        cleanup: bool,
        stats: &Arc<UploadStats>,
    ) {
        use rayon::prelude::*;

        batch.par_iter().for_each(|file| {
            let result = rt.block_on(Self::upload_file(store, file, prefix));

            match result {
                Ok(bytes) => {
                    stats.add_success(bytes);
                    debug!(
                        "Uploaded {}/{} ({} bytes)",
                        file.table_name,
                        file.local_path.file_name().unwrap_or_default().to_string_lossy(),
                        bytes
                    );

                    // Clean up local file if configured
                    if cleanup {
                        if let Err(e) = std::fs::remove_file(&file.local_path) {
                            warn!("Failed to delete local file {:?}: {}", file.local_path, e);
                        }
                    }
                }
                Err(e) => {
                    stats.add_error();
                    error!(
                        "Failed to upload {}/{}: {}",
                        file.table_name,
                        file.local_path.file_name().unwrap_or_default().to_string_lossy(),
                        e
                    );
                }
            }
        });
    }

    async fn upload_file(
        store: &Arc<dyn ObjectStore>,
        file: &GeneratedFile,
        prefix: &str,
    ) -> Result<u64> {
        let start = Instant::now();

        // Use file size from metadata (already available)
        let file_size = file.file_size;

        // Read file content with buffer pool to reduce allocations
        let content = tokio::fs::read(&file.local_path)
            .await
            .with_context(|| format!("Failed to read file: {:?}", file.local_path))?;

        // Construct GCS path: prefix/table_name/filename
        let filename = file
            .local_path
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("No filename"))?
            .to_string_lossy();

        let gcs_path = if prefix.is_empty() {
            format!("{}/{}", file.table_name, filename)
        } else {
            format!("{}/{}/{}", prefix, file.table_name, filename)
        };

        let object_path = ObjectPath::from(gcs_path.as_str());

        // Upload to GCS - reuse the bytes without additional allocation
        store
            .put(&object_path, Bytes::from(content).into())
            .await
            .with_context(|| format!("Failed to upload to GCS: {}", gcs_path))?;

        let elapsed = start.elapsed();
        let throughput_mbps = (file_size as f64 / 1024.0 / 1024.0) / elapsed.as_secs_f64();

        debug!(
            "Uploaded {} in {:?} ({:.2} MB/s)",
            gcs_path, elapsed, throughput_mbps
        );

        Ok(file_size)
    }
}

/// Uploader that watches a directory for new files (alternative approach)
/// This is an alternative implementation that uses file system notifications
/// instead of channel-based communication.
#[allow(dead_code)]
pub struct DirectoryWatcher {
    watch_dir: PathBuf,
    config: Config,
    stats: Arc<UploadStats>,
}

#[allow(dead_code)]
impl DirectoryWatcher {
    pub fn new(config: Config) -> Self {
        Self {
            watch_dir: PathBuf::from(&config.datagen.temp_dir),
            config,
            stats: Arc::new(UploadStats::new()),
        }
    }

    pub fn stats(&self) -> Arc<UploadStats> {
        Arc::clone(&self.stats)
    }

    /// Watch directory and upload files as they appear
    pub fn watch_and_upload(&self) -> Result<()> {
        use notify::{Config as NotifyConfig, RecommendedWatcher, RecursiveMode, Watcher};

        let bucket = self.config.gcs_bucket().to_string();
        let prefix = self.config.data_prefix();
        let cleanup = self.config.datagen.cleanup_after_upload;
        let num_threads = self.config.datagen.uploader_threads;

        info!(
            "Starting directory watcher on {:?}, {} upload threads",
            self.watch_dir, num_threads
        );

        // Create tokio runtime
        let rt = Runtime::new().context("Failed to create Tokio runtime")?;

        // Create object store client (coerce to trait object)
        let store: Arc<dyn ObjectStore> = rt.block_on(async {
            let gcs = GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(&bucket)
                .build()
                .context("Failed to create GCS client")?;
            Ok::<_, anyhow::Error>(Arc::new(gcs) as Arc<dyn ObjectStore>)
        })?;

        // Create channel for file events
        let (tx, rx) = crossbeam::channel::unbounded::<PathBuf>();

        // Set up file watcher
        let tx_clone = tx.clone();
        let mut watcher = RecommendedWatcher::new(
            move |res: Result<notify::Event, notify::Error>| {
                if let Ok(event) = res {
                    if event.kind.is_create() {
                        for path in event.paths {
                            if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                                let _ = tx_clone.send(path);
                            }
                        }
                    }
                }
            },
            NotifyConfig::default(),
        )
        .context("Failed to create file watcher")?;

        watcher
            .watch(&self.watch_dir, RecursiveMode::Recursive)
            .context("Failed to start watching directory")?;

        // Process file events with thread pool
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .context("Failed to build uploader thread pool")?;

        let stats = Arc::clone(&self.stats);
        let _watch_dir = self.watch_dir.clone();

        pool.install(|| {
            for path in rx.iter() {
                // Extract table name from path
                let table_name = path
                    .parent()
                    .and_then(|p| p.file_name())
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| "unknown".to_string());

                let file = GeneratedFile {
                    local_path: path.clone(),
                    table_name: table_name.clone(),
                    file_number: 0,
                    row_count: 0,
                    file_size: std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0),
                };

                let result = rt.block_on(Self::upload_file_static(&store, &file, &prefix));

                match result {
                    Ok(bytes) => {
                        stats.add_success(bytes);
                        if cleanup {
                            let _ = std::fs::remove_file(&path);
                        }
                    }
                    Err(e) => {
                        stats.add_error();
                        error!("Failed to upload {:?}: {}", path, e);
                    }
                }
            }
        });

        Ok(())
    }

    async fn upload_file_static(
        store: &Arc<dyn ObjectStore>,
        file: &GeneratedFile,
        prefix: &str,
    ) -> Result<u64> {
        Uploader::upload_file(store, file, prefix).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upload_stats() {
        let stats = UploadStats::new();
        stats.add_success(1000);
        stats.add_success(2000);
        stats.add_error();

        assert_eq!(stats.files_uploaded.load(Ordering::Relaxed), 2);
        assert_eq!(stats.bytes_uploaded.load(Ordering::Relaxed), 3000);
        assert_eq!(stats.upload_errors.load(Ordering::Relaxed), 1);
    }
}
