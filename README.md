[![Rust Build](https://github.com/cloudymoma/dataproc-tpcds/actions/workflows/rust.yml/badge.svg)](https://github.com/cloudymoma/dataproc-tpcds/actions/workflows/rust.yml)

English | [简体中文](README_cn.md)

# GCP Dataproc TPC-DS Auto-Benchmark Tool

A lightweight, highly automated command-line tool for running TPC-DS (1TB scale) performance benchmarks on Google Cloud Dataproc with a single configuration file.

## Features

- **Simple**: Pure Python and Shell, no complex dependencies
- **Stateless**: No Hive Metastore required - uses GCS + Temporary Views
- **Observable**: Structured results stored in BigQuery for easy comparison across different machine types and Spark configurations
- **Automated**: One command to create cluster, generate data, run queries, and report metrics

## Step-by-Step Benchmark Guide

This guide walks you through the complete benchmark process from setup to cleanup.

### Prerequisites

1. **Google Cloud SDK** installed and configured (`gcloud` CLI)
2. **Python 3.8+** with pip
3. **Rust 1.70+** (optional, only required for Rust data generator engine) - install via [rustup](https://rustup.rs/)
4. **GCP Project** with these APIs enabled:
   - Compute Engine API
   - Dataproc API
   - Cloud Storage API
   - BigQuery API

   Enable all required APIs with:
   ```bash
   gcloud services enable compute.googleapis.com \
       dataproc.googleapis.com \
       storage.googleapis.com \
       bigquery.googleapis.com
   ```

### Step 1: Installation

```bash
# Clone the repository
git clone <repository-url>
cd dataproc-tpcds

# Install Python dependencies
pip install -r requirements.txt

# Authenticate with Google Cloud
gcloud auth application-default login
```

### Step 2: Configuration

Edit `conf.yaml` with your settings:

```yaml
gcp:
  project_id: "your-project-id"
  region: "us-central1"
  staging_bucket: "gs://your-bucket"

dataproc:
  cluster_name: "tpcds-bench"
  num_workers: 4
  worker_machine_type: "n2-standard-8"

benchmark:
  scale_factor: 1000  # 1TB (use 1-10 for testing)
  data_path: "gs://your-bucket/tpcds-data/1T"
  data_format: "parquet"

# Data Generator Engine Selection
datagen:
  engine: "spark"  # "spark" (default) or "rust"

  # Spark engine settings (distributed on Dataproc cluster)
  spark:
    parallelism: 32

  # Rust engine settings (local high-performance, requires Rust toolchain)
  rust:
    generator_threads: 8
    uploader_threads: 4
    file_size_mb: 128
```

### Step 3: Validate Configuration

```bash
# Validate config without executing anything
make dry-run
```

### Step 4: Generate TPC-DS Data

> **Important:** Data generation and benchmarking are **independent operations**. You must generate data before running benchmarks. Data only needs to be generated **once per scale factor** - you can run multiple benchmarks against the same dataset.

Data generation uses the engine specified in `conf.yaml` (`datagen.engine`):

```bash
# First, create the cluster (required for Spark engine)
make cluster-create

# Generate data using configured engine (spark or rust)
make data-gen

# Verify data was generated
make data-check
```

**Engine Selection:**
- **`spark`** (default): Distributed generation on Dataproc cluster. No Rust dependency required. Faster for large scale factors (100GB+) due to distributed I/O. **Requires cluster to be running.**
- **`rust`**: High-performance local generator with parallel GCS upload. Requires Rust toolchain. Can generate data without a cluster.

**Data Reuse:** Once generated, the same dataset can be used for multiple benchmark runs. Set `skip_data_gen: true` in conf.yaml to skip data generation in subsequent runs, or simply don't run `make data-gen` again.

### Step 5: Run the Benchmark

> **Prerequisite:** Ensure data has been generated (Step 4) before running benchmarks.

```bash
# Interactive mode (prompts for cluster cleanup at end)
make run

# Or auto-delete cluster when done (recommended for cost savings)
make run-auto-delete

# For debugging, use verbose mode
make run-verbose
```

### Step 6: View Results

Results are stored in BigQuery:

```bash
# Open BigQuery Console
open "https://console.cloud.google.com/bigquery?project=your-project-id"
```

Query your results:

```sql
-- View all benchmark runs
SELECT * FROM `your-project.tpcds_metrics.benchmark_history`
ORDER BY run_timestamp DESC
LIMIT 100;

-- Compare query performance across runs
SELECT
  batch_id,
  query_name,
  duration_sec,
  worker_count,
  worker_machine_type
FROM `your-project.tpcds_metrics.benchmark_history`
WHERE status = 'SUCCESS'
ORDER BY query_name, run_timestamp DESC;
```

### Step 7: Cleanup Resources

**IMPORTANT:** Clean up to avoid ongoing charges!

```bash
# Delete the benchmark cluster
make cluster-delete

# Delete the history server (if enabled)
make history-server-delete

# Optionally delete generated data from GCS
gsutil -m rm -r gs://your-bucket/tpcds-data/

# Optionally delete BigQuery dataset
bq rm -r -f your-project:tpcds_metrics
```

See [Cleanup Guide](#cleanup-guide) for detailed cleanup instructions.

---

## Quick Start (5 Minutes)

For those who want to get started quickly:

```bash
# 1. Clone and install
git clone <repository-url> && cd dataproc-tpcds
pip install -r requirements.txt
gcloud auth application-default login

# 2. Edit conf.yaml with your project/bucket
vim conf.yaml

# 3. Run benchmark with auto-cleanup
make run-auto-delete
```

## Make Targets

All common operations are available via Make:

```bash
make help              # Show all available targets

# Setup
make install           # Install production dependencies
make install-dev       # Install dev dependencies (pytest, etc.)
make quick-start       # First-time setup: install + validate

# Running Benchmarks
make run               # Run full benchmark (interactive)
make dry-run           # Validate config without executing
make run-auto-delete   # Run and auto-delete cluster when done
make run-verbose       # Run with verbose logging

# Testing
make test              # Run all tests (Python)
make test-cov          # Run tests with coverage report
make check-syntax      # Verify Python syntax
make full-test         # Run all tests (Python + Rust)

# Cluster Operations
make cluster-create    # Create Dataproc cluster only
make cluster-delete    # Delete Dataproc cluster
make cluster-status    # Check cluster status
make cluster-info      # Show cluster config

# Data Operations
make data-gen          # Generate TPC-DS data (uses engine from config)
make data-check        # Check if data exists
make data-tables       # List available tables

# Rust Data Generator (Development)
make datagen-build     # Build Rust data generator (release)
make datagen-test      # Run Rust datagen tests
make datagen-clean     # Clean Rust build artifacts

# BigQuery
make bq-setup          # Create BQ dataset/table
make bq-schema         # Show table schema

# Utilities
make validate          # Run all validation checks
make clean             # Remove cache files
make list-queries      # List available SQL queries
make show-query QUERY=q1  # Show specific query
```

## Data Generation Engines

This tool supports two data generation engines. Choose based on your needs:

### Spark Engine (Default)

Distributed generation running on the Dataproc cluster. Recommended for:
- Large scale factors (100GB+) where distributed I/O provides significant speedup
- Users without Rust toolchain installed
- Environments where local disk space is limited

Configuration in `conf.yaml`:
```yaml
datagen:
  engine: "spark"
  spark:
    parallelism: 32           # Number of Spark partitions (0 = auto)
    partitions_per_table: 0   # Partitions per table (0 = auto based on scale)
```

### Rust Engine (High-Performance)

Local high-performance generator with parallel GCS upload. Uses a dual-threadpool architecture:
1. **Generator threads**: Generate Parquet files for all 24 TPC-DS tables in parallel
2. **Uploader threads**: Upload completed files to GCS concurrently

Recommended for:
- Smaller scale factors (1-100GB) where single-machine performance is sufficient
- Generating data without spinning up a cluster
- Maximum control over the generation process

#### Performance Optimizations

The Rust generator is highly optimized for maximum throughput:

| Optimization | Description |
|-------------|-------------|
| **Static lookup tables** | 20+ pre-computed arrays for zero-allocation string generation |
| **itoa fast formatting** | ~3x faster integer-to-string conversion than format!() |
| **Cached distributions** | Pre-computed random distributions reused across batches |
| **Zero-copy appends** | Direct StringBuilder append without intermediate allocations |
| **Capacity pre-allocation** | Estimated string lengths to minimize reallocations |
| **Optimized compression** | ZSTD level 3, GZIP level 6 tuned for speed |
| **Batch processing** | Large upload batches (threads × 8) for throughput |
| **LTO release build** | Link-time optimization with single codegen unit |

#### Building the Rust Generator

Prerequisites: Rust 1.70+ (install via [rustup](https://rustup.rs/))

```bash
# Build the release binary (with LTO optimization)
make datagen-build

# Run tests
make datagen-test
```

Note: When `engine: "rust"` is configured, `make data-gen` will automatically build the Rust binary if not already built.

#### Rust Generator Configuration

Configure in `conf.yaml` under `datagen.rust:`:

```yaml
datagen:
  engine: "rust"
  rust:
    generator_threads: 8      # Number of data generation threads (default: CPU count)
    uploader_threads: 4       # Number of GCS upload threads
    file_size_mb: 128         # Target Parquet file size
    temp_dir: "/tmp/tpcds-datagen"  # Local temp directory
    batch_size: 50000         # Rows per batch (optimized for Parquet row groups)
    cleanup_after_upload: true  # Delete local files after upload
```

#### Performance Tips

- **Scale factor 1-10**: Single machine, 4-8 generator threads
- **Scale factor 100-1000**: Use 16+ generator threads, 8+ uploader threads
- **Network-bound**: Increase `uploader_threads` if upload is the bottleneck
- **CPU-bound**: Increase `generator_threads` up to 2x CPU cores
- **Disk space**: Ensure `temp_dir` has enough space for ~10% of total data size

## Project Structure

```
dataproc-tpcds/
├── conf.yaml                 # Unified configuration file
├── main.py                   # Entry script (orchestrates all modules)
├── requirements.txt          # Python dependencies
├── Makefile                  # All make targets
├── lib/
│   ├── cluster_manager.py    # Dataproc cluster create/delete
│   ├── data_generator.py     # TPC-DS data generation logic (engine dispatcher)
│   ├── query_runner.py       # Spark SQL job submission
│   └── bq_reporter.py        # Metrics collection and BigQuery reporting
├── datagen_spark/            # Spark-based data generator
│   └── tpcds_datagen.py      # PySpark TPC-DS data generation script
├── datagen/                  # High-performance Rust data generator
│   ├── Cargo.toml            # Rust dependencies
│   └── src/
│       ├── main.rs           # CLI entry point
│       ├── config.rs         # Configuration parsing
│       ├── generator/        # Multi-threaded data generation
│       ├── schema/           # TPC-DS table schemas (24 tables)
│       └── uploader.rs       # Parallel GCS upload
├── sql/                      # TPC-DS standard queries (q1.sql - q99.sql)
├── jar/                      # Pre-compiled JARs (optional)
└── tests/                    # Unit and integration tests
```

## Configuration Reference

### GCP Configuration

| Parameter | Description | Required |
|-----------|-------------|----------|
| `project_id` | Your GCP Project ID | Yes |
| `region` | GCP region for Dataproc cluster | Yes |
| `zone` | GCP zone for compute resources | No |
| `service_account_key_path` | Path to service account JSON key | No |
| `staging_bucket` | GCS bucket for scripts and data | Yes |

### Dataproc Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `cluster_name` | Name of the Dataproc cluster | Required |
| `image_version` | Dataproc image version (determines Spark version) | `2.3-debian12` |
| `master_machine_type` | Machine type for master node | `n2-standard-4` |
| `worker_machine_type` | Machine type for worker nodes | `n2-standard-8` |
| `num_workers` | Number of worker nodes | 4 |
| `enable_component_gateway` | Enable web UI access | `true` |
| `spark_properties` | Spark configuration properties | See conf.yaml |

### Benchmark Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `scale_factor` | TPC-DS scale in GB (1000 = 1TB) | 1000 |
| `data_format` | Data format (parquet/orc) | `parquet` |
| `format_compression` | Compression codec | `snappy` |
| `data_path` | GCS path for TPC-DS data | Required |
| `skip_data_gen` | Skip data generation if data exists | `false` |
| `queries_to_run` | "all" or list like [1, 2, 3] | `all` |
| `iterations` | Number of iterations per query | 1 |

### BigQuery Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `enable` | Enable BigQuery reporting | `true` |
| `dataset` | BigQuery dataset name | `tpcds_metrics` |
| `table` | BigQuery table name | `benchmark_history` |

## Command-Line Options

```bash
python main.py [OPTIONS]

Options:
  --config, -c PATH      Path to configuration file (default: conf.yaml)
  --skip-cluster-delete  Don't prompt for cluster deletion after benchmark
  --auto-delete          Automatically delete cluster after benchmark
  --dry-run              Validate config and show plan without executing
  --verbose, -v          Enable verbose logging
```

## How It Works

### Phase 1: Cluster Creation
Creates a Dataproc cluster with your specified configuration. If the cluster already exists, it will be reused.

### Phase 2: Data Generation
If `skip_data_gen` is false, generates TPC-DS data at the specified scale factor using Spark. Data is stored in Parquet/ORC format on GCS.

### Phase 3: Query Execution
For each TPC-DS query:
1. Scans GCS to discover available table directories
2. Registers each table as a Spark Temporary View
3. Executes the SQL query
4. Collects execution metrics

### Phase 4: Reporting
Writes detailed metrics to BigQuery for analysis:
- Query execution time
- Input/shuffle bytes
- Status (success/failed)
- Cluster configuration details

## BigQuery Schema

The `benchmark_history` table includes:

| Field | Type | Description |
|-------|------|-------------|
| `job_uuid` | STRING | Unique test ID |
| `batch_id` | STRING | Batch ID for grouping queries |
| `run_timestamp` | TIMESTAMP | Execution time |
| `project_id` | STRING | GCP Project ID |
| `cluster_name` | STRING | Cluster name |
| `scale_factor` | INTEGER | Data size in GB |
| `query_name` | STRING | Query name (e.g., "q1") |
| `status` | STRING | SUCCESS/FAILED |
| `duration_sec` | FLOAT | Execution time in seconds |
| `input_bytes` | INT64 | Data scanned |
| `shuffle_read_bytes` | INT64 | Shuffle read volume |
| `shuffle_write_bytes` | INT64 | Shuffle write volume |
| `error_message` | STRING | Error details if failed |

## Analysis Queries

Query benchmark results in BigQuery:

```sql
-- Compare performance across different worker counts
SELECT
  worker_count,
  worker_machine_type,
  AVG(duration_sec) as avg_duration,
  COUNT(*) as query_count
FROM `project.tpcds_metrics.benchmark_history`
WHERE status = 'SUCCESS'
GROUP BY worker_count, worker_machine_type
ORDER BY avg_duration;

-- Find slowest queries
SELECT
  query_name,
  AVG(duration_sec) as avg_duration,
  MAX(duration_sec) as max_duration
FROM `project.tpcds_metrics.benchmark_history`
WHERE status = 'SUCCESS'
GROUP BY query_name
ORDER BY avg_duration DESC
LIMIT 10;
```

## Testing

Run the test suite:

```bash
# Install test dependencies
pip install pytest pytest-cov pytest-mock

# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=lib --cov-report=html
```

## Architecture Notes

### Stateless Design (No Metastore)

This tool intentionally avoids Hive Metastore dependencies:

1. **Data Discovery**: Scans GCS directories to find table data
2. **View Registration**: Creates Spark Temporary Views for each table
3. **Query Execution**: Runs standard TPC-DS SQL against temporary views

This ensures the benchmark measures pure Spark SQL performance without metastore overhead.

### Error Handling

- Cluster creation failures are fatal (exit code 1)
- Data generation failures are fatal
- Individual query failures are logged but don't stop the benchmark
- Cluster cleanup prompts user even on failure

## Best Practices

### For 1TB Benchmarks
- Use at least 4-8 worker nodes
- Recommended: `n2-standard-8` workers
- Enable Spark adaptive query execution

### For Reproducible Results
- Run multiple iterations (`iterations: 3`)
- Compare results with same cluster configuration
- Use BigQuery for trend analysis over time

### Cost Optimization
- Use `--auto-delete` to clean up clusters automatically
- Consider preemptible VMs for large-scale tests
- Use `skip_data_gen: true` after initial data generation

## Troubleshooting

### Common Issues

**Cluster creation fails**
- Check project quotas for Compute Engine
- Verify service account permissions

**Data generation times out**
- Increase worker count for larger scale factors
- Check GCS bucket permissions

**Queries fail with table not found**
- Verify `data_path` contains table directories
- Check that data generation completed successfully

### Logs

- Driver logs: Available in GCS staging bucket
- Cluster logs: View in Cloud Console or via `gcloud dataproc jobs describe`

## Cleanup Guide

Proper cleanup is essential to avoid ongoing GCP charges. Follow this checklist after completing your benchmarks.

### Quick Cleanup (Single Command)

```bash
# Delete benchmark cluster and clean local files
make clean-all
```

### Detailed Cleanup Steps

#### 1. Delete Dataproc Clusters

```bash
# Check running clusters
gcloud dataproc clusters list --region=us-central1

# Delete the benchmark cluster
make cluster-delete
# Or manually:
gcloud dataproc clusters delete tpcds-bench --region=us-central1 --quiet

# Delete history server (if enabled)
gcloud dataproc clusters delete tpcds-history-server --region=us-central1 --quiet
```

#### 2. Delete GCS Data (Optional)

Only delete if you don't need the generated data for future benchmarks:

```bash
# List data size
gsutil du -s gs://your-bucket/tpcds-data/

# Delete all TPC-DS data
gsutil -m rm -r gs://your-bucket/tpcds-data/

# Delete temp files from Rust generator
rm -rf /tmp/tpcds-datagen
```

#### 3. Delete BigQuery Data (Optional)

Only delete if you don't need historical benchmark results:

```bash
# View dataset info
bq show your-project:tpcds_metrics

# Delete entire dataset (includes all tables)
bq rm -r -f your-project:tpcds_metrics

# Or delete just the benchmark table
bq rm -f your-project:tpcds_metrics.benchmark_history
```

#### 4. Clean Local Build Artifacts

```bash
# Clean Python cache
make clean

# Clean Rust build artifacts
make datagen-clean

# Clean all (Python + Rust)
rm -rf __pycache__ .pytest_cache datagen/target
```

### Resource Cost Summary

| Resource | Cost Driver | Cleanup Priority |
|----------|-------------|------------------|
| **Dataproc Cluster** | Per-minute compute charges | **HIGH** - Delete immediately after use |
| **History Server** | Low cost (single small VM) | MEDIUM - Keep if running benchmarks regularly |
| **GCS Data** | Storage costs (~$0.02/GB/month) | LOW - Keep for re-running benchmarks |
| **BigQuery** | Storage + query costs | LOW - Minimal cost for metrics data |

### Verify Cleanup

```bash
# Check no Dataproc clusters are running
gcloud dataproc clusters list --region=us-central1

# Check GCS bucket contents
gsutil ls gs://your-bucket/

# Check BigQuery datasets
bq ls your-project:

# Check local disk usage
du -sh /tmp/tpcds-datagen 2>/dev/null || echo "Temp dir already cleaned"
```

### Automated Cleanup

For fully automated benchmarks with cleanup:

```bash
# Run benchmark and auto-delete cluster when done
make run-auto-delete

# Or use Python directly
python main.py --auto-delete
```

## License

Apache 2.0

## Contributing

1. Fork the repository
2. Create a feature branch
3. Run tests: `pytest tests/ -v`
4. Submit a pull request
