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
3. **GCP Project** with these APIs enabled:
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

4. **GCS Bucket** name for storing data and scripts (will be created automatically in the same region as your cluster if it doesn't exist)

### Step 1: Installation

```bash
# Clone the repository
git clone <repository-url>
cd dataproc-tpcds

# Install Python dependencies
make install

# Or use a custom Python version/path
make PYTHON=/usr/bin/python3.11 install

# Authenticate with Google Cloud
gcloud auth application-default login
```

> **Note**: The `PYTHON` variable can be set to any Python 3.8+ interpreter path.
> All pip operations automatically use `$(PYTHON) -m pip` for consistency.

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

# Data Generation Configuration (spark-sql-perf)
datagen:
  num_partitions: 0              # Auto-calculate for ~128MB files
  partition_tables: true         # Partition large tables
  spark_sql_perf_version: "0.5.1"
  tpcds_kit_version: "1.0.0"
```

### Step 3: Validate Configuration

```bash
# Validate config without executing anything
make dry-run
```

### Step 4: Generate TPC-DS Data

> **Important:** Data generation and benchmarking are **independent operations**. You must generate data before running benchmarks. Data only needs to be generated **once per scale factor** - you can run multiple benchmarks against the same dataset.

Data is generated using [spark-sql-perf](https://github.com/databricks/spark-sql-perf) with the native `dsdgen` binary for full TPC-DS compliance (all 24 tables with correct schemas).

```bash
# First, create the cluster
make cluster-create

# Generate data using spark-sql-perf on Dataproc
make data-gen

# Verify data was generated (checks all 24 tables)
make data-check
```

**First-Time Setup:** Before generating data, you need the pre-built assets (JAR and dsdgen binary). Either:
1. Download pre-built assets from releases
2. Build them yourself: `make build-assets` (requires Linux with SBT, GCC, Make)

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
make install
gcloud auth application-default login

# 2. Edit conf.yaml with your project/bucket
vim conf.yaml

# 3. Create cluster and generate data (first time only)
make cluster-create
make data-gen

# 4. Run benchmark with auto-cleanup
make run-auto-delete
```

> **Note:** Data generation (step 3) only needs to run once per scale factor. For subsequent benchmark runs, skip step 3 and run `make run-auto-delete` directly.

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
make test              # Run all tests
make test-cov          # Run tests with coverage report
make check-syntax      # Verify Python syntax
make full-test         # Run all tests

# Cluster Operations
make cluster-create    # Create Dataproc cluster only
make cluster-delete    # Delete Dataproc cluster
make cluster-status    # Check cluster status
make cluster-info      # Show cluster config

# Data Operations
make data-gen          # Generate TPC-DS data using spark-sql-perf
make data-check        # Check data existence and completeness
make data-tables       # List available tables
make build-assets      # Build spark-sql-perf assets (one-time setup)

# BigQuery
make bq-setup          # Create BQ dataset/table
make bq-schema         # Show table schema

# Utilities
make validate          # Run all validation checks
make clean             # Remove cache files
make list-queries      # List available SQL queries
make show-query QUERY=q1  # Show specific query
```

## Data Generation

This tool uses [spark-sql-perf](https://github.com/databricks/spark-sql-perf) from Databricks for TPC-DS data generation. This approach provides:

- **Full TPC-DS compliance**: All 24 tables with correct schemas
- **Native dsdgen binary**: Uses the official TPC-DS data generator
- **Distributed generation**: Runs on Dataproc cluster for scalability
- **Optimized partitioning**: Auto-calculated for ~128MB Parquet files

### Configuration

```yaml
datagen:
  num_partitions: 0              # Auto-calculate (scale_factor * 8)
  partition_tables: true         # Partition large fact tables
  cluster_by_partition_columns: true
  spark_sql_perf_version: "0.5.1"
  tpcds_kit_version: "1.0.0"
```

### Partition Calculation

Partitions are auto-calculated to target ~128MB Parquet files:

| Scale Factor | Partitions | Approx File Size |
|-------------|------------|------------------|
| 1 GB | 100 (min) | ~10MB |
| 100 GB | 800 | ~128MB |
| 1000 GB (1TB) | 8000 | ~128MB |
| 10000 GB (10TB) | 50000 (max) | ~200MB |

### Pre-built Assets

This repository includes pre-built assets for **Linux x86_64** architecture in the `assets/` directory:

- `assets/spark-sql-perf-assembly-0.5.1.jar` - Fat JAR with all dependencies
- `assets/tpcds-kit-1.0.0.tar.gz` - Native dsdgen binary for Linux x86_64

These assets are uploaded to your Dataproc cluster during data generation. The `dsdgen` binary is a **native executable** that must match your cluster's architecture.

> **Architecture Requirement:** The pre-built `dsdgen` binary is compiled for Linux x86_64, which is the default architecture for GCP Dataproc clusters. If your job cluster uses a different architecture (e.g., ARM-based instances), you must build the assets yourself to match your cluster's CPU architecture.

### Building Assets (Optional)

If the pre-built assets don't match your cluster architecture, or you want to use newer versions, build them yourself:

```bash
# Prerequisites: git, sbt, make, gcc, Java 11 on a Linux system
# matching your target Dataproc cluster architecture
make build-assets
```

The `make build-assets` target:
1. Clones (or updates) the [spark-sql-perf](https://github.com/databricks/spark-sql-perf) repository
2. Builds the assembly JAR using SBT
3. Clones (or updates) the [tpcds-kit](https://github.com/databricks/tpcds-kit) repository
4. Compiles the native `dsdgen` binary using GCC
5. Packages everything into the `assets/` directory

Build artifacts are created in `tmp/` (git-ignored) and final assets are placed in `assets/`.

### Troubleshooting Asset Builds

**spark-sql-perf JAR build fails with Ivy/Maven errors**

If you see errors like `origin location must be absolute` or other dependency resolution failures, clear the caches:

```bash
# Clear Ivy, SBT, and Maven caches
rm -rf ~/.ivy2/cache ~/.sbt/boot ~/.sbt/1.0/staging ~/.m2/repository

# Clear spark-sql-perf build artifacts
rm -rf tmp/spark-sql-perf/target tmp/spark-sql-perf/project/target

# Re-run the build
make build-assets
```

**tpcds-kit build fails with GCC errors**

The build script includes workarounds for GCC 14+ strictness. If you still encounter issues:

```bash
# Ensure build tools are installed
sudo apt-get install gcc make

# For persistent issues, try with an older GCC version
sudo apt-get install gcc-11
export CC=gcc-11
make build-assets
```

**Build succeeds but only one asset is created**

The script continues building even if one component fails. Check the build summary output for specific error messages and apply the relevant fix above.

## Project Structure

```
dataproc-tpcds/
├── conf.yaml                 # Unified configuration file
├── main.py                   # Entry script (orchestrates all modules)
├── requirements.txt          # Python dependencies
├── Makefile                  # All make targets
├── lib/
│   ├── cluster_manager.py    # Dataproc cluster create/delete
│   ├── data_generator.py     # TPC-DS data generation using spark-sql-perf
│   ├── query_runner.py       # Spark SQL job submission
│   └── bq_reporter.py        # Metrics collection and BigQuery reporting
├── scripts/
│   └── build_assets.sh       # One-time asset build script
├── assets/                   # Pre-built data generation assets
│   ├── spark-sql-perf-assembly-*.jar  # spark-sql-perf fat JAR
│   └── tpcds-kit-*.tar.gz    # Native dsdgen binary
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
| `batch_id` | STRING | Batch ID for grouping queries in a run |
| `run_timestamp` | TIMESTAMP | Execution timestamp (partitioned) |
| `project_id` | STRING | GCP Project ID |
| `cluster_name` | STRING | Dataproc cluster name |
| `scale_factor` | INTEGER | TPC-DS scale factor in GB |
| `spark_version` | STRING | Spark version |
| `image_version` | STRING | Dataproc image version |
| `worker_count` | INTEGER | Number of worker nodes |
| `worker_machine_type` | STRING | Worker machine type |
| `query_name` | STRING | Query name (e.g., "q1") |
| `iteration` | INTEGER | Iteration number |
| `status` | STRING | DONE, FAILED, or SKIPPED |
| `duration_sec` | FLOAT | Total execution time in seconds |
| `input_bytes` | INT64 | Total input data scanned (when available) |
| `shuffle_read_bytes` | INT64 | Shuffle read bytes (when available) |
| `shuffle_write_bytes` | INT64 | Shuffle write bytes (when available) |
| `records_read` | INT64 | Total records processed (when available) |
| `executor_cores` | INTEGER | Executor cores (from config) |
| `executor_memory` | STRING | Executor memory (from config) |
| `data_format` | STRING | Data format (parquet/orc) |
| `job_id` | STRING | Dataproc job ID |
| `error_message` | STRING | Error message if failed |

> **Note:** Metrics like `input_bytes`, `shuffle_*_bytes`, and `records_read` are collected from Spark's internal metrics when available. These may not be populated for all queries depending on Spark version and query execution.

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
# Install development dependencies (includes pytest)
make install-dev

# Run all tests
make test

# Run with coverage
make test-cov
```

## Architecture Notes

### GCS Bucket Structure

This tool uses a **single GCS bucket** with organized subfolders for all data and assets:

```
gs://{staging_bucket}/
├── lib/                              # Data generation assets (uploaded automatically)
│   ├── spark-sql-perf-assembly-*.jar   # spark-sql-perf fat JAR
│   └── tpcds-kit-*.tar.gz              # Native dsdgen binary (distributed to workers)
├── spark-events/                     # Spark event logs (for History Server)
├── scripts/                          # Query execution scripts (auto-generated)
└── tpcds-data/{scale}/              # Generated TPC-DS data
    ├── _SUCCESS                       # Data generation completion marker
    ├── store_sales/                   # 24 TPC-DS tables in Parquet/ORC format
    ├── catalog_sales/
    ├── web_sales/
    ├── date_dim/
    └── ...
```

**Asset Distribution to Workers:** The native `dsdgen` binary is packaged in a tarball and distributed to all Spark executors using Spark's `archive_uris` feature. This ensures each worker has access to the binary for parallel data generation.

**Configuration:** All paths are derived from the `staging_bucket` setting in conf.yaml. The `data_path` setting allows you to customize where TPC-DS data is stored (e.g., to use scale-factor-specific subdirectories like `tpcds-data/1T` or `tpcds-data/10T`).

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

### Cluster and Executor Configuration

All cluster and executor settings are **explicitly configured** in `conf.yaml`. No auto-calculation is performed - you are responsible for ensuring resources fit within cluster capacity.

**Cluster-level settings:**
```yaml
dataproc:
  num_masters: 1          # 1 for standard, 3 for high-availability
  num_workers: 4          # 4 workers × 2 executors = 8 executors
  # Master and worker use same machine type (n2-standard-8) with local SSD
  master_machine_type: "n2-standard-8"
  worker_machine_type: "n2-standard-8"
```

**Executor and Driver settings:**
```yaml
spark_properties:
  # Executors (8 total, 2 per worker)
  "spark.executor.instances": "8"
  "spark.executor.cores": "4"
  "spark.executor.memory": "14g"
  "spark.executor.memoryOverhead": "1g"
  # Driver (runs on master node in client mode)
  "spark.driver.cores": "6"
  "spark.driver.memory": "24g"
  "spark.driver.memoryOverhead": "4g"
```

**Default resource allocation (1 master + 4 workers, all n2-standard-8):**

| Component | Location | Cores | Memory |
|-----------|----------|-------|--------|
| 1 Driver | Master node | 6 | 28GB |
| 8 Executors | Workers 1-4 (2 each) | 32 | 120GB |

**Key principles:**
- **Driver**: Runs on master node (client mode via Python API)
- **4-5 cores per executor**: Optimal for HDFS/GCS parallel I/O throughput
- **Memory under 32GB**: Enables JVM compressed OOPs (saves 5-15% memory)
- **Insufficient resources**: Job submission fails with YARN allocation error

### For 1TB Benchmarks
- Use at least 4-8 worker nodes
- Recommended: `n2-standard-8` workers with local SSD
- Enable Spark adaptive query execution (enabled by default)

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

# Clean all local files
make clean-local

# Or manually
rm -rf __pycache__ .pytest_cache tmp/
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
