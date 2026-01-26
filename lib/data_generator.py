"""TPC-DS Data Generation using spark-sql-perf on Dataproc.

This module generates TPC-DS data using the Databricks spark-sql-perf library
with the native dsdgen binary for full TPC-DS compliance.
"""

import logging
import time
from pathlib import Path
from typing import Any, Dict, Tuple

from google.cloud import dataproc_v1
from google.cloud import storage

logger = logging.getLogger(__name__)

# All 24 TPC-DS tables
TPCDS_TABLES = [
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
]

# Default asset versions
DEFAULT_SPARK_SQL_PERF_VERSION = "0.5.1"
DEFAULT_TPCDS_DATAGEN_VERSION = "1.0.0"
DEFAULT_TPCDS_KIT_VERSION = "1.0.0"


def _human_size(size_bytes: int) -> str:
    """Convert bytes to human-readable size string."""
    size = float(size_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"


def _parse_gcs_path(gcs_path: str) -> Tuple[str, str]:
    """Parse GCS path into bucket name and prefix.

    Args:
        gcs_path: GCS path like "gs://bucket/path/to/data"

    Returns:
        Tuple of (bucket_name, prefix)
    """
    path = gcs_path.replace("gs://", "")
    parts = path.split("/", 1)
    bucket_name = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket_name, prefix


class DataGenerator:
    """Generates TPC-DS data using spark-sql-perf on Dataproc."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize DataGenerator with configuration.

        Args:
            config: Full configuration dictionary from conf.yaml
        """
        self.config = config
        self.gcp_config = config["gcp"]
        self.dataproc_config = config["dataproc"]
        self.benchmark_config = config["benchmark"]
        self.datagen_config = config.get("datagen", {})

        self.project_id = self.gcp_config["project_id"]
        self.region = self.gcp_config["region"]
        self.staging_bucket = self.gcp_config["staging_bucket"].replace("gs://", "")
        self.cluster_name = self.dataproc_config["cluster_name"]

        self.scale_factor = self.benchmark_config["scale_factor"]
        self.data_format = self.benchmark_config.get("data_format", "parquet")
        self.compression = self.benchmark_config.get("format_compression", "snappy")
        self.data_path = self.benchmark_config["data_path"]

        # Initialize clients
        self.job_client = dataproc_v1.JobControllerClient(
            client_options={"api_endpoint": f"{self.region}-dataproc.googleapis.com:443"}
        )
        self.storage_client = storage.Client()

    def check_data_exists(self) -> Tuple[bool, int]:
        """Check if TPC-DS data generation completed successfully.

        Checks both _SUCCESS marker and table count to detect partial failures.

        Returns:
            Tuple of (is_complete: bool, table_count: int)
        """
        try:
            bucket_name, prefix = _parse_gcs_path(self.data_path)
            if prefix and not prefix.endswith("/"):
                prefix += "/"

            bucket = self.storage_client.bucket(bucket_name)

            # Check _SUCCESS marker
            success_blob = bucket.blob(f"{prefix}_SUCCESS")
            success_exists = success_blob.exists()

            # Count table directories
            blobs = bucket.list_blobs(prefix=prefix, delimiter="/")
            table_count = 0
            for page in blobs.pages:
                for p in page.prefixes:
                    table_name = p.rstrip("/").split("/")[-1]
                    if table_name in TPCDS_TABLES:
                        table_count += 1

            # Data is complete only if _SUCCESS exists AND all 24 tables present
            is_complete = success_exists and table_count >= 24

            if is_complete:
                logger.info(f"TPC-DS data exists at {self.data_path} ({table_count} tables)")
            elif table_count > 0:
                logger.warning(
                    f"Partial data found at {self.data_path}: {table_count}/24 tables, "
                    f"_SUCCESS={'Yes' if success_exists else 'No'}"
                )

            return is_complete, table_count

        except Exception as e:
            logger.debug(f"Error checking data existence: {e}")
            return False, 0

    def data_exists(self) -> bool:
        """Check if TPC-DS data already exists at the target path.

        This is a simple boolean wrapper around check_data_exists().
        """
        is_complete, _ = self.check_data_exists()
        return is_complete

    def list_tables(self) -> list:
        """List available TPC-DS tables in the data path."""
        try:
            bucket_name, prefix = _parse_gcs_path(self.data_path)
            if prefix and not prefix.endswith("/"):
                prefix += "/"

            bucket = self.storage_client.bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=prefix, delimiter="/")

            tables = []
            for page in blobs.pages:
                for p in page.prefixes:
                    table_name = p.rstrip("/").split("/")[-1]
                    if table_name and not table_name.startswith("_"):
                        tables.append(table_name)

            return sorted(tables)

        except Exception as e:
            logger.error(f"Error listing tables: {e}")
            return []

    def get_data_stats(self) -> Dict[str, Any]:
        """Get statistics about the TPC-DS data at the target path.

        Returns:
            Dictionary with:
                - exists: bool - whether data is complete
                - is_complete: bool - same as exists
                - total_size_bytes: int - total size in bytes
                - total_size_human: str - human-readable size
                - table_count: int - number of tables found
                - tables: list - table names with sizes
                - data_path: str - the data path
                - scale_factor: int - configured scale factor
                - has_success_marker: bool - whether _SUCCESS file exists
        """
        try:
            bucket_name, prefix = _parse_gcs_path(self.data_path)
            if prefix and not prefix.endswith("/"):
                prefix += "/"

            bucket = self.storage_client.bucket(bucket_name)

            # Check _SUCCESS marker
            success_blob = bucket.blob(f"{prefix}_SUCCESS")
            has_success_marker = success_blob.exists()

            # Get all blobs under the data path
            blobs = bucket.list_blobs(prefix=prefix)

            total_size = 0
            table_sizes: Dict[str, int] = {}

            for blob in blobs:
                total_size += blob.size
                # Extract table name from path
                relative_path = blob.name[len(prefix):]
                if "/" in relative_path:
                    table_name = relative_path.split("/")[0]
                    if table_name and not table_name.startswith("_"):
                        table_sizes[table_name] = table_sizes.get(table_name, 0) + blob.size

            tables_with_sizes = [
                {"name": name, "size_bytes": size, "size_human": _human_size(size)}
                for name, size in sorted(table_sizes.items())
            ]

            table_count = len(table_sizes)
            is_complete = has_success_marker and table_count >= 24

            return {
                "exists": is_complete,
                "is_complete": is_complete,
                "has_success_marker": has_success_marker,
                "total_size_bytes": total_size,
                "total_size_human": _human_size(total_size),
                "table_count": table_count,
                "tables": tables_with_sizes,
                "data_path": self.data_path,
                "scale_factor": self.scale_factor,
            }

        except Exception as e:
            logger.error(f"Error getting data stats: {e}")
            return {
                "exists": False,
                "is_complete": False,
                "has_success_marker": False,
                "total_size_bytes": 0,
                "total_size_human": "0 B",
                "table_count": 0,
                "tables": [],
                "data_path": self.data_path,
                "scale_factor": self.scale_factor,
                "error": str(e),
            }

    def _upload_assets(self) -> Dict[str, str]:
        """Upload versioned assets to GCS staging bucket.

        Returns:
            Dict with GCS paths to uploaded assets:
                - datagen_jar: GCS path to tpcds-datagen JAR (main class)
                - perf_jar: GCS path to spark-sql-perf JAR (library)
                - kit: GCS path to tpcds-kit tarball

        Raises:
            FileNotFoundError: If required assets are not found locally
        """
        bucket = self.storage_client.bucket(self.staging_bucket)

        perf_jar_version = self.datagen_config.get(
            "spark_sql_perf_version", DEFAULT_SPARK_SQL_PERF_VERSION
        )
        datagen_jar_version = self.datagen_config.get(
            "tpcds_datagen_version", DEFAULT_TPCDS_DATAGEN_VERSION
        )
        kit_version = self.datagen_config.get(
            "tpcds_kit_version", DEFAULT_TPCDS_KIT_VERSION
        )

        project_root = Path(__file__).parent.parent
        assets_dir = project_root / "assets"

        assets = {
            "datagen_jar": {
                "local": assets_dir / f"tpcds-datagen-{datagen_jar_version}.jar",
                "gcs": f"lib/tpcds-datagen-{datagen_jar_version}.jar",
            },
            "perf_jar": {
                "local": assets_dir / f"spark-sql-perf-assembly-{perf_jar_version}.jar",
                "gcs": f"lib/spark-sql-perf-assembly-{perf_jar_version}.jar",
            },
            "kit": {
                "local": assets_dir / f"tpcds-kit-{kit_version}.tar.gz",
                "gcs": f"lib/tpcds-kit-{kit_version}.tar.gz",
            },
        }

        gcs_paths = {}

        for name, paths in assets.items():
            local_path = paths["local"]
            gcs_path = paths["gcs"]

            if not local_path.exists():
                raise FileNotFoundError(
                    f"Asset not found: {local_path}\n"
                    f"Run 'make build-assets' to build required assets, "
                    f"or download pre-built assets."
                )

            blob = bucket.blob(gcs_path)

            # Skip upload if already exists with same size
            if blob.exists():
                blob.reload()
                if blob.size == local_path.stat().st_size:
                    logger.info(f"Asset already uploaded: gs://{self.staging_bucket}/{gcs_path}")
                    gcs_paths[name] = f"gs://{self.staging_bucket}/{gcs_path}"
                    continue

            logger.info(f"Uploading {local_path.name} to gs://{self.staging_bucket}/{gcs_path}")
            blob.upload_from_filename(str(local_path))
            gcs_paths[name] = f"gs://{self.staging_bucket}/{gcs_path}"

        return gcs_paths

    def _calculate_optimal_partitions(self) -> int:
        """Calculate optimal partition count based on target output file size.

        Strategy: Target ~128MB Parquet files for optimal GCS and query performance.
        Formula: scale_factor * 8 (1 GB raw data -> ~8 partitions after compression)

        Returns:
            Optimal number of partitions
        """
        # Check for user override
        user_partitions = self.datagen_config.get("num_partitions", 0)
        if user_partitions > 0:
            return user_partitions

        # Target ~128MB files: 1 GB raw data -> ~8 partitions
        optimal = self.scale_factor * 8

        # Clamp between 100 (minimum for parallelism) and 50000 (avoid excessive overhead)
        return max(min(optimal, 50000), 100)

    def _write_success_marker(self) -> None:
        """Write _SUCCESS marker file after successful generation."""
        try:
            bucket_name, prefix = _parse_gcs_path(self.data_path)
            if prefix and not prefix.endswith("/"):
                prefix += "/"

            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(f"{prefix}_SUCCESS")
            blob.upload_from_string("")
            logger.info(f"Created _SUCCESS marker at {self.data_path}/_SUCCESS")
        except Exception as e:
            logger.warning(f"Failed to write _SUCCESS marker: {e}")

    def generate_data(self) -> Dict[str, Any]:
        """Generate TPC-DS data using spark-sql-perf on Dataproc.

        This method:
        1. Checks if data already exists (skips if complete)
        2. Uploads required assets (JAR and dsdgen binary)
        3. Submits a Spark job using spark-sql-perf GenTPCDSData
        4. Writes _SUCCESS marker on completion

        Returns:
            Dictionary with generation result information:
                - status: "DONE", "SKIPPED", or "FAILED"
                - job_id: Dataproc job ID (if submitted)
                - data_path: Target data path
                - scale_factor: Scale factor used
                - num_partitions: Number of partitions used
                - elapsed_seconds: Time taken (if completed)
                - error: Error message (if failed)
        """
        # Check if data already exists
        overwrite = self.datagen_config.get("overwrite", False)
        if not overwrite:
            is_complete, table_count = self.check_data_exists()
            if is_complete:
                logger.info(f"Data already exists ({table_count} tables). Skipping generation.")
                return {
                    "status": "SKIPPED",
                    "message": "Data already exists",
                    "table_count": table_count,
                }

        # Upload assets
        logger.info("Uploading data generation assets...")
        try:
            asset_paths = self._upload_assets()
        except FileNotFoundError as e:
            logger.error(str(e))
            return {"status": "FAILED", "error": str(e)}

        # Calculate optimal partitions
        num_partitions = self._calculate_optimal_partitions()
        logger.info(f"Using {num_partitions} partitions for data generation")

        # Build job configuration
        job_details = {
            "placement": {"cluster_name": self.cluster_name},
            "spark_job": {
                # Custom main class with proper entry point
                "main_class": "com.tpcds.TPCDSDataGen",
                # Both JARs needed: our main class JAR + spark-sql-perf library JAR
                "jar_file_uris": [
                    asset_paths["datagen_jar"],
                    asset_paths["perf_jar"],
                ],
                # Distribute native dsdgen binary to all workers
                "archive_uris": [f"{asset_paths['kit']}#tpcds_kit"],
                # Arguments for TPCDSDataGen
                "args": [
                    "--dsdgenDir", "./tpcds_kit/tools",
                    "--location", self.data_path,
                    "--scaleFactor", str(self.scale_factor),
                    "--format", self.data_format,
                    "--numPartitions", str(num_partitions),
                    "--partitionTables",
                    str(self.datagen_config.get("partition_tables", True)).lower(),
                    "--clusterByPartitionColumns",
                    str(self.datagen_config.get("cluster_by_partition_columns", True)).lower(),
                    "--filterOutNullPartitionValues",
                    str(self.datagen_config.get("filter_out_null_partition_values", False)).lower(),
                    "--useDoubleForDecimal",
                    str(self.datagen_config.get("use_double_for_decimal", False)).lower(),
                ],
                # Performance tuning properties
                "properties": {
                    # Parallelism settings
                    "spark.sql.shuffle.partitions": str(num_partitions),
                    "spark.default.parallelism": str(num_partitions),
                    # Compression
                    "spark.sql.parquet.compression.codec": self.compression,
                    # File size control
                    "spark.sql.files.maxRecordsPerFile": "1000000",
                    # Memory optimization
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                    # GCS optimization
                    "spark.hadoop.fs.gs.outputstream.upload.chunk.size": "134217728",
                },
            },
        }

        # Submit job
        try:
            start_time = time.time()

            logger.info(f"Submitting data generation job to cluster '{self.cluster_name}'...")
            logger.info(f"Scale factor: {self.scale_factor} GB, Format: {self.data_format}")

            operation = self.job_client.submit_job_as_operation(
                request={
                    "project_id": self.project_id,
                    "region": self.region,
                    "job": job_details,
                }
            )

            logger.info("Waiting for data generation to complete...")
            logger.info("This may take a while for large scale factors.")

            # Wait for job with configurable timeout (default 2 hours)
            job_timeout = self.datagen_config.get("job_timeout_seconds", 7200)
            result = operation.result(timeout=job_timeout)

            elapsed_time = time.time() - start_time
            job_id = result.reference.job_id
            status = result.status.state.name

            logger.info(f"Data generation completed. Job ID: {job_id}, Status: {status}")
            logger.info(f"Elapsed time: {elapsed_time:.1f}s ({elapsed_time/60:.1f} min)")

            # Write _SUCCESS marker on successful completion
            if status == "DONE":
                self._write_success_marker()

            return {
                "status": status,
                "job_id": job_id,
                "data_path": self.data_path,
                "scale_factor": self.scale_factor,
                "num_partitions": num_partitions,
                "elapsed_seconds": elapsed_time,
            }

        except Exception as e:
            logger.error(f"Data generation failed: {e}")

            # Try to get job ID for debugging
            job_id = "unknown"
            try:
                if hasattr(operation, "metadata"):
                    job_id = operation.metadata.job_id
            except Exception:
                pass

            logger.error(f"Check Dataproc logs for job: {job_id}")

            return {
                "status": "FAILED",
                "job_id": job_id,
                "error": str(e),
            }


# Convenience function for Makefile integration
def submit_datagen_job(config: Dict[str, Any]) -> Dict[str, Any]:
    """Submit TPC-DS data generation job.

    This is a convenience function for calling from Makefile.

    Args:
        config: Full configuration dictionary from conf.yaml

    Returns:
        Dictionary with generation result information
    """
    generator = DataGenerator(config)
    return generator.generate_data()


def check_data_exists(gcs_path: str) -> Tuple[bool, int]:
    """Check if TPC-DS data exists at the given path.

    This is a standalone function for quick checks without full config.

    Args:
        gcs_path: GCS path to check

    Returns:
        Tuple of (is_complete: bool, table_count: int)
    """
    try:
        client = storage.Client()
        bucket_name, prefix = _parse_gcs_path(gcs_path)
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        bucket = client.bucket(bucket_name)

        # Check _SUCCESS marker
        success_blob = bucket.blob(f"{prefix}_SUCCESS")
        success_exists = success_blob.exists()

        # Count table directories
        blobs = bucket.list_blobs(prefix=prefix, delimiter="/")
        table_count = 0
        for page in blobs.pages:
            for p in page.prefixes:
                table_name = p.rstrip("/").split("/")[-1]
                if table_name in TPCDS_TABLES:
                    table_count += 1

        is_complete = success_exists and table_count >= 24
        return is_complete, table_count

    except Exception as e:
        logger.debug(f"Error checking data existence: {e}")
        return False, 0


def get_data_stats(config: Dict[str, Any]) -> Dict[str, Any]:
    """Get statistics about TPC-DS data.

    This is a convenience function for calling from Makefile.

    Args:
        config: Full configuration dictionary from conf.yaml

    Returns:
        Dictionary with data statistics
    """
    generator = DataGenerator(config)
    return generator.get_data_stats()
