"""TPC-DS Data Generation using Spark on Dataproc or Rust generator."""

import logging
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Dict

from google.cloud import dataproc_v1
from google.cloud import storage

logger = logging.getLogger(__name__)

# Path to the Rust datagen binary relative to project root
RUST_DATAGEN_BINARY = "datagen/target/release/tpcds-datagen"

# Path to the Spark datagen script relative to project root
SPARK_DATAGEN_SCRIPT = "datagen_spark/tpcds_datagen.py"

# TPC-DS table names
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


class DataGenerator:
    """Generates TPC-DS data using Spark on Dataproc or Rust generator."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize DataGenerator with configuration.

        Args:
            config: Full configuration dictionary from conf.yaml
        """
        self.config = config
        self.gcp_config = config["gcp"]
        self.dataproc_config = config["dataproc"]
        self.benchmark_config = config["benchmark"]

        self.project_id = self.gcp_config["project_id"]
        self.region = self.gcp_config["region"]
        self.staging_bucket = self.gcp_config["staging_bucket"].replace("gs://", "")
        self.cluster_name = self.dataproc_config["cluster_name"]

        self.scale_factor = self.benchmark_config["scale_factor"]
        self.data_format = self.benchmark_config["data_format"]
        self.compression = self.benchmark_config["format_compression"]
        self.data_path = self.benchmark_config["data_path"]

        # Initialize clients
        self.job_client = dataproc_v1.JobControllerClient(
            client_options={"api_endpoint": f"{self.region}-dataproc.googleapis.com:443"}
        )
        self.storage_client = storage.Client()

    def data_exists(self) -> bool:
        """Check if TPC-DS data already exists at the target path."""
        try:
            # Parse GCS path
            path = self.data_path.replace("gs://", "")
            bucket_name = path.split("/")[0]
            prefix = "/".join(path.split("/")[1:])

            bucket = self.storage_client.bucket(bucket_name)

            # Check if at least one table directory exists with data
            for table in TPCDS_TABLES[:3]:  # Quick check on first 3 tables
                blobs = list(bucket.list_blobs(prefix=f"{prefix}/{table}/", max_results=1))
                if not blobs:
                    return False

            logger.info(f"TPC-DS data already exists at {self.data_path}")
            return True

        except Exception as e:
            logger.debug(f"Error checking data existence: {e}")
            return False

    def list_tables(self) -> list:
        """List available TPC-DS tables in the data path."""
        try:
            path = self.data_path.replace("gs://", "")
            bucket_name = path.split("/")[0]
            prefix = "/".join(path.split("/")[1:]) + "/"

            bucket = self.storage_client.bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=prefix, delimiter="/")

            # Get directory names (table names)
            tables = []
            for page in blobs.pages:
                for prefix in page.prefixes:
                    table_name = prefix.rstrip("/").split("/")[-1]
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
                - exists: bool - whether data exists
                - total_size_bytes: int - total size in bytes
                - total_size_human: str - human-readable size
                - table_count: int - number of tables found
                - tables: list - table names with sizes
                - data_path: str - the data path
                - scale_factor: int - configured scale factor
        """
        try:
            path = self.data_path.replace("gs://", "")
            bucket_name = path.split("/")[0]
            prefix = "/".join(path.split("/")[1:])
            if prefix and not prefix.endswith("/"):
                prefix += "/"

            bucket = self.storage_client.bucket(bucket_name)

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

            # Convert to human-readable size
            def human_size(size_bytes: int) -> str:
                for unit in ["B", "KB", "MB", "GB", "TB"]:
                    if size_bytes < 1024:
                        return f"{size_bytes:.2f} {unit}"
                    size_bytes /= 1024
                return f"{size_bytes:.2f} PB"

            tables_with_sizes = [
                {"name": name, "size_bytes": size, "size_human": human_size(size)}
                for name, size in sorted(table_sizes.items())
            ]

            exists = len(table_sizes) > 0

            return {
                "exists": exists,
                "total_size_bytes": total_size,
                "total_size_human": human_size(total_size),
                "table_count": len(table_sizes),
                "tables": tables_with_sizes,
                "data_path": self.data_path,
                "scale_factor": self.scale_factor,
            }

        except Exception as e:
            logger.error(f"Error getting data stats: {e}")
            return {
                "exists": False,
                "total_size_bytes": 0,
                "total_size_human": "0 B",
                "table_count": 0,
                "tables": [],
                "data_path": self.data_path,
                "scale_factor": self.scale_factor,
                "error": str(e),
            }

    def _rust_datagen_available(self) -> bool:
        """Check if the Rust datagen binary is available."""
        project_root = Path(__file__).parent.parent
        binary_path = project_root / RUST_DATAGEN_BINARY
        return binary_path.exists() and os.access(binary_path, os.X_OK)

    def _build_rust_datagen(self) -> bool:
        """Build the Rust datagen binary if not already built.

        Returns:
            True if build succeeded or binary already exists, False otherwise
        """
        if self._rust_datagen_available():
            logger.info("Rust datagen binary already exists")
            return True

        project_root = Path(__file__).parent.parent
        datagen_dir = project_root / "datagen"

        if not datagen_dir.exists():
            logger.error("Rust datagen source directory not found")
            return False

        logger.info("Building Rust datagen binary (release mode)...")

        try:
            result = subprocess.run(
                ["cargo", "build", "--release"],
                cwd=datagen_dir,
                capture_output=True,
                text=True,
            )

            if result.returncode == 0:
                logger.info("Rust datagen build completed successfully")
                return True
            else:
                logger.error(f"Rust datagen build failed: {result.stderr}")
                return False

        except FileNotFoundError:
            logger.error("Rust toolchain (cargo) not found. Please install Rust.")
            return False
        except Exception as e:
            logger.error(f"Error building Rust datagen: {e}")
            return False

    def generate_data_rust(self, config_path: str = "conf.yaml", verbose: bool = False) -> Dict[str, Any]:
        """Generate TPC-DS data using the high-performance Rust generator.

        Args:
            config_path: Path to the configuration file
            verbose: Enable verbose logging

        Returns:
            Dictionary with generation result information
        """
        if self.config["benchmark"].get("skip_data_gen", False):
            logger.info("Data generation skipped (skip_data_gen=true)")
            return {"status": "SKIPPED", "message": "Data generation skipped"}

        if self.data_exists():
            logger.info("Data already exists, skipping generation")
            return {"status": "SKIPPED", "message": "Data already exists"}

        if not self._rust_datagen_available():
            logger.error("Rust datagen binary not found. Run 'make datagen-build' first.")
            return {
                "status": "FAILED",
                "error": "Rust datagen binary not available. Build with 'make datagen-build'"
            }

        project_root = Path(__file__).parent.parent
        binary_path = project_root / RUST_DATAGEN_BINARY

        logger.info(f"Generating TPC-DS data using Rust generator (scale={self.scale_factor}GB)...")

        # Build command
        cmd = [str(binary_path), "--config", config_path]
        if verbose:
            cmd.append("--verbose")

        try:
            start_time = time.time()

            # Run the Rust datagen
            result = subprocess.run(
                cmd,
                cwd=project_root,
                capture_output=True,
                text=True,
            )

            elapsed_time = time.time() - start_time

            if result.returncode == 0:
                logger.info(f"Rust data generation completed in {elapsed_time:.2f}s")
                logger.info(result.stdout)

                return {
                    "status": "COMPLETED",
                    "generator": "rust",
                    "data_path": self.data_path,
                    "scale_factor": self.scale_factor,
                    "elapsed_seconds": elapsed_time,
                    "output": result.stdout,
                }
            else:
                logger.error(f"Rust data generation failed: {result.stderr}")
                return {
                    "status": "FAILED",
                    "generator": "rust",
                    "error": result.stderr,
                    "returncode": result.returncode,
                }

        except Exception as e:
            logger.error(f"Rust data generation error: {e}")
            return {
                "status": "FAILED",
                "generator": "rust",
                "error": str(e),
            }

    def generate_data_spark(self) -> Dict[str, Any]:
        """Generate TPC-DS data using PySpark on Dataproc cluster.

        This method submits a PySpark job to the Dataproc cluster to generate
        TPC-DS data in a distributed manner. The job is submitted to the
        benchmark cluster configured in conf.yaml.

        Returns:
            Dictionary with generation result information
        """
        if self.config["benchmark"].get("skip_data_gen", False):
            logger.info("Data generation skipped (skip_data_gen=true)")
            return {"status": "SKIPPED", "message": "Data generation skipped"}

        if self.data_exists():
            logger.info("Data already exists, skipping generation")
            return {"status": "SKIPPED", "message": "Data already exists"}

        logger.info(f"Generating TPC-DS data using Spark on cluster '{self.cluster_name}' "
                   f"(scale={self.scale_factor}GB)...")

        # Upload the Spark datagen script to GCS
        project_root = Path(__file__).parent.parent
        script_path = project_root / SPARK_DATAGEN_SCRIPT

        if not script_path.exists():
            logger.error(f"Spark datagen script not found: {script_path}")
            return {
                "status": "FAILED",
                "error": f"Spark datagen script not found: {SPARK_DATAGEN_SCRIPT}"
            }

        # Read and upload script
        with open(script_path, "r") as f:
            script_content = f.read()

        bucket = self.storage_client.bucket(self.staging_bucket)
        blob_path = "scripts/tpcds_datagen_spark.py"
        blob = bucket.blob(blob_path)
        blob.upload_from_string(script_content)
        script_gcs_path = f"gs://{self.staging_bucket}/{blob_path}"

        logger.info(f"Uploaded Spark datagen script to {script_gcs_path}")

        # Get Spark settings from datagen config
        datagen_config = self.config.get("datagen", {})
        spark_settings = datagen_config.get("spark", {})
        parallelism = spark_settings.get("parallelism", 0)

        # Submit PySpark job to the benchmark cluster
        job = {
            "placement": {"cluster_name": self.cluster_name},
            "pyspark_job": {
                "main_python_file_uri": script_gcs_path,
                "args": [
                    "--scale-factor", str(self.scale_factor),
                    "--output-path", self.data_path,
                    "--format", self.data_format,
                    "--compression", self.compression,
                    "--parallelism", str(parallelism),
                ],
                "properties": {
                    "spark.executor.memory": self.dataproc_config["spark_properties"].get(
                        "spark.executor.memory", "6g"
                    ),
                    "spark.executor.cores": self.dataproc_config["spark_properties"].get(
                        "spark.executor.cores", "2"
                    ),
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                },
            },
        }

        try:
            start_time = time.time()

            operation = self.job_client.submit_job_as_operation(
                project_id=self.project_id,
                region=self.region,
                job=job,
            )

            logger.info("Waiting for Spark data generation job to complete...")
            result = operation.result()

            elapsed_time = time.time() - start_time
            job_id = result.reference.job_id
            status = result.status.state.name

            logger.info(f"Spark data generation job {job_id} completed with status: {status}")
            logger.info(f"Elapsed time: {elapsed_time:.2f}s")

            return {
                "status": status,
                "generator": "spark",
                "job_id": job_id,
                "data_path": self.data_path,
                "scale_factor": self.scale_factor,
                "elapsed_seconds": elapsed_time,
            }

        except Exception as e:
            logger.error(f"Spark data generation failed: {e}")
            return {
                "status": "FAILED",
                "generator": "spark",
                "error": str(e),
            }

    def generate_data_auto(self, config_path: str = "conf.yaml") -> Dict[str, Any]:
        """Generate TPC-DS data using the configured engine.

        The engine is determined by the 'datagen.engine' configuration:
        - "spark" (default): Use distributed PySpark generation on Dataproc cluster
        - "rust": Use the high-performance Rust generator (requires Rust toolchain)

        The Spark engine submits jobs to the benchmark cluster, so ensure the
        cluster is created before calling this method.

        Args:
            config_path: Path to the configuration file

        Returns:
            Dictionary with generation result information
        """
        datagen_config = self.config.get("datagen", {})
        engine = datagen_config.get("engine", "spark").lower()

        if engine == "spark":
            logger.info("Using Spark data generator (configured engine: spark)")
            return self.generate_data_spark()

        elif engine == "rust":
            logger.info("Using Rust data generator (configured engine: rust)")

            # Build Rust binary if needed
            if not self._build_rust_datagen():
                return {
                    "status": "FAILED",
                    "error": "Failed to build Rust datagen. Check Rust toolchain installation."
                }

            return self.generate_data_rust(config_path)

        else:
            logger.error(f"Unknown datagen engine: {engine}. Use 'spark' or 'rust'.")
            return {
                "status": "FAILED",
                "error": f"Unknown datagen engine: {engine}. Valid options: 'spark', 'rust'"
            }
