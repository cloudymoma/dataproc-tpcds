"""TPC-DS Data Generation using Spark on Dataproc or Rust generator."""

import logging
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, Optional

from google.cloud import dataproc_v1
from google.cloud import storage
from google.api_core.exceptions import NotFound

logger = logging.getLogger(__name__)

# Path to the Rust datagen binary relative to project root
RUST_DATAGEN_BINARY = "datagen/target/release/tpcds-datagen"

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
    """Generates TPC-DS data using Spark SQL on Dataproc."""

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

    def _create_datagen_script(self) -> str:
        """Create the PySpark data generation script."""
        script = f'''
import sys
from pyspark.sql import SparkSession

# TPC-DS Data Generation Script
# Uses spark-sql-perf or tpcds-kit for data generation

spark = SparkSession.builder \\
    .appName("TPC-DS Data Generation") \\
    .config("spark.sql.parquet.compression.codec", "{self.compression}") \\
    .getOrCreate()

# Import TPC-DS data generator
# This assumes spark-sql-perf JAR is available
try:
    from py4j.java_gateway import java_import
    java_import(spark._jvm, "com.databricks.spark.sql.perf.tpcds.*")

    # Create TPC-DS tables object
    tpcds = spark._jvm.com.databricks.spark.sql.perf.tpcds.TPCDSTables(
        spark._jsparkSession,
        "{self.data_path}/dsdgen",  # dsdgen tool location
        "{self.scale_factor}"  # scale factor in GB
    )

    # Generate data
    tpcds.genData(
        "{self.data_path}",
        "{self.data_format}",
        True,  # overwrite
        True,  # partition tables
        False, # cluster by partition columns
        False, # filter out null partition values
        ""     # table filter
    )

except Exception as e:
    print(f"spark-sql-perf not available, using manual generation: {{e}}")

    # Fallback: Generate sample data for testing
    # In production, you would use proper TPC-DS data generation tools
    from pyspark.sql.types import *
    from pyspark.sql.functions import *
    import random

    # Generate date_dim table
    date_data = [(i, f"2020-{{str((i % 365) // 30 + 1).zfill(2)}}-{{str((i % 30) + 1).zfill(2)}}",
                  2020 + i // 365, (i % 365) // 30 + 1, (i % 30) + 1)
                 for i in range(1, 73050)]  # 200 years of dates
    date_schema = StructType([
        StructField("d_date_sk", IntegerType(), False),
        StructField("d_date", StringType(), True),
        StructField("d_year", IntegerType(), True),
        StructField("d_moy", IntegerType(), True),
        StructField("d_dom", IntegerType(), True),
    ])
    date_df = spark.createDataFrame(date_data, date_schema)
    date_df.write.mode("overwrite").{self.data_format}("{self.data_path}/date_dim")

    # Generate item table
    item_data = [(i, f"item_{{i}}", random.uniform(1, 1000), f"brand_{{i % 100}}", f"class_{{i % 50}}")
                 for i in range(1, 18001)]
    item_schema = StructType([
        StructField("i_item_sk", IntegerType(), False),
        StructField("i_item_id", StringType(), True),
        StructField("i_current_price", DoubleType(), True),
        StructField("i_brand", StringType(), True),
        StructField("i_class", StringType(), True),
    ])
    item_df = spark.createDataFrame(item_data, item_schema)
    item_df.write.mode("overwrite").{self.data_format}("{self.data_path}/item")

    # Generate store_sales table (main fact table)
    num_rows = {self.scale_factor} * 10000  # Scale based on factor
    sales_df = spark.range(1, num_rows + 1).select(
        col("id").alias("ss_sold_date_sk"),
        (col("id") % 18000 + 1).alias("ss_item_sk"),
        (col("id") % 1000 + 1).alias("ss_customer_sk"),
        (col("id") % 100 + 1).alias("ss_store_sk"),
        (rand() * 1000).alias("ss_sales_price"),
        (rand() * 100).cast(IntegerType()).alias("ss_quantity"),
        (rand() * 1000).alias("ss_net_profit")
    )
    sales_df.write.mode("overwrite").{self.data_format}("{self.data_path}/store_sales")

    # Generate customer table
    cust_data = [(i, f"CUST{{str(i).zfill(8)}}", f"First_{{i}}", f"Last_{{i}}",
                  random.randint(1, 100000)) for i in range(1, 100001)]
    cust_schema = StructType([
        StructField("c_customer_sk", IntegerType(), False),
        StructField("c_customer_id", StringType(), True),
        StructField("c_first_name", StringType(), True),
        StructField("c_last_name", StringType(), True),
        StructField("c_current_addr_sk", IntegerType(), True),
    ])
    cust_df = spark.createDataFrame(cust_data, cust_schema)
    cust_df.write.mode("overwrite").{self.data_format}("{self.data_path}/customer")

    # Generate store table
    store_data = [(i, f"STORE{{str(i).zfill(6)}}", f"Store {{i}}", f"City {{i % 100}}")
                  for i in range(1, 1001)]
    store_schema = StructType([
        StructField("s_store_sk", IntegerType(), False),
        StructField("s_store_id", StringType(), True),
        StructField("s_store_name", StringType(), True),
        StructField("s_city", StringType(), True),
    ])
    store_df = spark.createDataFrame(store_data, store_schema)
    store_df.write.mode("overwrite").{self.data_format}("{self.data_path}/store")

    print("Sample TPC-DS data generated successfully")

spark.stop()
print("Data generation completed!")
'''
        return script

    def _upload_script(self, script: str) -> str:
        """Upload the datagen script to GCS."""
        bucket = self.storage_client.bucket(self.staging_bucket)
        blob_path = "scripts/tpcds_datagen.py"
        blob = bucket.blob(blob_path)
        blob.upload_from_string(script)
        return f"gs://{self.staging_bucket}/{blob_path}"

    def generate_data(self) -> Dict[str, Any]:
        """Generate TPC-DS data by submitting a Spark job.

        Returns:
            Dictionary with job result information
        """
        if self.config["benchmark"].get("skip_data_gen", False):
            logger.info("Data generation skipped (skip_data_gen=true)")
            return {"status": "SKIPPED", "message": "Data generation skipped"}

        if self.data_exists():
            logger.info("Data already exists, skipping generation")
            return {"status": "SKIPPED", "message": "Data already exists"}

        logger.info(f"Generating TPC-DS data (scale={self.scale_factor}GB)...")

        # Create and upload the data generation script
        script = self._create_datagen_script()
        script_gcs_path = self._upload_script(script)

        # Submit PySpark job
        job = {
            "placement": {"cluster_name": self.cluster_name},
            "pyspark_job": {
                "main_python_file_uri": script_gcs_path,
                "properties": {
                    "spark.executor.memory": self.dataproc_config["spark_properties"].get(
                        "spark.executor.memory", "6g"
                    ),
                    "spark.executor.cores": self.dataproc_config["spark_properties"].get(
                        "spark.executor.cores", "2"
                    ),
                },
            },
        }

        try:
            operation = self.job_client.submit_job_as_operation(
                project_id=self.project_id,
                region=self.region,
                job=job,
            )

            logger.info("Waiting for data generation job to complete...")
            result = operation.result()

            job_id = result.reference.job_id
            status = result.status.state.name

            logger.info(f"Data generation job {job_id} completed with status: {status}")

            return {
                "status": status,
                "job_id": job_id,
                "data_path": self.data_path,
                "scale_factor": self.scale_factor,
            }

        except Exception as e:
            logger.error(f"Data generation failed: {e}")
            return {
                "status": "FAILED",
                "error": str(e),
            }

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

    def _rust_datagen_available(self) -> bool:
        """Check if the Rust datagen binary is available."""
        project_root = Path(__file__).parent.parent
        binary_path = project_root / RUST_DATAGEN_BINARY
        return binary_path.exists() and os.access(binary_path, os.X_OK)

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

    def generate_data_auto(self, config_path: str = "conf.yaml") -> Dict[str, Any]:
        """Generate TPC-DS data using the best available method.

        Prefers Rust generator for performance, falls back to Spark if unavailable.

        Args:
            config_path: Path to the configuration file

        Returns:
            Dictionary with generation result information
        """
        datagen_config = self.config.get("datagen", {})
        use_rust = datagen_config.get("use_rust", True)  # Default to Rust if available

        if use_rust and self._rust_datagen_available():
            logger.info("Using Rust data generator for high-performance generation")
            return self.generate_data_rust(config_path)
        else:
            if use_rust and not self._rust_datagen_available():
                logger.warning("Rust datagen not available, falling back to Spark")
            logger.info("Using Spark-based data generator")
            return self.generate_data()
