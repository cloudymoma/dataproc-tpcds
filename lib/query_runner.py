"""TPC-DS Query Execution with Temporary Views (No Metastore)."""

import logging
import os
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from google.cloud import dataproc_v1
from google.cloud import storage

logger = logging.getLogger(__name__)


class QueryRunner:
    """Runs TPC-DS queries using Spark SQL with Temporary Views."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize QueryRunner with configuration.

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

        self.data_format = self.benchmark_config["data_format"]
        self.data_path = self.benchmark_config["data_path"]
        self.iterations = self.benchmark_config.get("iterations", 1)

        # Initialize clients
        self.job_client = dataproc_v1.JobControllerClient(
            client_options={"api_endpoint": f"{self.region}-dataproc.googleapis.com:443"}
        )
        self.storage_client = storage.Client()

        # SQL directory
        self.sql_dir = Path(__file__).parent.parent / "sql"

    def _list_tables_from_gcs(self) -> List[str]:
        """List available table directories in GCS data path."""
        try:
            path = self.data_path.replace("gs://", "")
            bucket_name = path.split("/")[0]
            prefix = "/".join(path.split("/")[1:]) + "/"

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

    def _create_temp_view_script(self, tables: List[str], query_sql: str) -> str:
        """Create PySpark script that registers temp views and runs query."""
        view_registrations = []
        for table in tables:
            table_path = f"{self.data_path}/{table}"
            view_registrations.append(
                f'spark.read.{self.data_format}("{table_path}").createOrReplaceTempView("{table}")'
            )

        views_code = "\n    ".join(view_registrations)

        script = f'''
import time
import json
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \\
    .appName("TPC-DS Query Execution") \\
    .config("spark.sql.adaptive.enabled", "true") \\
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
    .getOrCreate()

# Register Temporary Views for all TPC-DS tables
print("Registering temporary views...")
start_view_time = time.time()
try:
    {views_code}
except Exception as e:
    print(f"Error registering views: {{e}}")
    raise

view_registration_time = time.time() - start_view_time
print(f"Views registered in {{view_registration_time:.2f}} seconds")

# Execute the TPC-DS query
query = """
{query_sql}
"""

print("Executing query...")
start_query_time = time.time()

try:
    result = spark.sql(query)
    row_count = result.count()
    query_time = time.time() - start_query_time

    # Collect metrics
    metrics = {{
        "status": "SUCCESS",
        "duration_sec": query_time,
        "view_registration_sec": view_registration_time,
        "row_count": row_count,
    }}

    # Try to get Spark metrics
    try:
        sc = spark.sparkContext
        status = sc.statusTracker()
        # Get accumulator values if available
        metrics["spark_version"] = sc.version
    except:
        pass

    print(f"Query completed in {{query_time:.2f}} seconds")
    print(f"Result row count: {{row_count}}")
    print(f"METRICS_JSON:{{json.dumps(metrics)}}")

except Exception as e:
    query_time = time.time() - start_query_time
    error_msg = str(e)
    print(f"Query failed: {{error_msg}}")
    metrics = {{
        "status": "FAILED",
        "duration_sec": query_time,
        "error_message": error_msg[:1000],
    }}
    print(f"METRICS_JSON:{{json.dumps(metrics)}}")
    raise

spark.stop()
'''
        return script

    def _upload_script(self, script: str, query_name: str) -> str:
        """Upload query script to GCS."""
        bucket = self.storage_client.bucket(self.staging_bucket)
        blob_path = f"scripts/query_{query_name}_{uuid.uuid4().hex[:8]}.py"
        blob = bucket.blob(blob_path)
        blob.upload_from_string(script)
        return f"gs://{self.staging_bucket}/{blob_path}"

    def _get_query_list(self) -> List[str]:
        """Get list of queries to run based on configuration."""
        queries_config = self.benchmark_config.get("queries_to_run", "all")

        if queries_config == "all":
            # Get all SQL files from sql directory
            if self.sql_dir.exists():
                sql_files = sorted(self.sql_dir.glob("q*.sql"))
                return [f.stem for f in sql_files]
            else:
                # Default to q1-q99
                return [f"q{i}" for i in range(1, 100)]
        elif isinstance(queries_config, list):
            return [f"q{q}" if isinstance(q, int) else q for q in queries_config]
        else:
            return [queries_config]

    def _load_query_sql(self, query_name: str) -> Optional[str]:
        """Load SQL content for a query."""
        sql_file = self.sql_dir / f"{query_name}.sql"
        if sql_file.exists():
            return sql_file.read_text()
        else:
            logger.warning(f"SQL file not found: {sql_file}")
            return None

    def run_query(
        self, query_name: str, query_sql: str, iteration: int = 1
    ) -> Dict[str, Any]:
        """Run a single TPC-DS query.

        Args:
            query_name: Name of the query (e.g., 'q1')
            query_sql: SQL content to execute
            iteration: Current iteration number

        Returns:
            Dictionary with query execution results
        """
        logger.info(f"Running query {query_name} (iteration {iteration})...")

        # Get available tables
        tables = self._list_tables_from_gcs()
        if not tables:
            return {
                "query_name": query_name,
                "iteration": iteration,
                "status": "FAILED",
                "error_message": "No tables found in data path",
            }

        # Create and upload the query script
        script = self._create_temp_view_script(tables, query_sql)
        script_gcs_path = self._upload_script(script, query_name)

        # Submit PySpark job
        job = {
            "placement": {"cluster_name": self.cluster_name},
            "pyspark_job": {
                "main_python_file_uri": script_gcs_path,
                "properties": self.dataproc_config.get("spark_properties", {}),
            },
        }

        start_time = time.time()

        try:
            operation = self.job_client.submit_job_as_operation(
                project_id=self.project_id,
                region=self.region,
                job=job,
            )

            result = operation.result()
            duration = time.time() - start_time

            job_id = result.reference.job_id
            status = result.status.state.name

            # Get job details for metrics
            job_details = self.job_client.get_job(
                project_id=self.project_id,
                region=self.region,
                job_id=job_id,
            )

            # Extract metrics from driver output if available
            metrics = self._parse_job_metrics(job_details)

            return {
                "query_name": query_name,
                "iteration": iteration,
                "status": status,
                "job_id": job_id,
                "duration_sec": duration,
                **metrics,
            }

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Query {query_name} failed: {e}")
            return {
                "query_name": query_name,
                "iteration": iteration,
                "status": "FAILED",
                "duration_sec": duration,
                "error_message": str(e)[:1000],
            }

    def _parse_job_metrics(self, job_details) -> Dict[str, Any]:
        """Parse metrics from job details."""
        metrics = {}

        try:
            # Get YARN application metrics if available
            if hasattr(job_details, "yarn_applications") and job_details.yarn_applications:
                app = job_details.yarn_applications[0]
                metrics["tracking_url"] = getattr(app, "tracking_url", None)

            # Get driver output URI and parse METRICS_JSON
            if hasattr(job_details, "driver_output_resource_uri"):
                driver_uri = job_details.driver_output_resource_uri
                metrics["driver_output_uri"] = driver_uri

        except Exception as e:
            logger.debug(f"Error parsing job metrics: {e}")

        return metrics

    def run_all_queries(self, batch_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Run all configured TPC-DS queries.

        Args:
            batch_id: Optional batch identifier for grouping results

        Returns:
            List of query execution results
        """
        if batch_id is None:
            batch_id = uuid.uuid4().hex

        queries = self._get_query_list()
        results = []

        logger.info(f"Running {len(queries)} queries with {self.iterations} iteration(s)")
        logger.info(f"Batch ID: {batch_id}")

        for iteration in range(1, self.iterations + 1):
            logger.info(f"=== Iteration {iteration}/{self.iterations} ===")

            for query_name in queries:
                query_sql = self._load_query_sql(query_name)
                if query_sql is None:
                    results.append({
                        "batch_id": batch_id,
                        "query_name": query_name,
                        "iteration": iteration,
                        "status": "SKIPPED",
                        "error_message": "SQL file not found",
                    })
                    continue

                result = self.run_query(query_name, query_sql, iteration)
                result["batch_id"] = batch_id
                results.append(result)

                # Log progress
                status = result.get("status", "UNKNOWN")
                duration = result.get("duration_sec", 0)
                logger.info(f"  {query_name}: {status} ({duration:.2f}s)")

        return results
