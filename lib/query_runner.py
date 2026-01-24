"""TPC-DS Query Execution with Temporary Views (No Metastore)."""

import logging
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

    # Collect basic metrics
    metrics = {{
        "status": "SUCCESS",
        "duration_sec": query_time,
        "view_registration_sec": view_registration_time,
        "row_count": row_count,
    }}

    # Collect Spark SQL execution metrics
    try:
        sc = spark.sparkContext
        metrics["spark_version"] = sc.version

        # Get metrics from the last SQL execution
        # Access the query execution metrics via internal API
        listener = spark._jsparkSession.sharedState().cacheManager()

        # Try to get metrics from StatusTracker
        status_tracker = sc.statusTracker()

        # Aggregate metrics from all completed jobs
        total_input_bytes = 0
        total_shuffle_read = 0
        total_shuffle_write = 0
        total_records = 0

        # Get job IDs and their stage info
        for job_id in status_tracker.getJobIdsForGroup():
            job_info = status_tracker.getJobInfo(job_id)
            if job_info:
                for stage_id in job_info.stageIds():
                    stage_info = status_tracker.getStageInfo(stage_id)
                    if stage_info:
                        # Note: These metrics might not be available via statusTracker
                        pass

        # Alternative: Get metrics from SparkContext accumulators
        # This works better for completed stages
        try:
            # Access Spark's internal metrics via listener
            from pyspark import SparkContext
            from py4j.java_gateway import java_import

            jvm = sc._jvm
            java_import(jvm, "org.apache.spark.sql.execution.ui.SQLAppStatusStore")

            # Get SQL metrics from the Spark UI store
            sql_store = spark._jsparkSession.sharedState().statusStore()
            executions = sql_store.executionsList()

            if executions and len(executions) > 0:
                # Get the most recent execution
                last_exec = executions[-1] if hasattr(executions, '__getitem__') else None
                if last_exec:
                    exec_id = last_exec.executionId()
                    exec_metrics = sql_store.executionMetrics(exec_id)

                    for metric in exec_metrics:
                        name = metric.name()
                        value = metric.metricValue()
                        if "scan" in name.lower() or "input" in name.lower():
                            if "bytes" in name.lower():
                                total_input_bytes += int(value) if value.isdigit() else 0
                            elif "rows" in name.lower():
                                total_records += int(value) if value.isdigit() else 0
                        elif "shuffle" in name.lower():
                            if "read" in name.lower() and "bytes" in name.lower():
                                total_shuffle_read += int(value) if value.isdigit() else 0
                            elif "write" in name.lower() and "bytes" in name.lower():
                                total_shuffle_write += int(value) if value.isdigit() else 0
        except Exception as metric_err:
            print(f"Could not collect detailed metrics: {{metric_err}}")

        # Only add metrics if we got meaningful values
        if total_input_bytes > 0:
            metrics["input_bytes"] = total_input_bytes
        if total_shuffle_read > 0:
            metrics["shuffle_read_bytes"] = total_shuffle_read
        if total_shuffle_write > 0:
            metrics["shuffle_write_bytes"] = total_shuffle_write
        if total_records > 0:
            metrics["records_read"] = total_records

    except Exception as e:
        print(f"Warning: Could not collect Spark metrics: {{e}}")

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
        """Parse metrics from job details and driver output.

        Extracts the METRICS_JSON line from the driver output to get
        detailed Spark execution metrics.
        """
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

                # Try to read driver output and extract METRICS_JSON
                try:
                    import json
                    import re

                    # Parse GCS URI
                    if driver_uri.startswith("gs://"):
                        path = driver_uri.replace("gs://", "")
                        bucket_name = path.split("/")[0]
                        blob_path = "/".join(path.split("/")[1:])

                        bucket = self.storage_client.bucket(bucket_name)
                        blob = bucket.blob(blob_path)

                        if blob.exists():
                            # Download and parse the driver output
                            output = blob.download_as_text()

                            # Find METRICS_JSON line
                            for line in output.split("\n"):
                                if line.startswith("METRICS_JSON:"):
                                    json_str = line[len("METRICS_JSON:"):]
                                    parsed_metrics = json.loads(json_str)

                                    # Copy relevant metrics
                                    for key in ["spark_version", "row_count", "view_registration_sec",
                                                 "input_bytes", "shuffle_read_bytes", "shuffle_write_bytes",
                                                 "records_read", "compile_time_sec"]:
                                        if key in parsed_metrics:
                                            metrics[key] = parsed_metrics[key]
                                    break

                except Exception as parse_err:
                    logger.debug(f"Could not parse driver output: {parse_err}")

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
