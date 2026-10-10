"""TPC-DS Query Execution with Temporary Views (No Metastore)."""

import logging
import re
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from google.cloud import dataproc_v1
from google.cloud import storage

from lib.cluster_manager import configure_gcp_credentials

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

        # Configure GCP credentials if specified in conf.yaml
        configure_gcp_credentials(self.gcp_config)

        # Initialize clients
        self.job_client = dataproc_v1.JobControllerClient(
            client_options={"api_endpoint": f"{self.region}-dataproc.googleapis.com:443"}
        )
        self.storage_client = storage.Client(project=self.project_id)

        # SQL directory and cached table list
        self.sql_dir = Path(__file__).parent.parent / "sql"
        self._tables_cache: Optional[List[str]] = None

    def _list_tables_from_gcs(self) -> List[str]:
        """List available table directories in GCS data path (cached after first lookup)."""
        if self._tables_cache is not None:
            return self._tables_cache

        try:
            path = self.data_path.replace("gs://", "")
            bucket_name = path.split("/")[0]
            prefix = "/".join(path.split("/")[1:]).rstrip("/") + "/"

            bucket = self.storage_client.bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=prefix, delimiter="/")

            tables = []
            for page in blobs.pages:
                for p in page.prefixes:
                    table_name = p.rstrip("/").split("/")[-1]
                    if table_name and not table_name.startswith("_"):
                        tables.append(table_name)

            sorted_tables = sorted(tables)
            if sorted_tables:
                self._tables_cache = sorted_tables
            return sorted_tables

        except Exception as e:
            logger.error(f"Error listing tables: {e}")
            return []

    def _create_temp_view_script(self, tables: List[str], query_sql: str) -> str:
        """Create PySpark script that registers temp views and runs query."""
        view_registrations = []
        for table in tables:
            table_path = f"{self.data_path.rstrip('/')}/{table}"
            view_registrations.append(
                f'spark.read.{self.data_format}("{table_path}").createOrReplaceTempView("{table}")'
            )

        views_code = "\n    ".join(view_registrations)

        script = f'''
import time
import json
import re
from pyspark.sql import SparkSession

# Initialize Spark Session (Spark properties are passed via Dataproc job config)
spark = SparkSession.builder \\
    .appName("TPC-DS Query Execution") \\
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

def _snapshot_stage_metrics(sc):
    """Snapshot cumulative stage metrics from Spark AppStatusStore."""
    totals = {{"input_bytes": 0, "records_read": 0, "shuffle_read_bytes": 0, "shuffle_write_bytes": 0}}
    try:
        app_store = sc._jsc.sc().statusStore()
        stages_it = app_store.stageList(None).iterator()
        while stages_it.hasNext():
            stage = stages_it.next()
            totals["input_bytes"] += int(stage.inputBytes())
            totals["records_read"] += int(stage.inputRecords())
            totals["shuffle_read_bytes"] += int(stage.shuffleReadBytes())
            totals["shuffle_write_bytes"] += int(stage.shuffleWriteBytes())
    except Exception:
        pass
    return totals

sc = spark.sparkContext
before_metrics = _snapshot_stage_metrics(sc)

print("Executing query...")
start_query_time = time.time()

try:
    start_compile_time = time.time()
    result = spark.sql(query)
    try:
        _ = result._jdf.queryExecution().executedPlan()
    except Exception:
        pass
    compile_time = time.time() - start_compile_time

    # Execute full query plan using noop sink so column projections and sorts are not pruned
    row_count = 0
    try:
        result.write.format("noop").mode("overwrite").save()
    except Exception:
        row_count = result.count()

    query_time = time.time() - start_query_time

    # Collect basic metrics
    metrics = {{
        "status": "SUCCESS",
        "duration_sec": query_time,
        "compile_time_sec": compile_time,
        "view_registration_sec": view_registration_time,
        "row_count": row_count,
    }}

    # Collect Spark SQL execution metrics
    try:
        metrics["spark_version"] = sc.version

        after_metrics = _snapshot_stage_metrics(sc)
        total_input_bytes = max(0, after_metrics["input_bytes"] - before_metrics["input_bytes"])
        total_records = max(0, after_metrics["records_read"] - before_metrics["records_read"])
        total_shuffle_read = max(0, after_metrics["shuffle_read_bytes"] - before_metrics["shuffle_read_bytes"])
        total_shuffle_write = max(0, after_metrics["shuffle_write_bytes"] - before_metrics["shuffle_write_bytes"])

        # Fallback to SQLAppStatusStore if stage metrics were not populated
        if total_input_bytes == 0 and total_records == 0:
            try:
                sql_store = spark._jsparkSession.sharedState().statusStore()
                executions_seq = sql_store.executionsList()
                exec_it = executions_seq.iterator()
                last_exec = None
                while exec_it.hasNext():
                    last_exec = exec_it.next()

                if last_exec is not None:
                    exec_id = last_exec.executionId()
                    exec_metrics = sql_store.executionMetrics(exec_id)
                    metrics_it = exec_metrics.iterator()
                    while metrics_it.hasNext():
                        entry = metrics_it.next()
                        val_str = str(entry._2())
                        nums = re.findall(r"\\d+", val_str.replace(",", ""))
                        if nums:
                            pass
            except Exception as metric_err:
                print(f"Could not collect SQL store fallback metrics: {{metric_err}}")

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

    @staticmethod
    def _query_sort_key(query_name: str) -> Tuple[float, str]:
        """Natural sort key for TPC-DS query names (e.g., q1, q2, ..., q14a, q14b, ..., q99)."""
        match = re.match(r"^q(\d+)([a-z]*)$", query_name, re.IGNORECASE)
        if match:
            return (int(match.group(1)), match.group(2).lower())
        return (float("inf"), query_name)

    def _get_query_list(self) -> List[str]:
        """Get list of queries to run based on configuration."""
        queries_config = self.benchmark_config.get("queries_to_run", "all")

        if queries_config == "all":
            # Get all SQL files from sql directory sorted in natural numeric order
            if self.sql_dir.exists():
                sql_files = self.sql_dir.glob("q*.sql")
                return sorted((f.stem for f in sql_files), key=self._query_sort_key)
            else:
                # Default to q1-q99
                return [f"q{i}" for i in range(1, 100)]

        raw_queries = queries_config if isinstance(queries_config, list) else [queries_config]
        expanded_queries: List[str] = []

        for q in raw_queries:
            q_name = f"q{q}" if isinstance(q, int) or str(q).isdigit() else str(q)
            # Expand multi-part queries (e.g., 14 -> q14a, q14b) if base SQL file doesn't exist
            if self.sql_dir.exists() and not (self.sql_dir / f"{q_name}.sql").exists():
                variants = sorted(
                    (f.stem for f in self.sql_dir.glob(f"{q_name}[a-z].sql")),
                    key=self._query_sort_key,
                )
                if variants:
                    expanded_queries.extend(variants)
                    continue
            expanded_queries.append(q_name)

        return expanded_queries

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
            # If METRICS_JSON provides duration_sec (pure SQL execution time), it overrides total job wall time
            metrics = self._parse_job_metrics(job_details)

            return {
                "query_name": query_name,
                "iteration": iteration,
                "status": status,
                "job_id": job_id,
                "duration_sec": duration,
                "job_duration_sec": duration,
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
        detailed Spark execution metrics (including pure SQL duration_sec).
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

                    # Parse GCS URI
                    if isinstance(driver_uri, str) and driver_uri.startswith("gs://"):
                        path = driver_uri.replace("gs://", "")
                        bucket_name = path.split("/")[0]
                        blob_path = "/".join(path.split("/")[1:])

                        bucket = self.storage_client.bucket(bucket_name)
                        blob = bucket.blob(blob_path)

                        output = None
                        if blob.exists():
                            output = blob.download_as_text()
                        else:
                            # Dataproc writes driver output shards as <prefix>.000000000
                            shards = sorted(
                                bucket.list_blobs(prefix=blob_path),
                                key=lambda b: b.name,
                            )
                            if shards:
                                output = "\n".join(s.download_as_text() for s in shards)

                        if isinstance(output, str):
                            # Find METRICS_JSON line
                            for line in output.split("\n"):
                                if line.startswith("METRICS_JSON:"):
                                    json_str = line[len("METRICS_JSON:"):]
                                    parsed_metrics = json.loads(json_str)

                                    # Copy relevant metrics (including pure SQL duration_sec)
                                    for key in [
                                        "duration_sec",
                                        "spark_version",
                                        "row_count",
                                        "view_registration_sec",
                                        "input_bytes",
                                        "shuffle_read_bytes",
                                        "shuffle_write_bytes",
                                        "records_read",
                                        "compile_time_sec",
                                    ]:
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
