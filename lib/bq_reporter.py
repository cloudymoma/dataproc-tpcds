"""BigQuery Metrics Reporter for TPC-DS Benchmark Results."""

import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from google.cloud import bigquery
from google.api_core.exceptions import NotFound, Conflict

logger = logging.getLogger(__name__)

# BigQuery location mapping from GCP regions
# Multi-region locations: US, EU
# Single-region locations use full region name
BQ_LOCATION_MAP = {
    "us": "US",
    "us-central1": "US",
    "us-east1": "US",
    "us-east4": "US",
    "us-west1": "US",
    "us-west2": "US",
    "us-west3": "US",
    "us-west4": "US",
    "northamerica-northeast1": "US",
    "northamerica-northeast2": "US",
    "southamerica-east1": "southamerica-east1",
    "europe": "EU",
    "europe-west1": "EU",
    "europe-west2": "EU",
    "europe-west3": "EU",
    "europe-west4": "EU",
    "europe-west6": "EU",
    "europe-north1": "EU",
    "europe-central2": "EU",
    "asia-east1": "asia-east1",
    "asia-east2": "asia-east2",
    "asia-northeast1": "asia-northeast1",
    "asia-northeast2": "asia-northeast2",
    "asia-northeast3": "asia-northeast3",
    "asia-south1": "asia-south1",
    "asia-southeast1": "asia-southeast1",
    "asia-southeast2": "asia-southeast2",
    "australia-southeast1": "australia-southeast1",
}

# BigQuery schema for benchmark_history table
BENCHMARK_SCHEMA = [
    bigquery.SchemaField("job_uuid", "STRING", mode="REQUIRED",
                         description="Unique ID for this test"),
    bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED",
                         description="Batch ID for grouping queries in a run"),
    bigquery.SchemaField("run_timestamp", "TIMESTAMP", mode="REQUIRED",
                         description="Execution timestamp"),
    bigquery.SchemaField("project_id", "STRING", mode="REQUIRED",
                         description="GCP Project ID"),
    bigquery.SchemaField("cluster_name", "STRING", mode="REQUIRED",
                         description="Dataproc cluster name"),
    bigquery.SchemaField("scale_factor", "INTEGER", mode="REQUIRED",
                         description="TPC-DS scale factor in GB"),
    bigquery.SchemaField("spark_version", "STRING", mode="NULLABLE",
                         description="Spark version"),
    bigquery.SchemaField("image_version", "STRING", mode="NULLABLE",
                         description="Dataproc image version"),
    bigquery.SchemaField("worker_count", "INTEGER", mode="NULLABLE",
                         description="Number of worker nodes"),
    bigquery.SchemaField("worker_machine_type", "STRING", mode="NULLABLE",
                         description="Worker machine type"),
    bigquery.SchemaField("query_name", "STRING", mode="REQUIRED",
                         description="Query name (e.g., q1, q99)"),
    bigquery.SchemaField("iteration", "INTEGER", mode="NULLABLE",
                         description="Iteration number"),
    bigquery.SchemaField("status", "STRING", mode="REQUIRED",
                         description="SUCCESS or FAILED"),
    bigquery.SchemaField("duration_sec", "FLOAT", mode="NULLABLE",
                         description="Total execution time in seconds"),
    bigquery.SchemaField("compile_time_sec", "FLOAT", mode="NULLABLE",
                         description="SQL compile/optimization time"),
    bigquery.SchemaField("input_bytes", "INTEGER", mode="NULLABLE",
                         description="Total input data scanned"),
    bigquery.SchemaField("shuffle_read_bytes", "INTEGER", mode="NULLABLE",
                         description="Shuffle read bytes"),
    bigquery.SchemaField("shuffle_write_bytes", "INTEGER", mode="NULLABLE",
                         description="Shuffle write bytes"),
    bigquery.SchemaField("records_read", "INTEGER", mode="NULLABLE",
                         description="Total records processed"),
    bigquery.SchemaField("executor_cores", "INTEGER", mode="NULLABLE",
                         description="Executor cores"),
    bigquery.SchemaField("executor_memory", "STRING", mode="NULLABLE",
                         description="Executor memory"),
    bigquery.SchemaField("data_format", "STRING", mode="NULLABLE",
                         description="Data format (parquet/orc)"),
    bigquery.SchemaField("job_id", "STRING", mode="NULLABLE",
                         description="Dataproc job ID"),
    bigquery.SchemaField("error_message", "STRING", mode="NULLABLE",
                         description="Error message if failed"),
]


class BQReporter:
    """Reports benchmark metrics to BigQuery.

    This reporter uses append-only inserts - it never deletes, truncates,
    or overwrites existing data. Each benchmark run creates new rows with
    unique job_uuid values, preserving full history for trend analysis.

    The table is partitioned by run_timestamp for efficient querying
    and automatic data lifecycle management if needed.
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize BQReporter with configuration.

        Args:
            config: Full configuration dictionary from conf.yaml
        """
        self.config = config
        self.gcp_config = config["gcp"]
        self.bq_config = config.get("bigquery", {})
        self.benchmark_config = config["benchmark"]
        self.dataproc_config = config["dataproc"]

        self.project_id = self.gcp_config["project_id"]
        self.enabled = self.bq_config.get("enable", True)
        self.dataset_id = self.bq_config.get("dataset", "tpcds_metrics")
        self.table_id = self.bq_config.get("table", "benchmark_history")

        # Initialize BigQuery client
        if self.enabled:
            self.client = bigquery.Client(project=self.project_id)
            self.full_table_id = f"{self.project_id}.{self.dataset_id}.{self.table_id}"

    def _get_bq_location(self) -> str:
        """Get BigQuery location from GCP region.

        Returns:
            Valid BigQuery location string
        """
        region = self.gcp_config.get("region", "us-central1").lower()
        return BQ_LOCATION_MAP.get(region, "US")

    def _ensure_dataset_exists(self):
        """Create the dataset if it doesn't exist."""
        dataset_ref = bigquery.DatasetReference(self.project_id, self.dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
            logger.debug(f"Dataset {self.dataset_id} already exists")
        except NotFound:
            try:
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = self._get_bq_location()
                self.client.create_dataset(dataset)
                logger.info(f"Created dataset {self.dataset_id} in location {dataset.location}")
            except Conflict:
                # Dataset was created by another process between get and create
                logger.debug(f"Dataset {self.dataset_id} was created by another process")

    def _ensure_table_exists(self):
        """Create the benchmark_history table if it doesn't exist."""
        self._ensure_dataset_exists()

        table_ref = bigquery.TableReference(
            bigquery.DatasetReference(self.project_id, self.dataset_id),
            self.table_id,
        )

        try:
            self.client.get_table(table_ref)
            logger.debug(f"Table {self.table_id} already exists")
        except NotFound:
            try:
                table = bigquery.Table(table_ref, schema=BENCHMARK_SCHEMA)
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="run_timestamp",
                )
                table.description = "TPC-DS benchmark results history"
                self.client.create_table(table)
                logger.info(f"Created table {self.full_table_id}")
            except Conflict:
                # Table was created by another process between get and create
                logger.debug(f"Table {self.table_id} was created by another process")

    def setup(self) -> bool:
        """Set up BigQuery dataset and table.

        This method explicitly creates the dataset and table if they don't exist.
        It's called automatically by report_result/report_results, but can be
        called separately for explicit setup.

        Returns:
            True if setup was successful, False otherwise
        """
        if not self.enabled:
            logger.info("BigQuery reporting is disabled, skipping setup")
            return True

        try:
            self._ensure_table_exists()
            logger.info(f"BigQuery setup complete: {self.full_table_id}")
            return True
        except Exception as e:
            logger.error(f"BigQuery setup failed: {e}")
            return False

    def _build_row(
        self,
        query_result: Dict[str, Any],
        cluster_info: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Build a BigQuery row from query result.

        Args:
            query_result: Result dictionary from QueryRunner
            cluster_info: Optional cluster information

        Returns:
            Dictionary formatted for BigQuery insertion
        """
        cluster_info = cluster_info or {}

        # Get executor settings from config
        spark_props = self.dataproc_config.get("spark_properties", {})
        executor_memory = spark_props.get("spark.executor.memory", "6g")
        executor_cores = spark_props.get("spark.executor.cores", "2")

        try:
            executor_cores_int = int(executor_cores)
        except (ValueError, TypeError):
            executor_cores_int = 2

        row = {
            "job_uuid": str(uuid.uuid4()),
            "batch_id": query_result.get("batch_id", str(uuid.uuid4())),
            "run_timestamp": datetime.utcnow().isoformat(),
            "project_id": self.project_id,
            "cluster_name": self.dataproc_config["cluster_name"],
            "scale_factor": self.benchmark_config["scale_factor"],
            "spark_version": query_result.get("spark_version"),
            "image_version": cluster_info.get(
                "image_version", self.dataproc_config.get("image_version")
            ),
            "worker_count": cluster_info.get(
                "worker_count", self.dataproc_config.get("num_workers")
            ),
            "worker_machine_type": cluster_info.get(
                "worker_machine_type", self.dataproc_config.get("worker_machine_type")
            ),
            "query_name": query_result.get("query_name", "unknown"),
            "iteration": query_result.get("iteration", 1),
            "status": query_result.get("status", "UNKNOWN"),
            "duration_sec": query_result.get("duration_sec"),
            "compile_time_sec": query_result.get("compile_time_sec"),
            "input_bytes": query_result.get("input_bytes"),
            "shuffle_read_bytes": query_result.get("shuffle_read_bytes"),
            "shuffle_write_bytes": query_result.get("shuffle_write_bytes"),
            "records_read": query_result.get("records_read"),
            "executor_cores": executor_cores_int,
            "executor_memory": executor_memory,
            "data_format": self.benchmark_config.get("data_format", "parquet"),
            "job_id": query_result.get("job_id"),
            "error_message": query_result.get("error_message"),
        }

        return row

    def report_result(
        self,
        query_result: Dict[str, Any],
        cluster_info: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Report a single query result to BigQuery (append-only).

        This method appends a new row to the BigQuery table. It never
        deletes or modifies existing data.

        Args:
            query_result: Result dictionary from QueryRunner
            cluster_info: Optional cluster information

        Returns:
            True if reporting was successful
        """
        if not self.enabled:
            logger.debug("BigQuery reporting is disabled")
            return True

        try:
            self._ensure_table_exists()
            row = self._build_row(query_result, cluster_info)

            # Append-only insert: insert_rows_json never deletes existing data
            errors = self.client.insert_rows_json(self.full_table_id, [row])
            if errors:
                logger.error(f"BigQuery insert errors: {errors}")
                return False

            logger.debug(f"Reported result for {row['query_name']} to BigQuery")
            return True

        except Exception as e:
            logger.error(f"Failed to report to BigQuery: {e}")
            return False

    def report_results(
        self,
        query_results: List[Dict[str, Any]],
        cluster_info: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Report multiple query results to BigQuery (append-only).

        This method appends new rows to the BigQuery table. It never
        deletes or modifies existing data. Previous benchmark results
        are preserved for historical comparison.

        Args:
            query_results: List of result dictionaries from QueryRunner
            cluster_info: Optional cluster information

        Returns:
            Number of successfully reported results
        """
        if not self.enabled:
            logger.info("BigQuery reporting is disabled")
            return 0

        if not query_results:
            logger.info("No results to report")
            return 0

        try:
            self._ensure_table_exists()

            rows = [self._build_row(r, cluster_info) for r in query_results]

            # Append-only insert: insert_rows_json never deletes existing data
            errors = self.client.insert_rows_json(self.full_table_id, rows)
            if errors:
                logger.error(f"BigQuery insert errors: {errors}")
                # Count successful inserts
                return len(rows) - len(errors)

            logger.info(f"Reported {len(rows)} results to BigQuery")
            return len(rows)

        except Exception as e:
            logger.error(f"Failed to report to BigQuery: {e}")
            return 0

    def get_summary_query(self) -> str:
        """Return a SQL query for summarizing benchmark results."""
        return f"""
SELECT
    batch_id,
    MIN(run_timestamp) as run_time,
    cluster_name,
    worker_count,
    worker_machine_type,
    scale_factor,
    COUNT(*) as total_queries,
    COUNTIF(status = 'SUCCESS') as successful_queries,
    COUNTIF(status = 'FAILED') as failed_queries,
    AVG(duration_sec) as avg_duration_sec,
    MAX(duration_sec) as max_duration_sec,
    SUM(duration_sec) as total_duration_sec
FROM `{self.full_table_id}`
GROUP BY batch_id, cluster_name, worker_count, worker_machine_type, scale_factor
ORDER BY run_time DESC
LIMIT 20
"""

    def print_summary(self, batch_id: Optional[str] = None):
        """Print a summary of benchmark results."""
        if not self.enabled:
            logger.info("BigQuery reporting is disabled")
            return

        try:
            query = f"""
SELECT
    query_name,
    iteration,
    status,
    duration_sec,
    error_message
FROM `{self.full_table_id}`
WHERE batch_id = @batch_id
ORDER BY query_name, iteration
"""
            if batch_id:
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id)
                    ]
                )
                results = self.client.query(query, job_config=job_config).result()

                print("\n=== Benchmark Results Summary ===")
                print(f"Batch ID: {batch_id}\n")
                print(f"{'Query':<10} {'Iter':<6} {'Status':<10} {'Duration(s)':<12}")
                print("-" * 40)

                for row in results:
                    print(
                        f"{row.query_name:<10} {row.iteration:<6} "
                        f"{row.status:<10} {row.duration_sec or 0:.2f}"
                    )

        except Exception as e:
            logger.error(f"Failed to print summary: {e}")
