"""Tests for QueryRunner module."""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from lib.query_runner import QueryRunner


class TestQueryRunner:
    """Test cases for QueryRunner class."""

    def test_init(self, sample_config):
        """Test QueryRunner initialization."""
        with patch("lib.query_runner.dataproc_v1.JobControllerClient"), \
             patch("lib.query_runner.storage.Client"):
            runner = QueryRunner(sample_config)

            assert runner.project_id == "test-project"
            assert runner.region == "us-central1"
            assert runner.cluster_name == "test-cluster"
            assert runner.data_format == "parquet"
            assert runner.iterations == 1

    def test_get_query_list_all(self, sample_config, sql_dir):
        """Test getting all queries when 'all' is specified."""
        with patch("lib.query_runner.dataproc_v1.JobControllerClient"), \
             patch("lib.query_runner.storage.Client"):
            runner = QueryRunner(sample_config)
            runner.sql_dir = sql_dir

            queries = runner._get_query_list()

            # Should return at least the sample queries we created
            assert len(queries) >= 1
            assert all(q.startswith("q") for q in queries)

    def test_get_query_list_specific(self, sample_config):
        """Test getting specific queries."""
        sample_config["benchmark"]["queries_to_run"] = [1, 2, 3]

        with patch("lib.query_runner.dataproc_v1.JobControllerClient"), \
             patch("lib.query_runner.storage.Client"):
            runner = QueryRunner(sample_config)
            queries = runner._get_query_list()

            assert queries == ["q1", "q2", "q3"]

    def test_load_query_sql(self, sample_config, sql_dir):
        """Test loading SQL from file."""
        with patch("lib.query_runner.dataproc_v1.JobControllerClient"), \
             patch("lib.query_runner.storage.Client"):
            runner = QueryRunner(sample_config)
            runner.sql_dir = sql_dir

            # Assuming q1.sql exists
            sql = runner._load_query_sql("q1")

            if sql:
                assert "SELECT" in sql or "WITH" in sql

    def test_load_query_sql_not_found(self, sample_config, tmp_path):
        """Test loading SQL when file doesn't exist."""
        with patch("lib.query_runner.dataproc_v1.JobControllerClient"), \
             patch("lib.query_runner.storage.Client"):
            runner = QueryRunner(sample_config)
            runner.sql_dir = tmp_path

            sql = runner._load_query_sql("nonexistent")

            assert sql is None

    def test_create_temp_view_script(self, sample_config):
        """Test temporary view script creation."""
        with patch("lib.query_runner.dataproc_v1.JobControllerClient"), \
             patch("lib.query_runner.storage.Client"):
            runner = QueryRunner(sample_config)

            tables = ["store_sales", "date_dim", "customer"]
            query_sql = "SELECT * FROM store_sales LIMIT 10"

            script = runner._create_temp_view_script(tables, query_sql)

            assert "SparkSession" in script
            assert "createOrReplaceTempView" in script
            assert "store_sales" in script
            assert "date_dim" in script
            assert "METRICS_JSON" in script

    def test_list_tables_from_gcs(self, sample_config):
        """Test listing tables from GCS data path."""
        with patch("lib.query_runner.dataproc_v1.JobControllerClient"), \
             patch("lib.query_runner.storage.Client") as mock_storage:
            mock_client = MagicMock()
            mock_storage.return_value = mock_client

            mock_bucket = MagicMock()
            mock_client.bucket.return_value = mock_bucket

            mock_page = MagicMock()
            mock_page.prefixes = ["data/store_sales/", "data/item/"]

            mock_blobs = MagicMock()
            mock_blobs.pages = [mock_page]
            mock_bucket.list_blobs.return_value = mock_blobs

            runner = QueryRunner(sample_config)
            tables = runner._list_tables_from_gcs()

            assert "store_sales" in tables
            assert "item" in tables

    def test_run_query_no_tables(self, sample_config):
        """Test run_query when no tables are found."""
        with patch("lib.query_runner.dataproc_v1.JobControllerClient"), \
             patch("lib.query_runner.storage.Client") as mock_storage:
            mock_client = MagicMock()
            mock_storage.return_value = mock_client

            mock_bucket = MagicMock()
            mock_client.bucket.return_value = mock_bucket

            mock_blobs = MagicMock()
            mock_blobs.pages = []
            mock_bucket.list_blobs.return_value = mock_blobs

            runner = QueryRunner(sample_config)
            result = runner.run_query("q1", "SELECT 1", 1)

            assert result["status"] == "FAILED"
            assert "No tables" in result["error_message"]

    def test_run_query_success(self, sample_config):
        """Test successful query execution."""
        with patch("lib.query_runner.dataproc_v1.JobControllerClient") as mock_job_client, \
             patch("lib.query_runner.storage.Client") as mock_storage:
            mock_storage_client = MagicMock()
            mock_storage.return_value = mock_storage_client

            mock_bucket = MagicMock()
            mock_storage_client.bucket.return_value = mock_bucket

            mock_page = MagicMock()
            mock_page.prefixes = ["data/store_sales/"]
            mock_blobs = MagicMock()
            mock_blobs.pages = [mock_page]
            mock_bucket.list_blobs.return_value = mock_blobs

            mock_blob = MagicMock()
            mock_bucket.blob.return_value = mock_blob

            # Mock job client
            mock_job = MagicMock()
            mock_job_client.return_value = mock_job

            mock_operation = MagicMock()
            mock_result = MagicMock()
            mock_result.reference.job_id = "job-123"
            mock_result.status.state.name = "DONE"
            mock_operation.result.return_value = mock_result
            mock_job.submit_job_as_operation.return_value = mock_operation
            mock_job.get_job.return_value = mock_result

            runner = QueryRunner(sample_config)
            result = runner.run_query("q1", "SELECT 1", 1)

            assert result["job_id"] == "job-123"
            assert result["status"] == "DONE"

    def test_run_all_queries(self, sample_config, sql_dir):
        """Test running multiple queries."""
        sample_config["benchmark"]["queries_to_run"] = [1, 2]

        with patch("lib.query_runner.dataproc_v1.JobControllerClient") as mock_job_client, \
             patch("lib.query_runner.storage.Client") as mock_storage:
            mock_storage_client = MagicMock()
            mock_storage.return_value = mock_storage_client

            mock_bucket = MagicMock()
            mock_storage_client.bucket.return_value = mock_bucket

            mock_page = MagicMock()
            mock_page.prefixes = ["data/store/"]
            mock_blobs = MagicMock()
            mock_blobs.pages = [mock_page]
            mock_bucket.list_blobs.return_value = mock_blobs

            mock_blob = MagicMock()
            mock_bucket.blob.return_value = mock_blob

            mock_job = MagicMock()
            mock_job_client.return_value = mock_job

            mock_operation = MagicMock()
            mock_result = MagicMock()
            mock_result.reference.job_id = "job-123"
            mock_result.status.state.name = "DONE"
            mock_operation.result.return_value = mock_result
            mock_job.submit_job_as_operation.return_value = mock_operation
            mock_job.get_job.return_value = mock_result

            runner = QueryRunner(sample_config)
            runner.sql_dir = sql_dir

            results = runner.run_all_queries(batch_id="test-batch")

            assert len(results) == 2
            assert all(r.get("batch_id") == "test-batch" for r in results)
