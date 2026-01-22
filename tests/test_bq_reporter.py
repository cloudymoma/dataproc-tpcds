"""Tests for BQReporter module."""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from google.api_core.exceptions import NotFound, Conflict

from lib.bq_reporter import BQReporter, BENCHMARK_SCHEMA, BQ_LOCATION_MAP


class TestBQReporter:
    """Test cases for BQReporter class."""

    def test_init(self, sample_config):
        """Test BQReporter initialization."""
        with patch("lib.bq_reporter.bigquery.Client"):
            reporter = BQReporter(sample_config)

            assert reporter.project_id == "test-project"
            assert reporter.enabled is True
            assert reporter.dataset_id == "tpcds_metrics"
            assert reporter.table_id == "benchmark_history"

    def test_init_disabled(self, sample_config):
        """Test BQReporter when disabled."""
        sample_config["bigquery"]["enable"] = False

        reporter = BQReporter(sample_config)

        assert reporter.enabled is False

    def test_schema_fields(self):
        """Test that schema contains all required fields."""
        field_names = [f.name for f in BENCHMARK_SCHEMA]

        required_fields = [
            "job_uuid", "batch_id", "run_timestamp", "project_id",
            "cluster_name", "scale_factor", "query_name", "status",
            "duration_sec", "error_message"
        ]

        for field in required_fields:
            assert field in field_names, f"Missing required field: {field}"

    def test_ensure_dataset_exists_creates(self, sample_config):
        """Test dataset creation when it doesn't exist."""
        with patch("lib.bq_reporter.bigquery.Client") as mock_bq:
            mock_client = MagicMock()
            mock_bq.return_value = mock_client
            mock_client.get_dataset.side_effect = NotFound("Not found")

            reporter = BQReporter(sample_config)
            reporter._ensure_dataset_exists()

            mock_client.create_dataset.assert_called_once()

    def test_ensure_dataset_exists_already(self, sample_config):
        """Test when dataset already exists."""
        with patch("lib.bq_reporter.bigquery.Client") as mock_bq:
            mock_client = MagicMock()
            mock_bq.return_value = mock_client
            mock_client.get_dataset.return_value = MagicMock()

            reporter = BQReporter(sample_config)
            reporter._ensure_dataset_exists()

            mock_client.create_dataset.assert_not_called()

    def test_ensure_table_exists_creates(self, sample_config):
        """Test table creation when it doesn't exist."""
        with patch("lib.bq_reporter.bigquery.Client") as mock_bq:
            mock_client = MagicMock()
            mock_bq.return_value = mock_client
            mock_client.get_dataset.return_value = MagicMock()
            mock_client.get_table.side_effect = NotFound("Not found")

            reporter = BQReporter(sample_config)
            reporter._ensure_table_exists()

            mock_client.create_table.assert_called_once()

    def test_build_row(self, sample_config, mock_query_result, mock_cluster_info):
        """Test building a BigQuery row from query result."""
        with patch("lib.bq_reporter.bigquery.Client"):
            reporter = BQReporter(sample_config)
            row = reporter._build_row(mock_query_result, mock_cluster_info)

            assert row["query_name"] == "q1"
            assert row["status"] == "SUCCESS"
            assert row["duration_sec"] == 45.5
            assert row["project_id"] == "test-project"
            assert row["scale_factor"] == 100
            assert row["cluster_name"] == "test-cluster"
            assert "job_uuid" in row
            assert "run_timestamp" in row

    def test_build_row_without_cluster_info(self, sample_config, mock_query_result):
        """Test building a row without cluster info."""
        with patch("lib.bq_reporter.bigquery.Client"):
            reporter = BQReporter(sample_config)
            row = reporter._build_row(mock_query_result)

            assert row["query_name"] == "q1"
            assert row["worker_count"] == 4  # From config

    def test_report_result_disabled(self, sample_config, mock_query_result):
        """Test reporting when disabled."""
        sample_config["bigquery"]["enable"] = False

        reporter = BQReporter(sample_config)
        result = reporter.report_result(mock_query_result)

        assert result is True  # Should succeed silently

    def test_report_result_success(self, sample_config, mock_query_result):
        """Test successful result reporting."""
        with patch("lib.bq_reporter.bigquery.Client") as mock_bq:
            mock_client = MagicMock()
            mock_bq.return_value = mock_client
            mock_client.get_dataset.return_value = MagicMock()
            mock_client.get_table.return_value = MagicMock()
            mock_client.insert_rows_json.return_value = []

            reporter = BQReporter(sample_config)
            result = reporter.report_result(mock_query_result)

            assert result is True
            mock_client.insert_rows_json.assert_called_once()

    def test_report_result_insert_error(self, sample_config, mock_query_result):
        """Test reporting with insert errors."""
        with patch("lib.bq_reporter.bigquery.Client") as mock_bq:
            mock_client = MagicMock()
            mock_bq.return_value = mock_client
            mock_client.get_dataset.return_value = MagicMock()
            mock_client.get_table.return_value = MagicMock()
            mock_client.insert_rows_json.return_value = [{"error": "Test error"}]

            reporter = BQReporter(sample_config)
            result = reporter.report_result(mock_query_result)

            assert result is False

    def test_report_results_multiple(self, sample_config, mock_query_result):
        """Test reporting multiple results."""
        with patch("lib.bq_reporter.bigquery.Client") as mock_bq:
            mock_client = MagicMock()
            mock_bq.return_value = mock_client
            mock_client.get_dataset.return_value = MagicMock()
            mock_client.get_table.return_value = MagicMock()
            mock_client.insert_rows_json.return_value = []

            results = [
                {**mock_query_result, "query_name": "q1"},
                {**mock_query_result, "query_name": "q2"},
                {**mock_query_result, "query_name": "q3"},
            ]

            reporter = BQReporter(sample_config)
            count = reporter.report_results(results)

            assert count == 3

    def test_report_results_empty(self, sample_config):
        """Test reporting empty results list."""
        with patch("lib.bq_reporter.bigquery.Client"):
            reporter = BQReporter(sample_config)
            count = reporter.report_results([])

            assert count == 0

    def test_get_summary_query(self, sample_config):
        """Test summary query generation."""
        with patch("lib.bq_reporter.bigquery.Client"):
            reporter = BQReporter(sample_config)
            query = reporter.get_summary_query()

            assert "SELECT" in query
            assert "benchmark_history" in query
            assert "GROUP BY" in query
            assert "batch_id" in query

    def test_bq_location_mapping(self):
        """Test BigQuery location mapping is correct."""
        # US regions should map to US
        assert BQ_LOCATION_MAP["us-central1"] == "US"
        assert BQ_LOCATION_MAP["us-east1"] == "US"
        assert BQ_LOCATION_MAP["us-west1"] == "US"

        # EU regions should map to EU
        assert BQ_LOCATION_MAP["europe-west1"] == "EU"
        assert BQ_LOCATION_MAP["europe-west2"] == "EU"

        # Asia regions should use their own location
        assert BQ_LOCATION_MAP["asia-east1"] == "asia-east1"
        assert BQ_LOCATION_MAP["asia-northeast1"] == "asia-northeast1"

    def test_get_bq_location(self, sample_config):
        """Test _get_bq_location method."""
        with patch("lib.bq_reporter.bigquery.Client"):
            reporter = BQReporter(sample_config)
            location = reporter._get_bq_location()
            assert location == "US"

    def test_get_bq_location_unknown_region(self, sample_config):
        """Test _get_bq_location with unknown region defaults to US."""
        sample_config["gcp"]["region"] = "unknown-region-123"
        with patch("lib.bq_reporter.bigquery.Client"):
            reporter = BQReporter(sample_config)
            location = reporter._get_bq_location()
            assert location == "US"

    def test_ensure_dataset_handles_conflict(self, sample_config):
        """Test dataset creation handles Conflict exception."""
        with patch("lib.bq_reporter.bigquery.Client") as mock_bq:
            mock_client = MagicMock()
            mock_bq.return_value = mock_client
            mock_client.get_dataset.side_effect = NotFound("Not found")
            mock_client.create_dataset.side_effect = Conflict("Already exists")

            reporter = BQReporter(sample_config)
            # Should not raise
            reporter._ensure_dataset_exists()

    def test_ensure_table_handles_conflict(self, sample_config):
        """Test table creation handles Conflict exception."""
        with patch("lib.bq_reporter.bigquery.Client") as mock_bq:
            mock_client = MagicMock()
            mock_bq.return_value = mock_client
            mock_client.get_dataset.return_value = MagicMock()
            mock_client.get_table.side_effect = NotFound("Not found")
            mock_client.create_table.side_effect = Conflict("Already exists")

            reporter = BQReporter(sample_config)
            # Should not raise
            reporter._ensure_table_exists()

    def test_setup_success(self, sample_config):
        """Test setup() method success."""
        with patch("lib.bq_reporter.bigquery.Client") as mock_bq:
            mock_client = MagicMock()
            mock_bq.return_value = mock_client
            mock_client.get_dataset.return_value = MagicMock()
            mock_client.get_table.return_value = MagicMock()

            reporter = BQReporter(sample_config)
            result = reporter.setup()

            assert result is True

    def test_setup_disabled(self, sample_config):
        """Test setup() when BQ is disabled."""
        sample_config["bigquery"]["enable"] = False

        reporter = BQReporter(sample_config)
        result = reporter.setup()

        assert result is True

    def test_setup_failure(self, sample_config):
        """Test setup() handles failures."""
        with patch("lib.bq_reporter.bigquery.Client") as mock_bq:
            mock_client = MagicMock()
            mock_bq.return_value = mock_client
            mock_client.get_dataset.side_effect = Exception("Connection failed")

            reporter = BQReporter(sample_config)
            result = reporter.setup()

            assert result is False
