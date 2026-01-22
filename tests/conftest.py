"""Pytest fixtures for TPC-DS benchmark tests."""

import pytest
from pathlib import Path
from typing import Dict, Any


@pytest.fixture
def sample_config() -> Dict[str, Any]:
    """Return a sample configuration for testing."""
    return {
        "gcp": {
            "project_id": "test-project",
            "region": "us-central1",
            "zone": "us-central1-a",
            "service_account_key_path": "",
            "staging_bucket": "gs://test-bucket",
        },
        "dataproc": {
            "cluster_name": "test-cluster",
            "image_version": "2.3-debian12",
            "master_machine_type": "n2-standard-4",
            "worker_machine_type": "n2-standard-8",
            "num_workers": 4,
            "enable_component_gateway": True,
            "init_actions": [],
            "spark_properties": {
                "spark.executor.memory": "6g",
                "spark.executor.cores": "2",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            },
        },
        "benchmark": {
            "scale_factor": 100,
            "data_format": "parquet",
            "format_compression": "snappy",
            "data_path": "gs://test-bucket/tpcds-data/100G",
            "skip_data_gen": False,
            "queries_to_run": "all",
            "iterations": 1,
        },
        "bigquery": {
            "enable": True,
            "dataset": "tpcds_metrics",
            "table": "benchmark_history",
        },
    }


@pytest.fixture
def minimal_config() -> Dict[str, Any]:
    """Return minimal required configuration."""
    return {
        "gcp": {
            "project_id": "test-project",
            "region": "us-central1",
            "staging_bucket": "gs://test-bucket",
        },
        "dataproc": {
            "cluster_name": "test-cluster",
            "image_version": "2.3-debian12",
            "num_workers": 2,
        },
        "benchmark": {
            "scale_factor": 10,
            "data_format": "parquet",
            "data_path": "gs://test-bucket/data",
        },
    }


@pytest.fixture
def project_root() -> Path:
    """Return the project root directory."""
    return Path(__file__).parent.parent


@pytest.fixture
def sql_dir(project_root: Path) -> Path:
    """Return the SQL directory."""
    return project_root / "sql"


@pytest.fixture
def mock_query_result() -> Dict[str, Any]:
    """Return a sample query result."""
    return {
        "query_name": "q1",
        "iteration": 1,
        "status": "SUCCESS",
        "job_id": "test-job-123",
        "duration_sec": 45.5,
        "batch_id": "test-batch-456",
    }


@pytest.fixture
def mock_cluster_info() -> Dict[str, Any]:
    """Return sample cluster info."""
    return {
        "cluster_name": "test-cluster",
        "project_id": "test-project",
        "image_version": "2.3-debian12",
        "worker_count": 4,
        "worker_machine_type": "n2-standard-8",
        "master_machine_type": "n2-standard-4",
    }
