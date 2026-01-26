"""Tests for DataGenerator module."""

import pytest
from unittest.mock import Mock, patch, MagicMock

from lib.data_generator import DataGenerator, TPCDS_TABLES


class TestDataGenerator:
    """Test cases for DataGenerator class."""

    def test_init(self, sample_config):
        """Test DataGenerator initialization."""
        with patch("lib.data_generator.dataproc_v1.JobControllerClient"), \
             patch("lib.data_generator.storage.Client"):
            generator = DataGenerator(sample_config)

            assert generator.project_id == "test-project"
            assert generator.region == "us-central1"
            assert generator.scale_factor == 100
            assert generator.data_format == "parquet"
            assert generator.compression == "snappy"

    def test_tpcds_tables_count(self):
        """Test that all TPC-DS tables are defined."""
        assert len(TPCDS_TABLES) == 24
        assert "store_sales" in TPCDS_TABLES
        assert "date_dim" in TPCDS_TABLES
        assert "customer" in TPCDS_TABLES
        assert "item" in TPCDS_TABLES

    def test_data_exists_true(self, sample_config):
        """Test data_exists when data is present."""
        with patch("lib.data_generator.dataproc_v1.JobControllerClient"), \
             patch("lib.data_generator.storage.Client") as mock_storage:
            mock_client = MagicMock()
            mock_storage.return_value = mock_client

            mock_bucket = MagicMock()
            mock_client.bucket.return_value = mock_bucket

            # Mock _SUCCESS marker exists
            mock_success_blob = MagicMock()
            mock_success_blob.exists.return_value = True
            mock_bucket.blob.return_value = mock_success_blob

            # Mock all 24 tables exist
            mock_page = MagicMock()
            mock_page.prefixes = [f"data/{table}/" for table in TPCDS_TABLES]
            mock_blobs = MagicMock()
            mock_blobs.pages = [mock_page]
            mock_bucket.list_blobs.return_value = mock_blobs

            generator = DataGenerator(sample_config)
            result = generator.data_exists()

            assert result is True

    def test_data_exists_false(self, sample_config):
        """Test data_exists when data is not present."""
        with patch("lib.data_generator.dataproc_v1.JobControllerClient"), \
             patch("lib.data_generator.storage.Client") as mock_storage:
            mock_client = MagicMock()
            mock_storage.return_value = mock_client

            mock_bucket = MagicMock()
            mock_client.bucket.return_value = mock_bucket

            # Mock _SUCCESS marker does not exist
            mock_success_blob = MagicMock()
            mock_success_blob.exists.return_value = False
            mock_bucket.blob.return_value = mock_success_blob

            # Mock no tables
            mock_blobs = MagicMock()
            mock_blobs.pages = []
            mock_bucket.list_blobs.return_value = mock_blobs

            generator = DataGenerator(sample_config)
            result = generator.data_exists()

            assert result is False

    def test_generate_data_already_exists(self, sample_config):
        """Test generate_data when data already exists."""
        with patch("lib.data_generator.dataproc_v1.JobControllerClient"), \
             patch("lib.data_generator.storage.Client") as mock_storage:
            mock_client = MagicMock()
            mock_storage.return_value = mock_client

            mock_bucket = MagicMock()
            mock_client.bucket.return_value = mock_bucket

            # Mock _SUCCESS marker exists
            mock_success_blob = MagicMock()
            mock_success_blob.exists.return_value = True
            mock_bucket.blob.return_value = mock_success_blob

            # Mock all 24 tables exist
            mock_page = MagicMock()
            mock_page.prefixes = [f"data/{table}/" for table in TPCDS_TABLES]
            mock_blobs = MagicMock()
            mock_blobs.pages = [mock_page]
            mock_bucket.list_blobs.return_value = mock_blobs

            generator = DataGenerator(sample_config)
            result = generator.generate_data()

            assert result["status"] == "SKIPPED"
            assert "exists" in result["message"].lower()

    def test_list_tables(self, sample_config):
        """Test listing tables from GCS."""
        with patch("lib.data_generator.dataproc_v1.JobControllerClient"), \
             patch("lib.data_generator.storage.Client") as mock_storage:
            mock_client = MagicMock()
            mock_storage.return_value = mock_client

            mock_bucket = MagicMock()
            mock_client.bucket.return_value = mock_bucket

            # Mock page with prefixes
            mock_page = MagicMock()
            mock_page.prefixes = ["data/store_sales/", "data/date_dim/"]

            mock_blobs = MagicMock()
            mock_blobs.pages = [mock_page]
            mock_bucket.list_blobs.return_value = mock_blobs

            generator = DataGenerator(sample_config)
            tables = generator.list_tables()

            assert "store_sales" in tables
            assert "date_dim" in tables
