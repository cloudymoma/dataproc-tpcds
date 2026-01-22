"""Tests for ClusterManager module."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from google.api_core.exceptions import NotFound, AlreadyExists

from lib.cluster_manager import ClusterManager


class TestClusterManager:
    """Test cases for ClusterManager class."""

    def test_init(self, sample_config):
        """Test ClusterManager initialization."""
        with patch("lib.cluster_manager.dataproc_v1.ClusterControllerClient"):
            manager = ClusterManager(sample_config)

            assert manager.project_id == "test-project"
            assert manager.region == "us-central1"
            assert manager.zone == "us-central1-a"
            assert manager.cluster_name == "test-cluster"
            assert manager.staging_bucket == "test-bucket"

    def test_build_cluster_config(self, sample_config):
        """Test cluster configuration building."""
        with patch("lib.cluster_manager.dataproc_v1.ClusterControllerClient"):
            manager = ClusterManager(sample_config)
            cluster = manager._build_cluster_config()

            assert cluster.cluster_name == "test-cluster"
            assert cluster.project_id == "test-project"
            assert cluster.config.worker_config.num_instances == 4

    def test_cluster_exists_true(self, sample_config):
        """Test cluster_exists returns True when cluster exists."""
        with patch("lib.cluster_manager.dataproc_v1.ClusterControllerClient") as mock_client:
            mock_instance = MagicMock()
            mock_client.return_value = mock_instance
            mock_instance.get_cluster.return_value = MagicMock()

            manager = ClusterManager(sample_config)
            result = manager.cluster_exists()

            assert result is True
            mock_instance.get_cluster.assert_called_once()

    def test_cluster_exists_false(self, sample_config):
        """Test cluster_exists returns False when cluster doesn't exist."""
        with patch("lib.cluster_manager.dataproc_v1.ClusterControllerClient") as mock_client:
            mock_instance = MagicMock()
            mock_client.return_value = mock_instance
            mock_instance.get_cluster.side_effect = NotFound("Cluster not found")

            manager = ClusterManager(sample_config)
            result = manager.cluster_exists()

            assert result is False

    def test_create_cluster_already_exists(self, sample_config):
        """Test create_cluster when cluster already exists."""
        with patch("lib.cluster_manager.dataproc_v1.ClusterControllerClient") as mock_client:
            mock_instance = MagicMock()
            mock_client.return_value = mock_instance
            mock_cluster = MagicMock()
            mock_instance.get_cluster.return_value = mock_cluster

            manager = ClusterManager(sample_config)
            result = manager.create_cluster()

            assert result == mock_cluster
            mock_instance.create_cluster.assert_not_called()

    def test_create_cluster_new(self, sample_config):
        """Test creating a new cluster."""
        with patch("lib.cluster_manager.dataproc_v1.ClusterControllerClient") as mock_client:
            mock_instance = MagicMock()
            mock_client.return_value = mock_instance
            mock_instance.get_cluster.side_effect = NotFound("Not found")

            mock_operation = MagicMock()
            mock_cluster = MagicMock()
            mock_operation.result.return_value = mock_cluster
            mock_instance.create_cluster.return_value = mock_operation

            manager = ClusterManager(sample_config)
            result = manager.create_cluster(wait=True)

            assert result == mock_cluster
            mock_instance.create_cluster.assert_called_once()

    def test_delete_cluster_not_exists(self, sample_config):
        """Test delete_cluster when cluster doesn't exist."""
        with patch("lib.cluster_manager.dataproc_v1.ClusterControllerClient") as mock_client:
            mock_instance = MagicMock()
            mock_client.return_value = mock_instance
            mock_instance.get_cluster.side_effect = NotFound("Not found")

            manager = ClusterManager(sample_config)
            result = manager.delete_cluster()

            assert result is True
            mock_instance.delete_cluster.assert_not_called()

    def test_delete_cluster_success(self, sample_config):
        """Test successful cluster deletion."""
        with patch("lib.cluster_manager.dataproc_v1.ClusterControllerClient") as mock_client:
            mock_instance = MagicMock()
            mock_client.return_value = mock_instance
            mock_instance.get_cluster.return_value = MagicMock()

            mock_operation = MagicMock()
            mock_instance.delete_cluster.return_value = mock_operation

            manager = ClusterManager(sample_config)
            result = manager.delete_cluster(wait=True)

            assert result is True
            mock_instance.delete_cluster.assert_called_once()

    def test_get_cluster_info(self, sample_config):
        """Test getting cluster information."""
        with patch("lib.cluster_manager.dataproc_v1.ClusterControllerClient") as mock_client:
            mock_instance = MagicMock()
            mock_client.return_value = mock_instance

            mock_cluster = MagicMock()
            mock_cluster.cluster_name = "test-cluster"
            mock_cluster.project_id = "test-project"
            mock_cluster.config.software_config.image_version = "2.3-debian12"
            mock_cluster.config.worker_config.num_instances = 4
            mock_cluster.config.worker_config.machine_type_uri = "n2-standard-8"
            mock_cluster.config.master_config.machine_type_uri = "n2-standard-4"
            mock_instance.get_cluster.return_value = mock_cluster

            manager = ClusterManager(sample_config)
            info = manager.get_cluster_info()

            assert info["cluster_name"] == "test-cluster"
            assert info["worker_count"] == 4
            assert info["image_version"] == "2.3-debian12"

    def test_get_cluster_info_not_found(self, sample_config):
        """Test get_cluster_info when cluster doesn't exist."""
        with patch("lib.cluster_manager.dataproc_v1.ClusterControllerClient") as mock_client:
            mock_instance = MagicMock()
            mock_client.return_value = mock_instance
            mock_instance.get_cluster.side_effect = NotFound("Not found")

            manager = ClusterManager(sample_config)
            info = manager.get_cluster_info()

            assert info is None
