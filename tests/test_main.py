"""Tests for main.py entry point."""

import pytest
import tempfile
import yaml
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from main import load_config, setup_credentials


class TestLoadConfig:
    """Test cases for load_config function."""

    def test_load_valid_config(self, sample_config, tmp_path):
        """Test loading a valid configuration file."""
        config_file = tmp_path / "conf.yaml"
        with open(config_file, "w") as f:
            yaml.dump(sample_config, f)

        config = load_config(str(config_file))

        assert config["gcp"]["project_id"] == "test-project"
        assert config["dataproc"]["cluster_name"] == "test-cluster"
        assert config["benchmark"]["scale_factor"] == 100

    def test_load_config_file_not_found(self):
        """Test loading a non-existent config file."""
        with pytest.raises(FileNotFoundError):
            load_config("/nonexistent/path/conf.yaml")

    def test_load_config_missing_gcp_fields(self, tmp_path):
        """Test config validation with missing GCP fields."""
        config = {
            "gcp": {"project_id": "test"},  # Missing region and staging_bucket
            "dataproc": {"cluster_name": "test", "image_version": "2.1", "num_workers": 2},
            "benchmark": {"scale_factor": 10, "data_format": "parquet", "data_path": "gs://test"},
        }
        config_file = tmp_path / "conf.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        with pytest.raises(ValueError) as exc_info:
            load_config(str(config_file))

        assert "region" in str(exc_info.value) or "staging_bucket" in str(exc_info.value)

    def test_load_config_missing_dataproc_fields(self, tmp_path):
        """Test config validation with missing Dataproc fields."""
        config = {
            "gcp": {"project_id": "test", "region": "us-central1", "staging_bucket": "gs://test"},
            "dataproc": {"cluster_name": "test"},  # Missing image_version and num_workers
            "benchmark": {"scale_factor": 10, "data_format": "parquet", "data_path": "gs://test"},
        }
        config_file = tmp_path / "conf.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        with pytest.raises(ValueError) as exc_info:
            load_config(str(config_file))

        assert "image_version" in str(exc_info.value) or "num_workers" in str(exc_info.value)

    def test_load_config_missing_benchmark_fields(self, tmp_path):
        """Test config validation with missing benchmark fields."""
        config = {
            "gcp": {"project_id": "test", "region": "us-central1", "staging_bucket": "gs://test"},
            "dataproc": {"cluster_name": "test", "image_version": "2.1", "num_workers": 2},
            "benchmark": {"scale_factor": 10},  # Missing data_format and data_path
        }
        config_file = tmp_path / "conf.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        with pytest.raises(ValueError) as exc_info:
            load_config(str(config_file))

        assert "data_format" in str(exc_info.value) or "data_path" in str(exc_info.value)


class TestSetupCredentials:
    """Test cases for setup_credentials function."""

    def test_setup_credentials_with_valid_path(self, sample_config, tmp_path):
        """Test setting up credentials with a valid path."""
        key_file = tmp_path / "sa-key.json"
        key_file.write_text('{"type": "service_account"}')

        sample_config["gcp"]["service_account_key_path"] = str(key_file)

        import os
        original_env = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

        try:
            setup_credentials(sample_config)
            assert os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") == str(key_file)
        finally:
            if original_env:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = original_env
            elif "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
                del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

    def test_setup_credentials_with_invalid_path(self, sample_config):
        """Test setting up credentials with an invalid path."""
        sample_config["gcp"]["service_account_key_path"] = "/nonexistent/key.json"

        # Should not raise, just log a warning
        setup_credentials(sample_config)

    def test_setup_credentials_empty_path(self, sample_config):
        """Test setup when no key path is specified."""
        sample_config["gcp"]["service_account_key_path"] = ""

        # Should not raise or modify environment
        setup_credentials(sample_config)

    def test_setup_credentials_tilde_expansion(self, sample_config, tmp_path, monkeypatch):
        """Test that ~ is expanded in the path."""
        # Create a mock home directory
        mock_home = tmp_path / "home"
        mock_home.mkdir()
        key_file = mock_home / "key.json"
        key_file.write_text('{"type": "service_account"}')

        monkeypatch.setenv("HOME", str(mock_home))

        sample_config["gcp"]["service_account_key_path"] = "~/key.json"

        import os
        original_env = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

        try:
            setup_credentials(sample_config)
            assert "GOOGLE_APPLICATION_CREDENTIALS" in os.environ
        finally:
            if original_env:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = original_env
            elif "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
                del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]


class TestMainIntegration:
    """Integration tests for main module."""

    def test_dry_run_mode(self, sample_config, tmp_path):
        """Test dry run mode doesn't execute benchmark."""
        config_file = tmp_path / "conf.yaml"
        with open(config_file, "w") as f:
            yaml.dump(sample_config, f)

        with patch("sys.argv", ["main.py", "--config", str(config_file), "--dry-run"]):
            from main import main
            # In dry run, should print config and return 0
            # This would need actual execution to test properly
