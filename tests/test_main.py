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

    def test_dry_run_mode(self, sample_config, tmp_path, capsys):
        """Test dry run mode validates config without touching GCS or running benchmark."""
        config_file = tmp_path / "conf.yaml"
        with open(config_file, "w") as f:
            yaml.dump(sample_config, f)

        with patch("sys.argv", ["main.py", "--config", str(config_file), "--dry-run"]), \
             patch("main.ensure_bucket_exists") as mock_ensure_bucket, \
             patch("main.run_benchmark") as mock_run_benchmark:
            from main import main

            exit_code = main()
            captured = capsys.readouterr()

            assert exit_code == 0
            assert "Configuration validated successfully" in captured.out
            mock_ensure_bucket.assert_not_called()
            mock_run_benchmark.assert_not_called()

    def test_auto_delete_does_not_prompt(self, sample_config):
        """Test --auto-delete deletes cluster automatically without interactive input()."""
        from main import run_benchmark

        with patch("main.ClusterManager") as mock_cm_cls, \
             patch("main.DataGenerator") as mock_dg_cls, \
             patch("main.QueryRunner") as mock_qr_cls, \
             patch("main.BQReporter") as mock_bqr_cls, \
             patch("builtins.input") as mock_input:
            mock_cm = MagicMock()
            mock_cm.create_cluster.return_value = MagicMock()
            mock_cm.get_cluster_info.return_value = {"cluster_name": "test-cluster"}
            mock_cm_cls.return_value = mock_cm

            mock_dg = MagicMock()
            mock_dg.check_data_exists.return_value = (True, 24)
            mock_dg_cls.return_value = mock_dg

            mock_qr = MagicMock()
            mock_qr.run_all_queries.return_value = [
                {"query_name": "q1", "status": "DONE", "duration_sec": 1.2}
            ]
            mock_qr_cls.return_value = mock_qr

            mock_bqr = MagicMock()
            mock_bqr.enabled = False
            mock_bqr_cls.return_value = mock_bqr

            exit_code = run_benchmark(
                sample_config,
                skip_cluster_delete=False,
                auto_delete=True,
            )

            assert exit_code == 0
            mock_input.assert_not_called()
            mock_cm.delete_cluster.assert_called_once_with(wait=True)
