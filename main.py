#!/usr/bin/env python3
"""
GCP Dataproc TPC-DS Auto-Benchmark Tool

A lightweight, highly automated CLI tool for running TPC-DS benchmarks
on Google Cloud Dataproc with single configuration file.

Usage:
    python main.py [--config conf.yaml] [--skip-cluster-delete]
"""

import argparse
import logging
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Dict

import yaml

from lib.cluster_manager import ClusterManager
from lib.data_generator import DataGenerator
from lib.query_runner import QueryRunner
from lib.bq_reporter import BQReporter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def load_config(config_path: str) -> Dict[str, Any]:
    """Load and validate configuration from YAML file.

    Args:
        config_path: Path to conf.yaml file

    Returns:
        Configuration dictionary

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If required config fields are missing
    """
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    # Validate required fields
    required_gcp = ["project_id", "region", "staging_bucket"]
    required_dataproc = ["cluster_name", "image_version", "num_workers"]
    required_benchmark = ["scale_factor", "data_format", "data_path"]

    for field in required_gcp:
        if field not in config.get("gcp", {}):
            raise ValueError(f"Missing required GCP config: {field}")

    for field in required_dataproc:
        if field not in config.get("dataproc", {}):
            raise ValueError(f"Missing required Dataproc config: {field}")

    for field in required_benchmark:
        if field not in config.get("benchmark", {}):
            raise ValueError(f"Missing required benchmark config: {field}")

    return config


def setup_credentials(config: Dict[str, Any]):
    """Set up Google Cloud credentials from config.

    Args:
        config: Configuration dictionary
    """
    key_path = config["gcp"].get("service_account_key_path")
    if key_path:
        # Expand ~ in path
        key_path = os.path.expanduser(key_path)
        if os.path.exists(key_path):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
            logger.info(f"Using service account credentials from: {key_path}")
        else:
            logger.warning(f"Service account key not found: {key_path}")
            logger.info("Falling back to default credentials")


def prompt_delete_cluster() -> bool:
    """Ask user whether to delete the cluster.

    Returns:
        True if user wants to delete the cluster
    """
    while True:
        response = input("\nDo you want to delete the cluster? [y/N]: ").strip().lower()
        if response in ("y", "yes"):
            return True
        elif response in ("n", "no", ""):
            return False
        else:
            print("Please enter 'y' or 'n'")


def run_benchmark(config: Dict[str, Any], skip_cluster_delete: bool = False) -> int:
    """Run the complete TPC-DS benchmark workflow.

    Args:
        config: Configuration dictionary
        skip_cluster_delete: If True, don't prompt for cluster deletion

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    batch_id = uuid.uuid4().hex
    logger.info(f"Starting TPC-DS benchmark (batch_id: {batch_id})")

    # Initialize components
    cluster_mgr = ClusterManager(config)
    data_gen = DataGenerator(config)
    query_runner = QueryRunner(config)
    bq_reporter = BQReporter(config)

    cluster_created = False
    history_server_enabled = config.get("history_server", {}).get("enable", False)

    try:
        # === Phase 0: Create History Server (if enabled) ===
        if history_server_enabled:
            logger.info("=" * 60)
            logger.info("Phase 0: Creating Spark History Server")
            logger.info("=" * 60)

            history_cluster = cluster_mgr.create_history_server(wait=True)
            if history_cluster:
                logger.info("History server is ready")
                history_url = cluster_mgr.get_history_server_url()
                if history_url:
                    logger.info(f"Spark History UI: {history_url}")
            else:
                logger.warning("History server creation skipped or failed")

        # === Phase 1: Create Job Cluster ===
        logger.info("=" * 60)
        logger.info("Phase 1: Creating Dataproc Job Cluster")
        logger.info("=" * 60)

        cluster = cluster_mgr.create_cluster(wait=True)
        if cluster:
            cluster_created = True
            logger.info("Job cluster is ready")
        else:
            logger.error("Failed to create cluster")
            return 1

        # Get cluster info for reporting
        cluster_info = cluster_mgr.get_cluster_info()

        # === Phase 2: Data Generation ===
        logger.info("=" * 60)
        logger.info("Phase 2: TPC-DS Data Generation")
        logger.info("=" * 60)

        datagen_result = data_gen.generate_data()
        if datagen_result["status"] == "FAILED":
            logger.error(f"Data generation failed: {datagen_result.get('error')}")
            return 1

        logger.info(f"Data generation: {datagen_result['status']}")

        # List available tables
        tables = data_gen.list_tables()
        logger.info(f"Available tables: {tables}")

        # === Phase 3: Run Queries ===
        logger.info("=" * 60)
        logger.info("Phase 3: Running TPC-DS Queries")
        logger.info("=" * 60)

        query_results = query_runner.run_all_queries(batch_id=batch_id)

        # Calculate summary
        total = len(query_results)
        success = sum(1 for r in query_results if r.get("status") == "DONE")
        failed = sum(1 for r in query_results if r.get("status") == "FAILED")
        skipped = sum(1 for r in query_results if r.get("status") == "SKIPPED")

        logger.info(f"Query execution complete: {success} success, {failed} failed, {skipped} skipped")

        # === Phase 4: Report to BigQuery ===
        logger.info("=" * 60)
        logger.info("Phase 4: Reporting Results to BigQuery")
        logger.info("=" * 60)

        if config.get("bigquery", {}).get("enable", True):
            reported = bq_reporter.report_results(query_results, cluster_info)
            logger.info(f"Reported {reported} results to BigQuery")

            # Print summary
            bq_reporter.print_summary(batch_id)
        else:
            logger.info("BigQuery reporting is disabled")

            # Print local summary
            print("\n=== Benchmark Results Summary ===")
            print(f"Batch ID: {batch_id}\n")
            print(f"{'Query':<10} {'Status':<10} {'Duration(s)':<12}")
            print("-" * 35)
            for r in query_results:
                print(f"{r.get('query_name', 'N/A'):<10} "
                      f"{r.get('status', 'N/A'):<10} "
                      f"{r.get('duration_sec', 0):.2f}")

        logger.info("=" * 60)
        logger.info("Benchmark completed successfully!")
        logger.info("=" * 60)

        return 0

    except KeyboardInterrupt:
        logger.warning("\nBenchmark interrupted by user")
        return 130

    except Exception as e:
        logger.error(f"Benchmark failed with error: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        # === Cleanup: Delete Cluster ===
        if cluster_created:
            if skip_cluster_delete:
                logger.info("Skipping cluster deletion (--skip-cluster-delete)")
            else:
                try:
                    should_delete = prompt_delete_cluster()
                    if should_delete:
                        logger.info("Deleting cluster...")
                        cluster_mgr.delete_cluster(wait=True)
                        logger.info("Cluster deleted")
                    else:
                        logger.info("Cluster kept running. Remember to delete it manually!")
                        logger.info(f"  gcloud dataproc clusters delete {config['dataproc']['cluster_name']} "
                                   f"--region={config['gcp']['region']}")
                except Exception as e:
                    logger.warning(f"Error during cleanup: {e}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="GCP Dataproc TPC-DS Auto-Benchmark Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                          # Run with default conf.yaml
  python main.py --config my_config.yaml  # Use custom config file
  python main.py --skip-cluster-delete    # Don't prompt for cluster deletion

For more information, see README.md
        """,
    )

    parser.add_argument(
        "--config", "-c",
        type=str,
        default="conf.yaml",
        help="Path to configuration file (default: conf.yaml)",
    )

    parser.add_argument(
        "--skip-cluster-delete",
        action="store_true",
        help="Skip the cluster deletion prompt",
    )

    parser.add_argument(
        "--auto-delete",
        action="store_true",
        help="Automatically delete cluster after benchmark",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate config and show plan without executing",
    )

    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    # Set log level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        # Load configuration
        logger.info(f"Loading configuration from: {args.config}")
        config = load_config(args.config)

        # Setup credentials
        setup_credentials(config)

        # Dry run mode
        if args.dry_run:
            logger.info("=== DRY RUN MODE ===")
            logger.info("Configuration validated successfully")
            logger.info(f"  Project: {config['gcp']['project_id']}")
            logger.info(f"  Region: {config['gcp']['region']}")
            logger.info(f"  Cluster: {config['dataproc']['cluster_name']}")
            logger.info(f"  Workers: {config['dataproc']['num_workers']} x {config['dataproc']['worker_machine_type']}")
            logger.info(f"  Scale Factor: {config['benchmark']['scale_factor']} GB")
            logger.info(f"  Data Path: {config['benchmark']['data_path']}")
            logger.info(f"  BigQuery: {'enabled' if config.get('bigquery', {}).get('enable', True) else 'disabled'}")
            return 0

        # Run benchmark
        exit_code = run_benchmark(config, skip_cluster_delete=args.skip_cluster_delete)

        # Auto-delete cluster if requested
        if args.auto_delete and exit_code == 0:
            cluster_mgr = ClusterManager(config)
            cluster_mgr.delete_cluster(wait=True)

        return exit_code

    except FileNotFoundError as e:
        logger.error(str(e))
        return 1
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
