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
from google.cloud import storage
from google.api_core.exceptions import Conflict

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


def ensure_bucket_exists(config: Dict[str, Any]) -> bool:
    """Ensure the GCS staging bucket exists, creating it if necessary.

    Creates the bucket in the same region as the Dataproc cluster to ensure
    data locality and avoid cross-region transfer costs.

    Args:
        config: Configuration dictionary

    Returns:
        True if bucket exists or was created successfully
    """
    bucket_name = config["gcp"]["staging_bucket"].replace("gs://", "")
    region = config["gcp"]["region"]
    project_id = config["gcp"]["project_id"]

    try:
        client = storage.Client(project=project_id)
        bucket = client.bucket(bucket_name)

        if bucket.exists():
            # Verify bucket location matches configured region
            bucket.reload()
            bucket_location = bucket.location.lower()
            config_region = region.lower()

            # Check if locations are compatible (same region or multi-region containing the region)
            if bucket_location != config_region and not config_region.startswith(bucket_location):
                logger.warning(
                    f"Bucket '{bucket_name}' exists in '{bucket_location}' but cluster region is '{region}'. "
                    f"This may cause cross-region data transfer costs."
                )
            else:
                logger.info(f"Using existing bucket: gs://{bucket_name} (location: {bucket_location})")
            return True

        # Create bucket in the same region as the cluster
        logger.info(f"Creating bucket gs://{bucket_name} in region {region}...")
        bucket = client.bucket(bucket_name)
        bucket.storage_class = "STANDARD"
        new_bucket = client.create_bucket(bucket, location=region)
        logger.info(f"Created bucket: gs://{new_bucket.name} (location: {new_bucket.location})")
        return True

    except Conflict:
        # Bucket already exists (race condition or owned by another project)
        logger.info(f"Bucket gs://{bucket_name} already exists")
        return True

    except Exception as e:
        logger.error(f"Failed to ensure bucket exists: {e}")
        return False


def print_dry_run_summary(config: Dict[str, Any]):
    """Print detailed configuration summary for dry-run mode."""
    # ANSI color codes
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    BOLD = "\033[1m"
    RESET = "\033[0m"

    gcp = config["gcp"]
    dp = config["dataproc"]
    hs = config.get("history_server", {})
    bm = config["benchmark"]
    bq = config.get("bigquery", {})
    spark_props = dp.get("spark_properties", {})

    # Calculate resources
    num_workers = dp.get("num_workers", 4)
    num_executors = int(spark_props.get("spark.executor.instances", 8))
    exec_cores = int(spark_props.get("spark.executor.cores", 4))
    exec_mem = spark_props.get("spark.executor.memory", "10g")
    exec_overhead = spark_props.get("spark.executor.memoryOverhead", "1g")
    driver_cores = int(spark_props.get("spark.driver.cores", 4))
    driver_mem = spark_props.get("spark.driver.memory", "8g")
    driver_overhead = spark_props.get("spark.driver.memoryOverhead", "1g")

    print(f"""
{CYAN}{BOLD}╔══════════════════════════════════════════════════════════════════════════════╗
║                    TPC-DS BENCHMARK CONFIGURATION SUMMARY                    ║
╚══════════════════════════════════════════════════════════════════════════════╝{RESET}

{GREEN}{BOLD}┌─────────────────────────────────────────────────────────────────────────────┐
│  GCP PROJECT                                                                 │
└─────────────────────────────────────────────────────────────────────────────┘{RESET}
  Project ID      : {BOLD}{gcp['project_id']}{RESET}
  Region          : {gcp['region']}
  Zone            : {gcp.get('zone', gcp['region'] + '-a')}
  Staging Bucket  : {gcp['staging_bucket']}
""")

    # History Server
    if hs.get("enable", False):
        print(f"""{YELLOW}{BOLD}┌─────────────────────────────────────────────────────────────────────────────┐
│  SPARK HISTORY SERVER                                                        │
└─────────────────────────────────────────────────────────────────────────────┘{RESET}
  Cluster Name    : {hs.get('cluster_name', 'tpcds-history-server')}
  Machine Type    : {hs.get('machine_type', 'n2-standard-4')}
  Boot Disk       : {hs.get('boot_disk_size_gb', 128)} GB ({hs.get('boot_disk_type', 'pd-balanced')})
  Log Directory   : {hs.get('log_dir', gcp['staging_bucket'] + '/spark-events')}
  Status          : {GREEN}ENABLED{RESET}
""")
    else:
        print(f"""{YELLOW}{BOLD}┌─────────────────────────────────────────────────────────────────────────────┐
│  SPARK HISTORY SERVER                                                        │
└─────────────────────────────────────────────────────────────────────────────┘{RESET}
  Status          : {YELLOW}DISABLED{RESET}
""")

    # Job Cluster
    print(f"""{BLUE}{BOLD}┌─────────────────────────────────────────────────────────────────────────────┐
│  JOB CLUSTER                                                                 │
└─────────────────────────────────────────────────────────────────────────────┘{RESET}
  Cluster Name    : {BOLD}{dp['cluster_name']}{RESET}
  Image Version   : {dp.get('image_version', '2.3-debian12')}
  Tier            : {dp.get('tier', 'standard')}
  Max Idle        : {dp.get('max_idle', '1h')}

  {BOLD}Master Node:{RESET}
    Count         : {dp.get('num_masters', 1)}
    Machine Type  : {dp.get('master_machine_type', 'n2-standard-8')}
    Boot Disk     : {dp.get('master_boot_disk_size_gb', 500)} GB ({dp.get('master_boot_disk_type', 'pd-ssd')})
    Local SSDs    : {dp.get('num_master_local_ssds', 0)} x NVME

  {BOLD}Worker Nodes:{RESET}
    Count         : {num_workers}
    Machine Type  : {dp.get('worker_machine_type', 'n2-standard-8')}
    Boot Disk     : {dp.get('worker_boot_disk_size_gb', 500)} GB ({dp.get('worker_boot_disk_type', 'pd-ssd')})
    Local SSDs    : {dp.get('num_worker_local_ssds', 1)} x NVME

  {BOLD}Event Logging:{RESET}
    Enabled       : {spark_props.get('spark.eventLog.enabled', 'true')}
    Directory     : {spark_props.get('spark.eventLog.dir', 'N/A')}
    Compression   : {spark_props.get('spark.eventLog.compress', 'false')}
""")

    # Spark Resources
    total_exec_cores = num_executors * exec_cores
    print(f"""{MAGENTA}{BOLD}┌─────────────────────────────────────────────────────────────────────────────┐
│  SPARK RESOURCES                                                             │
└─────────────────────────────────────────────────────────────────────────────┘{RESET}
  {BOLD}Driver (runs on Master):{RESET}
    Cores         : {driver_cores}
    Memory        : {driver_mem} + {driver_overhead} overhead

  {BOLD}Executors (run on Workers):{RESET}
    Count         : {num_executors}
    Cores/Exec    : {exec_cores}
    Memory/Exec   : {exec_mem} + {exec_overhead} overhead
    Total Cores   : {total_exec_cores}

  {BOLD}Resource Layout:{RESET}
    ┌─────────────────────────────────────────────────────────────────┐
    │  Master: Driver ({driver_cores} cores, {driver_mem}+{driver_overhead})                       │
    ├─────────────────────────────────────────────────────────────────┤
    │  Worker 1-{num_workers}: {num_executors} Executors ({exec_cores} cores, {exec_mem}+{exec_overhead} each)        │
    └─────────────────────────────────────────────────────────────────┘
""")

    # Benchmark Settings
    queries = bm.get("queries_to_run", "all")
    if queries == "all":
        query_str = "All 99 TPC-DS queries"
    else:
        query_str = str(queries)

    print(f"""{GREEN}{BOLD}┌─────────────────────────────────────────────────────────────────────────────┐
│  BENCHMARK SETTINGS                                                          │
└─────────────────────────────────────────────────────────────────────────────┘{RESET}
  Scale Factor    : {BOLD}{bm['scale_factor']} GB{RESET} ({bm['scale_factor'] / 1000:.1f} TB)
  Data Format     : {bm.get('data_format', 'parquet')} ({bm.get('format_compression', 'snappy')})
  Data Path       : {bm['data_path']}
  Queries         : {query_str}
  Iterations      : {bm.get('iterations', 1)}
""")

    # BigQuery Reporting
    if bq.get("enable", True):
        print(f"""{CYAN}{BOLD}┌─────────────────────────────────────────────────────────────────────────────┐
│  BIGQUERY REPORTING                                                          │
└─────────────────────────────────────────────────────────────────────────────┘{RESET}
  Dataset         : {bq.get('dataset', 'tpcds_metrics')}
  Table           : {bq.get('table', 'benchmark_history')}
  Status          : {GREEN}ENABLED{RESET}
""")
    else:
        print(f"""{CYAN}{BOLD}┌─────────────────────────────────────────────────────────────────────────────┐
│  BIGQUERY REPORTING                                                          │
└─────────────────────────────────────────────────────────────────────────────┘{RESET}
  Status          : {YELLOW}DISABLED{RESET}
""")

    print(f"""{GREEN}{BOLD}╔══════════════════════════════════════════════════════════════════════════════╗
║  ✓ Configuration validated successfully                                      ║
╚══════════════════════════════════════════════════════════════════════════════╝{RESET}
""")


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

        # === Phase 2: Verify Data Exists ===
        logger.info("=" * 60)
        logger.info("Phase 2: Verifying TPC-DS Data")
        logger.info("=" * 60)

        data_stats = data_gen.get_data_stats()
        if not data_stats["is_complete"]:
            data_path = config["benchmark"]["data_path"]
            table_count = data_stats["table_count"]
            has_marker = data_stats["has_success_marker"]
            logger.error("")
            logger.error("=" * 70)
            logger.error("ERROR: TPC-DS data not found or incomplete!")
            logger.error("=" * 70)
            logger.error(f"  Data path    : {data_path}")
            logger.error(f"  Tables found : {table_count}/24")
            logger.error(f"  _SUCCESS     : {'Yes' if has_marker else 'No'}")
            logger.error("")
            logger.error("Data generation is a prerequisite for benchmarking.")
            logger.error("Please run 'make data-gen' first to generate TPC-DS data.")
            logger.error("")
            logger.error("Example:")
            logger.error("  make cluster-create   # Create cluster (if not exists)")
            logger.error("  make data-gen         # Generate TPC-DS data")
            logger.error("  make run              # Then run benchmark")
            logger.error("=" * 70)
            return 1

        logger.info(f"Data verified: {data_stats['table_count']} tables, "
                   f"{data_stats['total_size_human']}")

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

        # Ensure GCS bucket exists (creates in same region as cluster if needed)
        if not ensure_bucket_exists(config):
            logger.error("Failed to ensure GCS bucket exists. Please check permissions.")
            return 1

        # Dry run mode
        if args.dry_run:
            print_dry_run_summary(config)
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
