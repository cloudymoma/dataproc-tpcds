"""Dataproc Cluster Lifecycle Management."""

import logging
import re
from typing import Any, Dict, Optional

from google.cloud import dataproc_v1
from google.cloud.dataproc_v1.types import Cluster, ClusterConfig, GceClusterConfig
from google.cloud.dataproc_v1.types import InstanceGroupConfig, SoftwareConfig
from google.cloud.dataproc_v1.types import DiskConfig, LifecycleConfig
from google.api_core.exceptions import NotFound, AlreadyExists
from google.api_core.operation import Operation
from google.protobuf import duration_pb2

logger = logging.getLogger(__name__)


def parse_duration_to_seconds(duration_str: str) -> int:
    """Parse duration string like '1h', '30m', '2h30m' to seconds.

    Args:
        duration_str: Duration string (e.g., '1h', '30m', '1h30m')

    Returns:
        Duration in seconds
    """
    total_seconds = 0
    pattern = r'(\d+)([hms])'
    matches = re.findall(pattern, duration_str.lower())

    for value, unit in matches:
        value = int(value)
        if unit == 'h':
            total_seconds += value * 3600
        elif unit == 'm':
            total_seconds += value * 60
        elif unit == 's':
            total_seconds += value

    # If no matches, try parsing as plain number (assume seconds)
    if not matches and duration_str.isdigit():
        total_seconds = int(duration_str)

    return total_seconds if total_seconds > 0 else 3600  # Default 1 hour


class ClusterManager:
    """Manages Dataproc cluster lifecycle: create, wait, delete."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize ClusterManager with configuration.

        Args:
            config: Full configuration dictionary from conf.yaml
        """
        self.config = config
        self.gcp_config = config["gcp"]
        self.dataproc_config = config["dataproc"]
        self.history_config = config.get("history_server", {})

        self.project_id = self.gcp_config["project_id"]
        self.region = self.gcp_config["region"]
        self.zone = self.gcp_config.get("zone", f"{self.region}-a")
        self.staging_bucket = self.gcp_config["staging_bucket"].replace("gs://", "")

        self.cluster_name = self.dataproc_config["cluster_name"]

        # History server settings
        self.history_server_enabled = self.history_config.get("enable", False)
        self.history_cluster_name = self.history_config.get("cluster_name", "tpcds-history-server")
        self.spark_log_dir = self.history_config.get(
            "log_dir", f"gs://{self.staging_bucket}/spark-events"
        )

        # Initialize Dataproc client
        self.cluster_client = dataproc_v1.ClusterControllerClient(
            client_options={"api_endpoint": f"{self.region}-dataproc.googleapis.com:443"}
        )

    def _get_history_server_properties(self) -> Dict[str, str]:
        """Get Spark properties for History Server from config.

        Reads spark_properties from history_server config and adds required
        history.fs.logDirectory property.
        """
        props = {}
        # Add log directory property (required for history server)
        props["spark:spark.history.fs.logDirectory"] = self.spark_log_dir

        # Add user-configured history server properties
        hs_spark_props = self.history_config.get("spark_properties", {})
        for key, value in hs_spark_props.items():
            # Add spark: prefix if not present
            prop_key = key if key.startswith("spark:") else f"spark:{key}"
            props[prop_key] = str(value)

        return props

    def _get_job_cluster_properties(self) -> Dict[str, str]:
        """Get Spark and Dataproc properties for job cluster from config.

        Reads spark_properties and dataproc_properties from dataproc config section.
        """
        props = {}

        # Add Spark properties (spark: prefix)
        spark_props = self.dataproc_config.get("spark_properties", {})
        for key, value in spark_props.items():
            prop_key = key if key.startswith("spark:") else f"spark:{key}"
            props[prop_key] = str(value)

        # Add Dataproc properties (dataproc: prefix)
        dataproc_props = self.dataproc_config.get("dataproc_properties", {})
        for key, value in dataproc_props.items():
            prop_key = key if key.startswith("dataproc:") else f"dataproc:{key}"
            props[prop_key] = str(value)

        return props

    def _build_history_server_config(self) -> Cluster:
        """Build configuration for Spark History Server cluster."""
        hs = self.history_config

        # Software configuration with properties from config
        software_config = SoftwareConfig(
            image_version=self.dataproc_config.get("image_version", "2.3-debian12"),
            properties=self._get_history_server_properties(),
        )

        # Master configuration (single node)
        master_disk = DiskConfig(
            boot_disk_type=hs.get("boot_disk_type", "pd-balanced"),
            boot_disk_size_gb=hs.get("boot_disk_size_gb", 128),
        )

        master_config = InstanceGroupConfig(
            num_instances=1,
            machine_type_uri=hs.get("machine_type", "n2-standard-4"),
            disk_config=master_disk,
        )

        # GCE configuration
        gce_config = GceClusterConfig(
            zone_uri=f"projects/{self.project_id}/zones/{self.zone}",
        )

        # Cluster config
        cluster_config = ClusterConfig(
            config_bucket=self.staging_bucket,
            gce_cluster_config=gce_config,
            master_config=master_config,
            software_config=software_config,
        )

        # Enable component gateway for web UI access
        cluster_config.endpoint_config = dataproc_v1.EndpointConfig(
            enable_http_port_access=True
        )

        return Cluster(
            project_id=self.project_id,
            cluster_name=self.history_cluster_name,
            config=cluster_config,
        )

    def _build_cluster_config(self) -> Cluster:
        """Build Dataproc job cluster configuration."""
        dp = self.dataproc_config

        # Get Spark properties from config (includes event logging if configured)
        spark_props = self._get_job_cluster_properties()

        # Software configuration
        software_config = SoftwareConfig(
            image_version=dp.get("image_version", "2.3-debian12"),
            properties=spark_props,
        )

        # Master disk configuration
        master_disk = DiskConfig(
            boot_disk_type=dp.get("master_boot_disk_type", "pd-balanced"),
            boot_disk_size_gb=dp.get("master_boot_disk_size_gb", 128),
        )

        # Master node configuration
        master_config = InstanceGroupConfig(
            num_instances=1,
            machine_type_uri=dp.get("master_machine_type", "n2-standard-4"),
            disk_config=master_disk,
        )

        # Worker disk configuration
        worker_disk = DiskConfig(
            boot_disk_type=dp.get("worker_boot_disk_type", "pd-ssd"),
            boot_disk_size_gb=dp.get("worker_boot_disk_size_gb", 500),
            num_local_ssds=dp.get("num_worker_local_ssds", 1),
            local_ssd_interface=dp.get("worker_local_ssd_interface", "NVME"),
        )

        # Worker nodes configuration
        worker_config = InstanceGroupConfig(
            num_instances=dp.get("num_workers", 4),
            machine_type_uri=dp.get("worker_machine_type", "n2-standard-8"),
            disk_config=worker_disk,
        )

        # GCE cluster configuration
        gce_config = GceClusterConfig(
            zone_uri=f"projects/{self.project_id}/zones/{self.zone}",
        )

        # Lifecycle configuration (max_idle)
        max_idle_str = dp.get("max_idle", "1h")
        max_idle_seconds = parse_duration_to_seconds(max_idle_str)
        lifecycle_config = LifecycleConfig(
            idle_delete_ttl=duration_pb2.Duration(seconds=max_idle_seconds),
        )

        # Build cluster config
        cluster_config = ClusterConfig(
            config_bucket=self.staging_bucket,
            gce_cluster_config=gce_config,
            master_config=master_config,
            worker_config=worker_config,
            software_config=software_config,
            lifecycle_config=lifecycle_config,
        )

        # Enable component gateway if specified
        if dp.get("enable_component_gateway", True):
            cluster_config.endpoint_config = dataproc_v1.EndpointConfig(
                enable_http_port_access=True
            )

        # Add initialization actions if specified
        init_actions = dp.get("init_actions", [])
        if init_actions:
            cluster_config.initialization_actions = [
                dataproc_v1.NodeInitializationAction(executable_file=action)
                for action in init_actions
            ]

        return Cluster(
            project_id=self.project_id,
            cluster_name=self.cluster_name,
            config=cluster_config,
        )

    def _cluster_exists(self, cluster_name: str) -> bool:
        """Check if a cluster exists by name."""
        try:
            self.cluster_client.get_cluster(
                project_id=self.project_id,
                region=self.region,
                cluster_name=cluster_name,
            )
            return True
        except NotFound:
            return False

    def cluster_exists(self) -> bool:
        """Check if the job cluster already exists."""
        return self._cluster_exists(self.cluster_name)

    def history_server_exists(self) -> bool:
        """Check if the history server cluster exists."""
        return self._cluster_exists(self.history_cluster_name)

    def create_history_server(self, wait: bool = True) -> Optional[Cluster]:
        """Create the Spark History Server cluster.

        Args:
            wait: Whether to wait for creation to complete

        Returns:
            Cluster object if successful, None otherwise
        """
        if not self.history_server_enabled:
            logger.info("History server is disabled in configuration")
            return None

        if self._cluster_exists(self.history_cluster_name):
            logger.info(f"History server '{self.history_cluster_name}' already exists, reusing it")
            return self.cluster_client.get_cluster(
                project_id=self.project_id,
                region=self.region,
                cluster_name=self.history_cluster_name,
            )

        logger.info(f"Creating Spark History Server '{self.history_cluster_name}'...")
        cluster = self._build_history_server_config()

        try:
            operation: Operation = self.cluster_client.create_cluster(
                project_id=self.project_id,
                region=self.region,
                cluster=cluster,
            )

            if wait:
                logger.info("Waiting for history server creation to complete...")
                result = operation.result()
                logger.info(f"History server '{self.history_cluster_name}' created successfully")
                logger.info(f"Spark event logs will be stored at: {self.spark_log_dir}")
                return result
            else:
                return None

        except AlreadyExists:
            logger.info(f"History server '{self.history_cluster_name}' already exists")
            return self.cluster_client.get_cluster(
                project_id=self.project_id,
                region=self.region,
                cluster_name=self.history_cluster_name,
            )

    def create_cluster(self, wait: bool = True) -> Optional[Cluster]:
        """Create a Dataproc job cluster.

        Args:
            wait: Whether to wait for cluster creation to complete

        Returns:
            Cluster object if successful, None otherwise
        """
        if self.cluster_exists():
            logger.info(f"Cluster '{self.cluster_name}' already exists, reusing it")
            return self.cluster_client.get_cluster(
                project_id=self.project_id,
                region=self.region,
                cluster_name=self.cluster_name,
            )

        logger.info(f"Creating Dataproc cluster '{self.cluster_name}'...")

        # Log key configuration
        dp = self.dataproc_config
        logger.info(f"  Image: {dp.get('image_version')}")
        logger.info(f"  Workers: {dp.get('num_workers')} x {dp.get('worker_machine_type')}")
        logger.info(f"  Max idle: {dp.get('max_idle', '1h')}")
        logger.info(f"  Tier: {dp.get('tier', 'standard')}")

        cluster = self._build_cluster_config()

        try:
            operation: Operation = self.cluster_client.create_cluster(
                project_id=self.project_id,
                region=self.region,
                cluster=cluster,
            )

            if wait:
                logger.info("Waiting for cluster creation to complete...")
                result = operation.result()
                logger.info(f"Cluster '{self.cluster_name}' created successfully")
                return result
            else:
                return None

        except AlreadyExists:
            logger.info(f"Cluster '{self.cluster_name}' already exists")
            return self.cluster_client.get_cluster(
                project_id=self.project_id,
                region=self.region,
                cluster_name=self.cluster_name,
            )

    def delete_cluster(self, wait: bool = True) -> bool:
        """Delete the Dataproc job cluster.

        Args:
            wait: Whether to wait for deletion to complete

        Returns:
            True if deletion was successful or cluster didn't exist
        """
        if not self.cluster_exists():
            logger.info(f"Cluster '{self.cluster_name}' does not exist")
            return True

        logger.info(f"Deleting Dataproc cluster '{self.cluster_name}'...")

        try:
            operation = self.cluster_client.delete_cluster(
                project_id=self.project_id,
                region=self.region,
                cluster_name=self.cluster_name,
            )

            if wait:
                operation.result()
                logger.info(f"Cluster '{self.cluster_name}' deleted successfully")

            return True

        except NotFound:
            logger.info(f"Cluster '{self.cluster_name}' not found")
            return True
        except Exception as e:
            logger.error(f"Failed to delete cluster: {e}")
            return False

    def delete_history_server(self, wait: bool = True) -> bool:
        """Delete the Spark History Server cluster.

        Args:
            wait: Whether to wait for deletion to complete

        Returns:
            True if deletion was successful or cluster didn't exist
        """
        if not self._cluster_exists(self.history_cluster_name):
            logger.info(f"History server '{self.history_cluster_name}' does not exist")
            return True

        logger.info(f"Deleting history server '{self.history_cluster_name}'...")

        try:
            operation = self.cluster_client.delete_cluster(
                project_id=self.project_id,
                region=self.region,
                cluster_name=self.history_cluster_name,
            )

            if wait:
                operation.result()
                logger.info(f"History server '{self.history_cluster_name}' deleted successfully")

            return True

        except NotFound:
            logger.info(f"History server '{self.history_cluster_name}' not found")
            return True
        except Exception as e:
            logger.error(f"Failed to delete history server: {e}")
            return False

    def get_cluster_info(self) -> Optional[Dict[str, Any]]:
        """Get cluster information for reporting."""
        try:
            cluster = self.cluster_client.get_cluster(
                project_id=self.project_id,
                region=self.region,
                cluster_name=self.cluster_name,
            )
            return {
                "cluster_name": cluster.cluster_name,
                "project_id": cluster.project_id,
                "image_version": cluster.config.software_config.image_version,
                "worker_count": cluster.config.worker_config.num_instances,
                "worker_machine_type": cluster.config.worker_config.machine_type_uri,
                "master_machine_type": cluster.config.master_config.machine_type_uri,
            }
        except NotFound:
            return None

    def get_history_server_url(self) -> Optional[str]:
        """Get the Spark History Server web UI URL."""
        if not self.history_server_enabled:
            return None

        try:
            cluster = self.cluster_client.get_cluster(
                project_id=self.project_id,
                region=self.region,
                cluster_name=self.history_cluster_name,
            )
            # The history server URL is available via component gateway
            endpoints = cluster.config.endpoint_config.http_ports
            if "Spark History Server" in endpoints:
                return endpoints["Spark History Server"]
            return None
        except NotFound:
            return None
