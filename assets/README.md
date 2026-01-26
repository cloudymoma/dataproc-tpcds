# TPC-DS Data Generation Assets

This directory contains pre-built assets required for TPC-DS data generation. These assets are **ready to use** - no additional setup required.

## Included Files

| File | Description |
|------|-------------|
| `spark-sql-perf-assembly-0.5.1.jar` | Fat JAR with spark-sql-perf and all dependencies |
| `tpcds-datagen-1.0.0.jar` | Custom main class for data generation |
| `tpcds-kit-1.0.0.tar.gz` | Native dsdgen binary for Linux x86_64 |
| `manifest.json` | Version tracking and build metadata |

## Building Assets (Optional)

The pre-built assets target **Linux x86_64**, which is the default architecture for GCP Dataproc clusters. You only need to rebuild if your Dataproc cluster uses a different architecture (e.g., ARM-based instances).

```bash
make build-assets
# Or directly:
./scripts/build_assets.sh
```

The script clones repositories and builds in `tmp/` directory (git-ignored). Final assets are placed in this `assets/` directory.

### Prerequisites for Building

**Important:** Build on a system matching your **Dataproc cluster's architecture**, not your local machine.

- Linux system matching target cluster architecture (x86_64 or ARM)
- Git
- SBT (Scala Build Tool)
- GCC and make
- Java 8 or 11

### Installing Prerequisites (Debian/Ubuntu)

```bash
# Install Java
sudo apt-get install openjdk-11-jdk

# Install SBT
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x99E82A75642AC823" | sudo apt-key add
sudo apt-get update
sudo apt-get install sbt

# Install build tools
sudo apt-get install git make gcc
```

## Usage

These assets are automatically uploaded to GCS and used by your Dataproc cluster during data generation (`make data-gen`). No manual steps required.

## Version Information

The assets are built from:
- **spark-sql-perf**: https://github.com/databricks/spark-sql-perf
- **tpcds-kit**: https://github.com/databricks/tpcds-kit

See `manifest.json` for exact versions and build date.
