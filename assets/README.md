# TPC-DS Data Generation Assets

This directory contains pre-built assets required for TPC-DS data generation.

## Required Files

| File | Description |
|------|-------------|
| `spark-sql-perf-assembly-0.5.1.jar` | Fat JAR with spark-sql-perf and all dependencies |
| `tpcds-kit-1.0.0.tar.gz` | Native dsdgen binary for Linux |
| `manifest.json` | Version tracking and build metadata |

## Building Assets

If assets are not present, run the build script:

```bash
make build-assets
# Or directly:
./scripts/build_assets.sh
```

The script clones repositories and builds in `tmp/` directory (git-ignored). Final assets are placed in this `assets/` directory.

### Prerequisites for Building

- Linux x86_64 system
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

## Distribution

Once built, these assets can be:
1. Committed to the repository (if size permits)
2. Uploaded to a GCS bucket for distribution
3. Shared via any file hosting service

Users of this tool do NOT need to rebuild these assets.

## Version Information

The assets are built from:
- **spark-sql-perf**: https://github.com/databricks/spark-sql-perf
- **tpcds-kit**: https://github.com/databricks/tpcds-kit

See `manifest.json` for exact versions and build date.
