# Spark Executor Sizing Guide for TPC-DS Benchmarks on Dataproc

## Executive Summary

This guide provides concrete recommendations for Spark executor sizing on Google Cloud Dataproc for TPC-DS benchmark workloads. The recommendations are based on extensive research of Google Cloud best practices, Apache Spark optimization guides, and TPC-DS benchmark results from 2024-2026.

---

## Table of Contents

1. [Key Principles](#key-principles)
2. [Optimal Configuration Parameters](#optimal-configuration-parameters)
3. [Machine Type Specific Recommendations](#machine-type-specific-recommendations)
4. [Memory Overhead Calculations](#memory-overhead-calculations)
5. [Common Pitfalls to Avoid](#common-pitfalls-to-avoid)
6. [TPC-DS Specific Considerations](#tpc-ds-specific-considerations)
7. [Calculation Formulas](#calculation-formulas)
8. [References](#references)

---

## Key Principles

### 1. Dataproc Default Configuration
- **By default, Dataproc configures 2 YARN containers per VM**
- Dynamic allocation is enabled by default on Dataproc clusters
- Default settings set `spark.executor.cores` and `spark.executor.memory` to approximately half of VM resources
- **Recommendation**: Leverage Dataproc defaults with dynamic allocation rather than manual sizing

### 2. HDFS Throughput Optimization
- **HDFS achieves optimal throughput with ~5 tasks per executor**
- More than 5 cores per executor degrades HDFS I/O throughput
- HDFS client struggles with excessive concurrent threads
- **Recommendation**: Use 4-5 cores per executor for optimal I/O

### 3. Memory Threshold - 32GB Limit
- JVM uses Compressed OOPs (Ordinary Object Pointers) when heap < 32GB
- Above 32GB, JVM automatically disables compressed OOPs
- This causes 64-bit pointers (vs 32-bit), doubling memory consumption
- Results in 5-15% throughput degradation and increased GC overhead
- **Recommendation**: Keep executor memory below 32GB

### 4. Reserve Resources for System
- Leave 1 core per node for Hadoop/YARN daemons
- Reserve 1 executor for Application Master (YARN mode)
- YARN reserves 75-80% of total VM memory (20-25% for OS/daemons)

---

## Optimal Configuration Parameters

### spark.executor.cores
**Recommended: 4-5 cores**

- **Rationale**:
  - HDFS I/O throughput optimization (5 tasks max)
  - Balance between parallelism and GC overhead
  - Avoids thread contention within executor

- **Avoid**:
  - Too few cores (1-2): Creates too many small executors, increases I/O cost
  - Too many cores (>5): Degrades HDFS throughput, increases GC overhead

### spark.executor.memory
**Recommended: 2-6 GB per core, typically 4-5 GB/core**

- **Rationale**:
  - Provides sufficient memory for task execution
  - Avoids excessive GC overhead
  - Stays well below 32GB compressed OOPs threshold

- **Common Configurations**:
  - 4 cores × 5GB = 20GB total
  - 5 cores × 4GB = 20GB total
  - 5 cores × 6GB = 30GB total (max recommended)

### spark.executor.memoryOverhead
**Formula**: `max(384MB, 0.10 × spark.executor.memory)`

**Default**: 10% of executor memory, minimum 384MB

- **For PySpark**: Increase to 15-25% due to Python process overhead
- **For Arrow/Native Libraries**: Increase to 20-25%
- **WARNING**: On Dataproc, do NOT manually override this unless necessary, as it may break Dataproc's automatic calculations

---

## Machine Type Specific Recommendations

### N2-Standard-4 (4 vCPUs, 16GB RAM)

**Default Dataproc Configuration**:
- YARN memory: ~12.8GB (80% of 16GB)
- Available cores: 3 (reserve 1 for daemons)
- Default: 2 executors per node

**Recommended Manual Configuration** (if not using dynamic allocation):
```
spark.executor.cores: 2
spark.executor.memory: 5g
spark.executor.memoryOverhead: 512m
Number of executors per node: 1
```

**Calculation**:
- Total memory per executor = 5GB + 512MB = 5.5GB
- Leaves ~7GB for second executor or YARN overhead
- 2 cores per executor allows good parallelism

**Notes**:
- Small machine type, best for testing or small-scale workloads
- Consider n2-highmem-4 if memory errors occur

---

### N2-Standard-8 (8 vCPUs, 32GB RAM)

**Default Dataproc Configuration**:
- YARN memory: ~25.6GB (80% of 32GB)
- Available cores: 7 (reserve 1 for daemons)
- Default: 2 executors per node

**Recommended Manual Configuration** (if not using dynamic allocation):
```
spark.executor.cores: 4
spark.executor.memory: 10g
spark.executor.memoryOverhead: 1g
Number of executors per node: 1-2
```

**Calculation**:
- Option 1 (1 executor): 4 cores × 10GB = good for memory-intensive queries
- Option 2 (2 executors): 3-4 cores × 5GB each = better parallelism
- Total memory per executor = 10GB + 1GB overhead = 11GB

**For TPC-DS Benchmarks**:
```
spark.executor.cores: 5
spark.executor.memory: 9g
spark.executor.memoryOverhead: 1g
Number of executors per node: 1
```

**Notes**:
- Good balance for medium-scale TPC-DS (100GB-1TB)
- 1 executor with 5 cores optimal for HDFS throughput
- Leaves memory for shuffle operations

---

### N2-Standard-16 (16 vCPUs, 64GB RAM)

**Default Dataproc Configuration**:
- YARN memory: ~51.2GB (80% of 64GB)
- Available cores: 15 (reserve 1 for daemons)
- Default: 2 executors per node (but 3 is optimal)

**Recommended Manual Configuration** (if not using dynamic allocation):
```
spark.executor.cores: 5
spark.executor.memory: 15g
spark.executor.memoryOverhead: 2g
Number of executors per node: 3
```

**Calculation**:
- 3 executors × 5 cores = 15 cores (fully utilized)
- 3 executors × (15GB + 2GB overhead) = 51GB total
- Stays well below 32GB compressed OOPs threshold per executor

**For Large TPC-DS Benchmarks (3TB+)**:
```
spark.executor.cores: 5
spark.executor.memory: 16g
spark.executor.memoryOverhead: 2g
Number of executors per node: 3
```

**Alternative Configuration** (fewer, larger executors):
```
spark.executor.cores: 7
spark.executor.memory: 22g
spark.executor.memoryOverhead: 3g
Number of executors per node: 2
```
**Note**: This violates the 5-core HDFS guideline but may work for non-I/O intensive queries

---

### N2-Highmem-4 (4 vCPUs, 32GB RAM)

**Default Dataproc Configuration**:
- YARN memory: ~25.6GB (80% of 32GB)
- Available cores: 3 (reserve 1 for daemons)

**Recommended Configuration**:
```
spark.executor.cores: 2-3
spark.executor.memory: 10g
spark.executor.memoryOverhead: 1117m
Number of executors per node: 2
```

**Notes**:
- Use when n2-standard-4 encounters memory errors
- Better for memory-intensive TPC-DS queries
- Memory overhead = 11171MB × 0.1 = 1117MB (Dataproc calculation)

---

### N2-Highmem-8 (8 vCPUs, 64GB RAM)

**Recommended Configuration**:
```
spark.executor.cores: 4-5
spark.executor.memory: 24g
spark.executor.memoryOverhead: 3g
Number of executors per node: 2
```

**Best for**: Medium to large TPC-DS with memory-intensive queries (50K-100K tasks)

---

### N2-Highmem-16 (16 vCPUs, 128GB RAM)

**Recommended Configuration**:
```
spark.executor.cores: 5
spark.executor.memory: 30g
spark.executor.memoryOverhead: 4g
Number of executors per node: 3
```

**Best for**: Very large TPC-DS (10TB+) with >10K apps

**Alternative** (stay below 32GB):
```
spark.executor.cores: 5
spark.executor.memory: 28g
spark.executor.memoryOverhead: 3g
Number of executors per node: 3-4
```

---

## Memory Overhead Calculations

### Formula
```
spark.executor.memoryOverhead = max(384MB, memoryOverheadFactor × spark.executor.memory)
```

Where:
- **memoryOverheadFactor**: Default = 0.10 (10%)
- **Minimum**: 384MB

### Total YARN Container Memory
```
Total Container Memory = spark.executor.memory + spark.executor.memoryOverhead
```

### YARN Memory per Node
**Dataproc Default**:
```
yarn.nodemanager.resource.memory-mb = 0.75 to 0.80 × Total VM Memory
```

Typically:
- 75-80% for YARN containers
- 20-25% reserved for OS, daemons, page cache

### Examples

**Example 1: n2-standard-8**
- VM Memory: 32GB
- YARN Memory: 32GB × 0.80 = 25.6GB = 26,214MB
- Executor Memory: 10GB = 10,240MB
- Memory Overhead: max(384MB, 10,240MB × 0.10) = 1,024MB
- Total per Executor: 10,240MB + 1,024MB = 11,264MB
- Executors per Node: 26,214MB ÷ 11,264MB = 2.3 → **2 executors**

**Example 2: n2-standard-16 with 3 executors**
- VM Memory: 64GB
- YARN Memory: 64GB × 0.80 = 51.2GB = 52,429MB
- Target: 3 executors per node
- Memory per Executor: 52,429MB ÷ 3 = 17,476MB
- Overhead (10%): 17,476MB × 0.10 = 1,748MB
- Executor Memory: 17,476MB - 1,748MB = 15,728MB ≈ **15GB**
- Configuration: `spark.executor.memory=15g`, `spark.executor.memoryOverhead=2g`

---

## Common Pitfalls to Avoid

### 1. Too Many Small Executors
**Problem**: Using 1-2 cores per executor
**Consequences**:
- High I/O overhead
- More task serialization overhead
- Inefficient resource utilization
- More executor JVMs = more memory overhead

**Example**: 16-core node with 16 executors of 1 core each

### 2. Too Few Large Executors
**Problem**: Using >5 cores per executor or >32GB memory
**Consequences**:
- Degraded HDFS I/O throughput (>5 cores)
- Excessive GC overhead
- Long stop-the-world GC pauses
- Disabled compressed OOPs (>32GB)
- 5-15% throughput loss

**Example**: 16-core node with 1 executor of 16 cores and 60GB memory

### 3. Ignoring Memory Overhead
**Problem**: Setting executor memory too high without accounting for overhead
**Consequences**:
- YARN kills containers for exceeding memory limits
- "Container killed by YARN for exceeding memory limits" errors
- Job failures

**Solution**: Always calculate total = executor.memory + memoryOverhead

### 4. Manually Overriding Dataproc Defaults Incorrectly
**Problem**: Setting `spark.executor.memoryOverhead` or other properties that conflict with Dataproc's automatic configuration
**Consequences**:
- Breaks Dataproc's resource allocation magic
- Unexpected container kills
- Poor cluster utilization

**Solution**:
- Use dynamic allocation (Dataproc default)
- If manual configuration needed, use highmem machines for memory errors
- Don't override `spark.executor.memoryOverhead` on Dataproc unless absolutely necessary

### 5. Not Reserving Resources
**Problem**: Allocating 100% of cores/memory to executors
**Consequences**:
- No resources for NodeManager, DataNode, other daemons
- System instability
- Poor performance

**Solution**: Reserve 1 core + 20-25% memory per node

### 6. Using Static Allocation for Variable Workloads
**Problem**: Disabling dynamic allocation and manually setting executor count
**Consequences**:
- Resource underutilization during low-demand phases
- Resource shortage during high-demand phases
- Higher costs

**Solution**: Use dynamic allocation (default on Dataproc) unless doing controlled benchmarking

---

## TPC-DS Specific Considerations

### Benchmark vs. Production Configuration

**For Benchmarking** (reproducible results):
```properties
# Disable dynamic allocation for consistent measurements
spark.dynamicAllocation.enabled=false

# Static executor configuration
spark.executor.instances=<calculated value>
spark.executor.cores=5
spark.executor.memory=<based on machine type>
spark.executor.memoryOverhead=<10% of memory>

# Additional TPC-DS optimizations
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
```

**For Production** (cost-efficient):
```properties
# Enable dynamic allocation (Dataproc default)
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.minExecutors=<based on min workload>
spark.dynamicAllocation.maxExecutors=<based on max workload>
spark.dynamicAllocation.initialExecutors=<based on typical workload>

# Let Dataproc manage executor sizing
# Don't override executor.cores or executor.memory unless necessary
```

### TPC-DS Workload Characteristics

1. **Query Complexity**: TPC-DS has 99 queries with varying complexity
   - Some are I/O intensive (scanning large tables)
   - Some are CPU intensive (complex aggregations, joins)
   - Some are memory intensive (large shuffles)

2. **Configuration Strategy**:
   - **I/O Intensive Queries**: Favor 5 cores/executor for HDFS throughput
   - **CPU Intensive Queries**: Can use 4 cores/executor with higher parallelism
   - **Memory Intensive Queries**: Increase executor memory (up to 30GB)

3. **Common TPC-DS Configurations from Benchmarks**:
   ```
   # Small scale (100GB-1TB)
   spark.executor.cores: 4-5
   spark.executor.memory: 10-20g

   # Medium scale (1TB-3TB)
   spark.executor.cores: 5
   spark.executor.memory: 16-24g

   # Large scale (3TB+)
   spark.executor.cores: 5
   spark.executor.memory: 24-30g
   ```

### Performance Optimizations for TPC-DS

1. **Memory Configuration**:
   - Allocate 35-45% of total memory to on-heap and off-heap
   - For 1TB dataset: Use machines with 30-60GB per executor

2. **Shuffle Optimization**:
   - Add local SSDs (1 disk per 4 CPUs recommended)
   - Configure `spark.local.dir` to use local SSDs
   - 60% performance boost observed on shuffle-heavy queries

3. **Partitioning**:
   - Reduce `numPartitions` for smaller datasets
   - For 10GB: ~20 partitions to reduce task overhead
   - For 1TB: ~200-1000 partitions

4. **Query-Specific Tuning**:
   - Query 64: Benefits significantly from external disks (60% improvement)
   - Query 78: Very shuffle-intensive, benefits from static allocation

---

## Calculation Formulas

### Complete Executor Sizing Formula

Given a cluster with:
- **N** nodes
- **C** cores per node
- **M** GB memory per node

**Step 1: Calculate available resources per node**
```
Available cores per node = C - 1  (reserve 1 for daemons)
Available memory per node = M × 0.80  (YARN takes 80%, OS takes 20%)
```

**Step 2: Choose executor cores**
```
Executor cores = 4 or 5  (recommended for HDFS throughput)
```

**Step 3: Calculate executors per node**
```
Executors per node = floor(Available cores / Executor cores)
```

**Step 4: Calculate memory per executor**
```
Memory per executor = floor(Available memory / Executors per node)
Account for overhead:
  Executor memory = Memory per executor / 1.10  (leave 10% for overhead)
  Memory overhead = Memory per executor × 0.10
```

**Step 5: Calculate total executors**
```
Total executors = (Executors per node × N) - 1  (reserve 1 for AM in YARN)
```

### Example: 10-node cluster with n2-standard-16

**Given**:
- N = 10 nodes
- C = 16 cores per node
- M = 64 GB per node

**Step 1**:
```
Available cores = 16 - 1 = 15 cores
Available memory = 64 × 0.80 = 51.2 GB
```

**Step 2**:
```
Executor cores = 5
```

**Step 3**:
```
Executors per node = floor(15 / 5) = 3
```

**Step 4**:
```
Memory per executor = 51.2 GB / 3 = 17.07 GB
Executor memory = 17.07 / 1.10 = 15.5 GB ≈ 15 GB
Memory overhead = 17.07 - 15.5 = 1.5 GB ≈ 2 GB
```

**Step 5**:
```
Total executors = (3 × 10) - 1 = 29
```

**Final Configuration**:
```
--num-executors 29
--executor-cores 5
--executor-memory 15g
spark.executor.memoryOverhead=2g
```

---

## Quick Reference Table

| Machine Type | vCPUs | RAM | Executors/Node | Cores/Exec | Memory/Exec | Overhead | Total/Exec |
|--------------|-------|-----|----------------|------------|-------------|----------|------------|
| n2-standard-4 | 4 | 16GB | 1 | 2 | 5g | 512m | 5.5g |
| n2-standard-8 | 8 | 32GB | 1 | 5 | 9g | 1g | 10g |
| n2-standard-8 | 8 | 32GB | 2 | 3-4 | 5g | 512m | 5.5g |
| n2-standard-16 | 16 | 64GB | 3 | 5 | 15g | 2g | 17g |
| n2-standard-16 | 16 | 64GB | 2 | 7 | 22g | 3g | 25g |
| n2-highmem-4 | 4 | 32GB | 2 | 2 | 10g | 1117m | 11g |
| n2-highmem-8 | 8 | 64GB | 2 | 4 | 24g | 3g | 27g |
| n2-highmem-16 | 16 | 128GB | 3 | 5 | 30g | 4g | 34g |
| n2-highmem-16 | 16 | 128GB | 4 | 4 | 28g | 3g | 31g |

**Notes**:
- All configurations keep executor memory < 32GB for compressed OOPs
- Cores/Exec optimized for HDFS (≤5 cores)
- Overhead calculated at 10% (PySpark may need 15-25%)

---

## References

### Google Cloud Dataproc Documentation
- [Spark Job Tuning Tips](https://docs.cloud.google.com/dataproc/docs/support/spark-job-tuning) - Official Dataproc tuning guide
- [Dataproc Best Practices Guide](https://cloud.google.com/blog/topics/developers-practitioners/dataproc-best-practices-guide) - Google Cloud Blog
- [Dataproc Performance Enhancements](https://docs.cloud.google.com/dataproc/docs/guides/performance-enhancements) - Performance features in Dataproc 2.0.69+ and 2.1.17+

### Spark Executor Sizing
- [Distribution of Executors, Cores and Memory for Spark Application](https://spoddutur.github.io/spark-notes/distribution_of_executors_cores_and_memory_for_spark_application.html) - Comprehensive calculation guide
- [How to Tune Your Apache Spark Jobs (Part 2)](https://blog.cloudera.com/how-to-tune-your-apache-spark-jobs-part-2/) - Cloudera's HDFS throughput recommendations
- [Tuning Spark Applications to Efficiently Utilize Dataproc Cluster](https://medium.com/paypal-tech/tuning-spark-applications-to-efficiently-utilize-dataproc-cluster-11bd51b36fe1) - PayPal's Dataproc tuning experience
- [Optimal Resource Allocation for Spark Applications](https://medium.com/@hgarg1010/optimal-resource-allocation-for-spark-applications-c6eb8a05a7f6) - Resource allocation best practices

### Memory and GC Optimization
- [Taming the Spark Memory Beast](https://medium.com/@idohlevi/taming-the-spark-memory-16a631148ec9) - GC overhead and executor sizing
- [Understanding Executor Memory Overhead in Spark](https://sparkbyexamples.com/spark/spark-executor-memory-overhead-understanding/) - Memory overhead calculations
- [How to Tune Spark Executor Memory for Peak Performance](https://www.getgalaxy.io/learn/glossary/spark-executor-memory-tuning) - Memory tuning guide
- [Compressed OOPs in JVM](https://www.baeldung.com/jvm-compressed-oops) - 32GB threshold explanation

### TPC-DS Benchmarks
- [IBM Spark TPC-DS Performance Test](https://github.com/IBM/spark-tpc-ds-performance-test) - TPC-DS benchmark toolkit
- [Running TPC-DS Benchmarks for Spark](https://blog.devgenius.io/running-tpc-ds-benchmarks-for-spark-31ce8a613619) - TPC-DS setup and configuration
- [Spark TPC-DS Benchmark (Palantir)](https://github.com/palantir/spark-tpcds-benchmark) - Benchmark utility with examples
- [Intel Spark Tuning Guide on Xeon](https://cdrdv2-public.intel.com/686403/spark-tuning-guide-on-xeon.pdf) - TPC-DS optimization (35-45% memory allocation)

### Dynamic Allocation
- [Dynamic Allocation - The Internals of Spark Core](https://books.japila.pl/apache-spark-internals/dynamic-allocation/) - How dynamic allocation works
- [Mastering Dynamic Allocation in Apache Spark](https://dev.to/krillinkills/mastering-dynamic-allocation-in-apache-spark-a-practical-guide-with-real-world-insights-1ak4) - Dynamic vs. static allocation trade-offs

### Machine Types and Configuration
- [GCP Dataproc and Apache Spark Tuning](https://mkuthan.github.io/blog/2022/03/24/gcp-dataproc-spark-tuning/) - Dataproc-specific configurations
- [GCP Machine Families Resource Guide](https://docs.cloud.google.com/compute/docs/machine-resource) - N2 machine specifications
- [GCP General-Purpose Machine Family](https://docs.cloud.google.com/compute/docs/general-purpose-machines) - N2-standard and N2-highmem details

---

## Document Version

- **Version**: 1.0
- **Last Updated**: 2026-01-24
- **Research Period**: 2024-2026
- **Target Dataproc Versions**: 2.0.69+ and 2.1.17+

---

## Summary Recommendations

### For TPC-DS Benchmarks on Dataproc

1. **Use Dynamic Allocation** (Dataproc default) unless benchmarking for reproducibility
2. **Set executor cores to 4-5** for optimal HDFS throughput
3. **Keep executor memory < 32GB** to maintain compressed OOPs
4. **Use 2-6 GB memory per core**, typically 4-5 GB/core
5. **Reserve resources**: 1 core and 20-25% memory for system
6. **Memory overhead**: 10% of executor memory (15-25% for PySpark)
7. **Machine type selection**:
   - Small workloads: n2-standard-4 or n2-standard-8
   - Medium workloads (100GB-1TB): n2-standard-8 or n2-standard-16
   - Large workloads (1TB+): n2-standard-16 or n2-highmem-8/16
8. **Add local SSDs**: 1 disk per 4 CPUs for shuffle-heavy workloads
9. **Don't override** Dataproc defaults unless you have specific requirements
10. **Monitor and adjust**: Use Spark UI and YARN RM to validate configuration

### Quick Start for Common Scenarios

**Testing/Development (Small scale)**:
```bash
# Let Dataproc handle everything
gcloud dataproc clusters create test-cluster \
  --region=us-central1 \
  --master-machine-type=n2-standard-4 \
  --worker-machine-type=n2-standard-4 \
  --num-workers=2
```

**TPC-DS Benchmark (1TB, n2-standard-8)**:
```bash
gcloud dataproc clusters create tpcds-cluster \
  --region=us-central1 \
  --master-machine-type=n2-standard-8 \
  --worker-machine-type=n2-standard-8 \
  --num-workers=10 \
  --properties="spark:spark.dynamicAllocation.enabled=false,spark:spark.executor.cores=5,spark:spark.executor.memory=9g,spark:spark.executor.memoryOverhead=1g"
```

**TPC-DS Production (3TB+, n2-standard-16 with local SSDs)**:
```bash
gcloud dataproc clusters create tpcds-prod \
  --region=us-central1 \
  --master-machine-type=n2-standard-16 \
  --worker-machine-type=n2-standard-16 \
  --num-workers=20 \
  --worker-boot-disk-size=500GB \
  --num-worker-local-ssds=4 \
  --properties="spark:spark.dynamicAllocation.enabled=true"
  # Let dynamic allocation handle executor sizing
```
