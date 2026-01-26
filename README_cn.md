[English](README.md) | 简体中文

# GCP Dataproc TPC-DS 自动化基准测试工具

一个轻量级、高度自动化的命令行工具，用于在 Google Cloud Dataproc 上运行 TPC-DS（1TB 规模）性能基准测试，只需一个配置文件即可完成所有操作。

## 特性

- **简单**: 纯 Python 和 Shell 实现，无复杂依赖
- **无状态**: 无需 Hive Metastore - 使用 GCS + Temporary Views
- **可观测**: 结构化结果存储在 BigQuery 中，便于比较不同机器类型和 Spark 配置
- **自动化**: 一条命令完成集群创建、数据生成、查询执行和指标上报

## 分步基准测试指南

本指南将引导您完成从设置到清理的完整基准测试流程。

### 前置条件

1. **Google Cloud SDK** 已安装并配置（`gcloud` CLI）
2. **Python 3.8+** 及 pip
3. **GCP 项目** 需启用以下 API：
   - Compute Engine API
   - Dataproc API
   - Cloud Storage API
   - BigQuery API

   使用以下命令启用所有必需的 API：
   ```bash
   gcloud services enable compute.googleapis.com \
       dataproc.googleapis.com \
       storage.googleapis.com \
       bigquery.googleapis.com
   ```

4. **GCS 存储桶** 名称用于存储数据和脚本（如果不存在，将自动在与集群相同的区域中创建）

### 步骤 1：安装

```bash
# 克隆仓库
git clone <repository-url>
cd dataproc-tpcds

# 安装 Python 依赖
make install

# 或使用自定义 Python 版本/路径
make PYTHON=/usr/bin/python3.11 install

# 使用 Google Cloud 进行身份验证
gcloud auth application-default login
```

> **注意**：`PYTHON` 变量可以设置为任何 Python 3.8+ 解释器路径。
> 所有 pip 操作自动使用 `$(PYTHON) -m pip` 以保持一致性。

### 步骤 2：配置

编辑 `conf.yaml` 填入您的设置：

```yaml
gcp:
  project_id: "your-project-id"
  region: "us-central1"
  staging_bucket: "gs://your-bucket"

dataproc:
  cluster_name: "tpcds-bench"
  num_workers: 4
  worker_machine_type: "n2-standard-8"

benchmark:
  scale_factor: 1000  # 1TB（测试时使用 1-10）
  data_path: "gs://your-bucket/tpcds-data/1T"
  data_format: "parquet"

# 数据生成配置（spark-sql-perf）
datagen:
  num_partitions: 0              # 自动计算（约 128MB 文件）
  partition_tables: true         # 分区大表
  spark_sql_perf_version: "0.5.1"
  tpcds_kit_version: "1.0.0"
```

### 步骤 3：验证配置

```bash
# 验证配置但不执行任何操作
make dry-run
```

### 步骤 4：生成 TPC-DS 数据

> **重要：** 数据生成和基准测试是**独立的操作**。必须在运行基准测试之前生成数据。每个规模因子只需生成**一次**数据 - 您可以对同一数据集运行多次基准测试。

数据使用 [spark-sql-perf](https://github.com/databricks/spark-sql-perf) 和原生 `dsdgen` 二进制文件生成，确保完全符合 TPC-DS 规范（所有 24 个表的正确 schema）。

```bash
# 首先创建集群
make cluster-create

# 使用 spark-sql-perf 在 Dataproc 上生成数据
make data-gen

# 验证数据是否已生成（检查全部 24 个表）
make data-check
```

**首次设置：** 生成数据前，需要预构建的资产（JAR 和 dsdgen 二进制文件）。可以：
1. 从 releases 下载预构建资产
2. 自行构建：`make build-assets`（需要 Linux 系统和 SBT、GCC、Make）

**数据复用：** 生成后，同一数据集可用于多次基准测试运行。在 conf.yaml 中设置 `skip_data_gen: true` 以在后续运行中跳过数据生成，或者直接不再运行 `make data-gen`。

### 步骤 5：运行基准测试

> **前提条件：** 确保在运行基准测试之前已生成数据（步骤 4）。

```bash
# 交互模式（结束时提示清理集群）
make run

# 或完成后自动删除集群（推荐以节省成本）
make run-auto-delete

# 调试时使用详细模式
make run-verbose
```

### 步骤 6：查看结果

结果存储在 BigQuery 中：

```bash
# 打开 BigQuery 控制台
open "https://console.cloud.google.com/bigquery?project=your-project-id"
```

查询您的结果：

```sql
-- 查看所有基准测试运行记录
SELECT * FROM `your-project.tpcds_metrics.benchmark_history`
ORDER BY run_timestamp DESC
LIMIT 100;

-- 比较不同运行之间的查询性能
SELECT
  batch_id,
  query_name,
  duration_sec,
  worker_count,
  worker_machine_type
FROM `your-project.tpcds_metrics.benchmark_history`
WHERE status = 'SUCCESS'
ORDER BY query_name, run_timestamp DESC;
```

### 步骤 7：清理资源

**重要：** 清理资源以避免持续产生费用！

```bash
# 删除基准测试集群
make cluster-delete

# 删除 history server（如果启用）
make history-server-delete

# 可选：从 GCS 删除生成的数据
gsutil -m rm -r gs://your-bucket/tpcds-data/

# 可选：删除 BigQuery 数据集
bq rm -r -f your-project:tpcds_metrics
```

详细清理说明请参阅[清理指南](#清理指南)。

---

## 快速开始（5 分钟）

想要快速开始的用户：

```bash
# 1. 克隆并安装
git clone <repository-url> && cd dataproc-tpcds
make install
gcloud auth application-default login

# 2. 编辑 conf.yaml 填入您的项目/存储桶
vim conf.yaml

# 3. 创建集群并生成数据（仅首次需要）
make cluster-create
make data-gen

# 4. 运行基准测试并自动清理
make run-auto-delete
```

> **注意：** 数据生成（步骤 3）每个 scale factor 只需运行一次。后续基准测试运行时，跳过步骤 3，直接运行 `make run-auto-delete` 即可。

## Make 目标

所有常用操作都可通过 Make 执行：

```bash
make help              # 显示所有可用目标

# 设置
make install           # 安装生产依赖
make install-dev       # 安装开发依赖（pytest 等）
make quick-start       # 首次设置：安装 + 验证

# 运行基准测试
make run               # 运行完整基准测试（交互式）
make dry-run           # 验证配置但不执行
make run-auto-delete   # 运行并在完成后自动删除集群
make run-verbose       # 详细日志运行

# 测试
make test              # 运行所有测试
make test-cov          # 运行测试并生成覆盖率报告
make check-syntax      # 验证 Python 语法
make full-test         # 运行所有测试

# 集群操作
make cluster-create    # 仅创建 Dataproc 集群
make cluster-delete    # 删除 Dataproc 集群
make cluster-status    # 检查集群状态
make cluster-info      # 显示集群配置

# 数据操作
make data-gen          # 使用 spark-sql-perf 生成 TPC-DS 数据
make data-check        # 检查数据是否存在及完整性
make data-tables       # 列出可用表
make build-assets      # 构建 spark-sql-perf 资产（一次性设置）

# BigQuery
make bq-setup          # 创建 BQ 数据集/表
make bq-schema         # 显示表 schema

# 工具
make validate          # 运行所有验证检查
make clean             # 删除缓存文件
make list-queries      # 列出可用 SQL 查询
make show-query QUERY=q1  # 显示指定查询
```

## 数据生成

本工具使用 Databricks 的 [spark-sql-perf](https://github.com/databricks/spark-sql-perf) 进行 TPC-DS 数据生成。这种方式提供：

- **完全符合 TPC-DS 规范**: 所有 24 个表的正确 schema
- **原生 dsdgen 二进制文件**: 使用官方 TPC-DS 数据生成器
- **分布式生成**: 在 Dataproc 集群上运行，具有可扩展性
- **优化分区**: 自动计算，目标约 128MB Parquet 文件

### 配置

```yaml
datagen:
  num_partitions: 0              # 自动计算（scale_factor * 8）
  partition_tables: true         # 分区大型事实表
  cluster_by_partition_columns: true
  spark_sql_perf_version: "0.5.1"
  tpcds_kit_version: "1.0.0"
```

### 分区计算

分区数自动计算，目标约 128MB Parquet 文件：

| 规模因子 | 分区数 | 大约文件大小 |
|----------|--------|-------------|
| 1 GB | 100（最小） | ~10MB |
| 100 GB | 800 | ~128MB |
| 1000 GB (1TB) | 8000 | ~128MB |
| 10000 GB (10TB) | 50000（最大） | ~200MB |

### 预构建资产

本仓库包含适用于 **Linux x86_64** 架构的预构建资产，位于 `assets/` 目录：

- `assets/spark-sql-perf-assembly-0.5.1.jar` - 包含所有依赖的 Fat JAR
- `assets/tpcds-kit-1.0.0.tar.gz` - Linux x86_64 原生 dsdgen 二进制文件

这些资产在数据生成期间会上传到您的 Dataproc 集群。`dsdgen` 二进制文件是**原生可执行文件**，必须与您的集群架构匹配。

> **架构要求：** 预构建的 `dsdgen` 二进制文件是为 Linux x86_64 编译的，这是 GCP Dataproc 集群的默认架构。如果您的作业集群使用不同的架构（例如基于 ARM 的实例），您必须自行构建资产以匹配集群的 CPU 架构。

### 构建资产（可选）

如果预构建资产与您的集群架构不匹配，或您想使用更新版本，请自行构建：

```bash
# 前置条件：git, sbt, make, gcc, Java 11
# 在与目标 Dataproc 集群架构匹配的 Linux 系统上运行
make build-assets
```

`make build-assets` 目标会：
1. 克隆（或更新）[spark-sql-perf](https://github.com/databricks/spark-sql-perf) 仓库
2. 使用 SBT 构建 assembly JAR
3. 克隆（或更新）[tpcds-kit](https://github.com/databricks/tpcds-kit) 仓库
4. 使用 GCC 编译原生 `dsdgen` 二进制文件
5. 将所有内容打包到 `assets/` 目录

构建产物创建在 `tmp/`（git 忽略），最终资产放置在 `assets/`。

### 资产构建故障排除

**spark-sql-perf JAR 构建失败（Ivy/Maven 错误）**

如果看到类似 `origin location must be absolute` 或其他依赖解析失败的错误，请清除缓存：

```bash
# 清除 Ivy、SBT 和 Maven 缓存
rm -rf ~/.ivy2/cache ~/.sbt/boot ~/.sbt/1.0/staging ~/.m2/repository

# 清除 spark-sql-perf 构建产物
rm -rf tmp/spark-sql-perf/target tmp/spark-sql-perf/project/target

# 重新运行构建
make build-assets
```

**tpcds-kit 构建失败（GCC 错误）**

构建脚本已包含 GCC 14+ 严格模式的解决方案。如果仍然遇到问题：

```bash
# 确保安装了构建工具
sudo apt-get install gcc make

# 对于持续性问题，尝试使用较旧的 GCC 版本
sudo apt-get install gcc-11
export CC=gcc-11
make build-assets
```

**构建成功但只创建了一个资产**

即使一个组件失败，脚本也会继续构建另一个。检查构建摘要输出中的具体错误信息，并应用上述相应的修复方法。

## 项目结构

```
dataproc-tpcds/
├── conf.yaml                 # 统一配置文件
├── main.py                   # 入口脚本（编排所有模块）
├── requirements.txt          # Python 依赖
├── Makefile                  # 所有 make 目标
├── lib/
│   ├── cluster_manager.py    # Dataproc 集群创建/删除
│   ├── data_generator.py     # TPC-DS 数据生成（使用 spark-sql-perf）
│   ├── query_runner.py       # Spark SQL 作业提交
│   └── bq_reporter.py        # 指标收集和 BigQuery 上报
├── scripts/
│   └── build_assets.sh       # 一次性资产构建脚本
├── assets/                   # 预构建数据生成资产
│   ├── spark-sql-perf-assembly-*.jar  # spark-sql-perf fat JAR
│   └── tpcds-kit-*.tar.gz    # 原生 dsdgen 二进制文件
├── sql/                      # TPC-DS 标准查询（q1.sql - q99.sql）
├── jar/                      # 预编译 JAR（可选）
└── tests/                    # 单元测试和集成测试
```

## 配置参考

### GCP 配置

| 参数 | 描述 | 必需 |
|------|------|------|
| `project_id` | 您的 GCP Project ID | 是 |
| `region` | Dataproc 集群的 GCP 区域 | 是 |
| `zone` | 计算资源的 GCP 可用区 | 否 |
| `service_account_key_path` | 服务账号 JSON 密钥路径 | 否 |
| `staging_bucket` | 用于脚本和数据的 GCS 存储桶 | 是 |

### Dataproc 配置

| 参数 | 描述 | 默认值 |
|------|------|--------|
| `cluster_name` | Dataproc 集群名称 | 必需 |
| `image_version` | Dataproc 镜像版本（决定 Spark 版本） | `2.3-debian12` |
| `master_machine_type` | Master 节点机器类型 | `n2-standard-4` |
| `worker_machine_type` | Worker 节点机器类型 | `n2-standard-8` |
| `num_workers` | Worker 节点数量 | 4 |
| `enable_component_gateway` | 启用 Web UI 访问 | `true` |
| `spark_properties` | Spark 配置属性 | 见 conf.yaml |

### 基准测试配置

| 参数 | 描述 | 默认值 |
|------|------|--------|
| `scale_factor` | TPC-DS 规模（GB，1000 = 1TB） | 1000 |
| `data_format` | 数据格式（parquet/orc） | `parquet` |
| `format_compression` | 压缩编解码器 | `snappy` |
| `data_path` | TPC-DS 数据的 GCS 路径 | 必需 |
| `skip_data_gen` | 如果数据存在则跳过数据生成 | `false` |
| `queries_to_run` | "all" 或列表如 [1, 2, 3] | `all` |
| `iterations` | 每个查询的迭代次数 | 1 |

### BigQuery 配置

| 参数 | 描述 | 默认值 |
|------|------|--------|
| `enable` | 启用 BigQuery 上报 | `true` |
| `dataset` | BigQuery 数据集名称 | `tpcds_metrics` |
| `table` | BigQuery 表名称 | `benchmark_history` |

## 命令行选项

```bash
python main.py [OPTIONS]

选项：
  --config, -c PATH      配置文件路径（默认：conf.yaml）
  --skip-cluster-delete  基准测试后不提示删除集群
  --auto-delete          基准测试后自动删除集群
  --dry-run              验证配置并显示计划但不执行
  --verbose, -v          启用详细日志
```

## 工作原理

### 阶段 1：集群创建
使用您指定的配置创建 Dataproc 集群。如果集群已存在，将复用它。

### 阶段 2：数据生成
如果 `skip_data_gen` 为 false，使用 Spark 按指定的 scale factor 生成 TPC-DS 数据。数据以 Parquet/ORC 格式存储在 GCS 上。

### 阶段 3：查询执行
对于每个 TPC-DS 查询：
1. 扫描 GCS 目录发现可用的表目录
2. 将每个表注册为 Spark Temporary View
3. 执行 SQL 查询
4. 收集执行指标

### 阶段 4：上报
将详细指标写入 BigQuery 进行分析：
- 查询执行时间
- 输入/shuffle 字节数
- 状态（成功/失败）
- 集群配置详情

## BigQuery Schema

`benchmark_history` 表包含：

| 字段 | 类型 | 描述 |
|------|------|------|
| `job_uuid` | STRING | 唯一测试 ID |
| `batch_id` | STRING | 用于分组查询的批次 ID |
| `run_timestamp` | TIMESTAMP | 执行时间戳（已分区） |
| `project_id` | STRING | GCP Project ID |
| `cluster_name` | STRING | Dataproc 集群名称 |
| `scale_factor` | INTEGER | TPC-DS 规模因子（GB） |
| `spark_version` | STRING | Spark 版本 |
| `image_version` | STRING | Dataproc 镜像版本 |
| `worker_count` | INTEGER | Worker 节点数量 |
| `worker_machine_type` | STRING | Worker 机器类型 |
| `query_name` | STRING | 查询名称（如 "q1"） |
| `iteration` | INTEGER | 迭代次数 |
| `status` | STRING | DONE、FAILED 或 SKIPPED |
| `duration_sec` | FLOAT | 总执行时间（秒） |
| `input_bytes` | INT64 | 扫描的总数据量（如可用） |
| `shuffle_read_bytes` | INT64 | Shuffle 读取字节数（如可用） |
| `shuffle_write_bytes` | INT64 | Shuffle 写入字节数（如可用） |
| `records_read` | INT64 | 处理的总记录数（如可用） |
| `executor_cores` | INTEGER | Executor 核心数（来自配置） |
| `executor_memory` | STRING | Executor 内存（来自配置） |
| `data_format` | STRING | 数据格式（parquet/orc） |
| `job_id` | STRING | Dataproc 作业 ID |
| `error_message` | STRING | 失败时的错误消息 |

> **注意：** `input_bytes`、`shuffle_*_bytes` 和 `records_read` 等指标从 Spark 内部指标中收集（如可用）。根据 Spark 版本和查询执行情况，这些字段可能不会对所有查询都填充。

## 分析查询

在 BigQuery 中查询基准测试结果：

```sql
-- 比较不同 worker 数量的性能
SELECT
  worker_count,
  worker_machine_type,
  AVG(duration_sec) as avg_duration,
  COUNT(*) as query_count
FROM `project.tpcds_metrics.benchmark_history`
WHERE status = 'SUCCESS'
GROUP BY worker_count, worker_machine_type
ORDER BY avg_duration;

-- 查找最慢的查询
SELECT
  query_name,
  AVG(duration_sec) as avg_duration,
  MAX(duration_sec) as max_duration
FROM `project.tpcds_metrics.benchmark_history`
WHERE status = 'SUCCESS'
GROUP BY query_name
ORDER BY avg_duration DESC
LIMIT 10;
```

## 测试

运行测试套件：

```bash
# 安装开发依赖（包含 pytest）
make install-dev

# 运行所有测试
make test

# 运行并生成覆盖率报告
make test-cov
```

## 架构说明

### GCS 存储桶结构

本工具使用**单个 GCS 存储桶**，通过子文件夹组织所有数据和资源：

```
gs://{staging_bucket}/
├── lib/                              # 数据生成资源（自动上传）
│   ├── spark-sql-perf-assembly-*.jar   # spark-sql-perf fat JAR
│   └── tpcds-kit-*.tar.gz              # 原生 dsdgen 二进制文件（分发到各 worker）
├── spark-events/                     # Spark 事件日志（供 History Server 使用）
├── scripts/                          # 查询执行脚本（自动生成）
└── tpcds-data/{scale}/              # 生成的 TPC-DS 数据
    ├── _SUCCESS                       # 数据生成完成标记
    ├── store_sales/                   # 24 个 TPC-DS 表（Parquet/ORC 格式）
    ├── catalog_sales/
    ├── web_sales/
    ├── date_dim/
    └── ...
```

**资源分发到 Worker 节点:** 原生 `dsdgen` 二进制文件被打包成 tarball，通过 Spark 的 `archive_uris` 功能分发到所有 Spark executor。这确保每个 worker 都能访问该二进制文件进行并行数据生成。

**配置说明:** 所有路径都基于 conf.yaml 中的 `staging_bucket` 设置。`data_path` 设置允许您自定义 TPC-DS 数据的存储位置（例如，使用基于 scale factor 的子目录，如 `tpcds-data/1T` 或 `tpcds-data/10T`）。

### 无状态设计（无 Metastore）

本工具故意避免 Hive Metastore 依赖：

1. **数据发现**: 扫描 GCS 目录查找表数据
2. **视图注册**: 为每个表创建 Spark Temporary View
3. **查询执行**: 对临时视图运行标准 TPC-DS SQL

这确保基准测试衡量的是纯 Spark SQL 性能，不包含 metastore 开销。

### 错误处理

- 集群创建失败是致命错误（退出码 1）
- 数据生成失败是致命错误
- 单个查询失败会记录日志但不会停止基准测试
- 即使失败也会提示用户清理集群

## 最佳实践

### 集群和 Executor 配置

所有集群和 executor 设置都在 `conf.yaml` 中**显式配置**。不进行自动计算 - 您需要确保资源配置在集群容量范围内。

**集群级别设置：**
```yaml
dataproc:
  num_masters: 1          # 1 为标准模式，3 为高可用 (HA)
  num_workers: 4          # 4 个 worker × 2 个 executor = 8 个 executor
  # Master 和 worker 使用相同机器类型（n2-standard-8）并配备本地 SSD
  master_machine_type: "n2-standard-8"
  worker_machine_type: "n2-standard-8"
```

**Executor 和 Driver 设置：**
```yaml
spark_properties:
  # Executors（共 8 个，每个 worker 2 个）
  "spark.executor.instances": "8"
  "spark.executor.cores": "4"
  "spark.executor.memory": "14g"
  "spark.executor.memoryOverhead": "1g"
  # Driver（在 master 节点上以 client 模式运行）
  "spark.driver.cores": "6"
  "spark.driver.memory": "24g"
  "spark.driver.memoryOverhead": "4g"
```

**默认资源分配（1 个 master + 4 个 worker，全部为 n2-standard-8）：**

| 组件 | 位置 | 核心数 | 内存 |
|------|------|--------|------|
| 1 个 Driver | Master 节点 | 6 | 28GB |
| 8 个 Executor | Workers 1-4（每个 2 个） | 32 | 120GB |

**关键原则：**
- **Driver**：运行在 master 节点（通过 Python API 以 client 模式运行）
- **每 executor 4-5 个核心**：HDFS/GCS 并行 I/O 吞吐量最优
- **内存低于 32GB**：启用 JVM 压缩 OOPs（节省 5-15% 内存）
- **资源不足**：作业提交将失败并显示 YARN 分配错误

### 1TB 基准测试
- 使用至少 4-8 个 worker 节点
- 推荐：带本地 SSD 的 `n2-standard-8` workers
- 启用 Spark adaptive query execution（默认已启用）

### 可重复结果
- 运行多次迭代（`iterations: 3`）
- 使用相同的集群配置进行比较
- 使用 BigQuery 进行趋势分析

### 成本优化
- 使用 `--auto-delete` 自动清理集群
- 考虑使用抢占式 VM 进行大规模测试
- 初次数据生成后使用 `skip_data_gen: true`

## 故障排除

### 常见问题

**集群创建失败**
- 检查 Compute Engine 项目配额
- 验证服务账号权限

**数据生成超时**
- 增加 worker 数量以处理更大的 scale factor
- 检查 GCS 存储桶权限

**查询失败：找不到表**
- 验证 `data_path` 包含表目录
- 检查数据生成是否成功完成

### 日志

- Driver 日志：在 GCS staging bucket 中可用
- 集群日志：在 Cloud Console 中查看或通过 `gcloud dataproc jobs describe` 查看

## 清理指南

正确清理对于避免持续产生 GCP 费用至关重要。完成基准测试后请遵循此清单。

### 快速清理（单条命令）

```bash
# 删除基准测试集群并清理本地文件
make clean-all
```

### 详细清理步骤

#### 1. 删除 Dataproc 集群

```bash
# 检查运行中的集群
gcloud dataproc clusters list --region=us-central1

# 删除基准测试集群
make cluster-delete
# 或手动删除：
gcloud dataproc clusters delete tpcds-bench --region=us-central1 --quiet

# 删除 history server（如果启用）
gcloud dataproc clusters delete tpcds-history-server --region=us-central1 --quiet
```

#### 2. 删除 GCS 数据（可选）

仅在不需要生成的数据用于将来基准测试时删除：

```bash
# 列出数据大小
gsutil du -s gs://your-bucket/tpcds-data/

# 删除所有 TPC-DS 数据
gsutil -m rm -r gs://your-bucket/tpcds-data/
```

#### 3. 删除 BigQuery 数据（可选）

仅在不需要历史基准测试结果时删除：

```bash
# 查看数据集信息
bq show your-project:tpcds_metrics

# 删除整个数据集（包括所有表）
bq rm -r -f your-project:tpcds_metrics

# 或仅删除基准测试表
bq rm -f your-project:tpcds_metrics.benchmark_history
```

#### 4. 清理本地构建产物

```bash
# 清理 Python 缓存
make clean

# 清理所有本地文件
make clean-local

# 或手动清理
rm -rf __pycache__ .pytest_cache tmp/
```

### 资源成本摘要

| 资源 | 成本驱动因素 | 清理优先级 |
|------|-------------|-----------|
| **Dataproc Cluster** | 按分钟计算费用 | **高** - 使用后立即删除 |
| **History Server** | 低成本（单个小型 VM） | 中等 - 如果经常运行基准测试可以保留 |
| **GCS Data** | 存储成本（约 $0.02/GB/月） | 低 - 保留以重新运行基准测试 |
| **BigQuery** | 存储 + 查询成本 | 低 - 指标数据成本很小 |

### 验证清理

```bash
# 检查没有 Dataproc 集群在运行
gcloud dataproc clusters list --region=us-central1

# 检查 GCS 存储桶内容
gsutil ls gs://your-bucket/

# 检查 BigQuery 数据集
bq ls your-project:
```

### 自动清理

完全自动化的基准测试和清理：

```bash
# 运行基准测试并在完成后自动删除集群
make run-auto-delete

# 或直接使用 Python
python main.py --auto-delete
```

## 许可证

Apache 2.0

## 贡献

1. Fork 仓库
2. 创建功能分支
3. 运行测试：`pytest tests/ -v`
4. 提交 Pull Request
