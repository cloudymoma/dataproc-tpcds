[![Rust Build](https://github.com/cloudymoma/dataproc-tpcds/actions/workflows/rust.yml/badge.svg)](https://github.com/cloudymoma/dataproc-tpcds/actions/workflows/rust.yml)

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
3. **Rust 1.70+**（用于高性能数据生成器）- 通过 [rustup](https://rustup.rs/) 安装
4. **GCP 项目** 需启用以下 API：
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

### 步骤 1：安装

```bash
# 克隆仓库
git clone <repository-url>
cd dataproc-tpcds

# 安装 Python 依赖
pip install -r requirements.txt

# 构建 Rust 数据生成器（可选但推荐，以获得更好的性能）
make datagen-build

# 使用 Google Cloud 进行身份验证
gcloud auth application-default login
```

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

# 可选：调整 Rust 数据生成器
datagen:
  generator_threads: 8
  uploader_threads: 4
  file_size_mb: 128
```

### 步骤 3：验证配置

```bash
# 验证配置但不执行任何操作
make dry-run
```

### 步骤 4：生成 TPC-DS 数据

选择一个选项：

```bash
# 选项 A：使用高性能 Rust 生成器（推荐）
make datagen-run

# 选项 B：使用基于 Spark 的生成器（较慢但无需 Rust）
make data-gen

# 验证数据是否已生成
make data-check
```

### 步骤 5：运行基准测试

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
pip install -r requirements.txt
gcloud auth application-default login

# 2. 编辑 conf.yaml 填入您的项目/存储桶
vim conf.yaml

# 3. 运行基准测试并自动清理
make run-auto-delete
```

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
make test              # 运行所有测试（Python）
make test-cov          # 运行测试并生成覆盖率报告
make check-syntax      # 验证 Python 语法
make full-test         # 运行所有测试（Python + Rust）

# 集群操作
make cluster-create    # 仅创建 Dataproc 集群
make cluster-delete    # 删除 Dataproc 集群
make cluster-status    # 检查集群状态
make cluster-info      # 显示集群配置

# 数据操作
make data-gen          # 仅生成 TPC-DS 数据
make data-check        # 检查数据是否存在
make data-tables       # 列出可用表

# Rust 数据生成器（高性能）
make datagen-build     # 构建 Rust 数据生成器（release）
make datagen-test      # 运行 Rust datagen 测试
make datagen-run       # 生成数据并上传到 GCS
make datagen-dry-run   # 试运行（仅本地，不上传）
make datagen-verbose   # 详细日志生成
make datagen-clean     # 清理构建产物

# BigQuery
make bq-setup          # 创建 BQ 数据集/表
make bq-schema         # 显示表 schema

# 工具
make validate          # 运行所有验证检查
make clean             # 删除缓存文件
make list-queries      # 列出可用 SQL 查询
make show-query QUERY=q1  # 显示指定查询
```

## 高性能 Rust 数据生成器

对于极致性能的数据生成，请使用基于 Rust 的生成器。它使用双线程池架构：

1. **Generator threads**: 并行生成所有 24 个 TPC-DS 表的 Parquet 文件
2. **Uploader threads**: 并发上传完成的文件到 GCS

### 性能优化

Rust 生成器针对最大吞吐量进行了高度优化：

| 优化项 | 描述 |
|--------|------|
| **Static lookup tables** | 20+ 预计算数组，实现零分配字符串生成 |
| **itoa fast formatting** | 比 format!() 快约 3 倍的整数转字符串 |
| **Cached distributions** | 预计算的随机分布，跨批次复用 |
| **Zero-copy appends** | 直接 StringBuilder append，无中间分配 |
| **Capacity pre-allocation** | 预估字符串长度以最小化重新分配 |
| **Optimized compression** | ZSTD level 3、GZIP level 6，针对速度调优 |
| **Batch processing** | 大批量上传（threads × 8）提高吞吐量 |
| **LTO release build** | Link-time optimization，单 codegen unit |

### 构建 Rust 生成器

前置条件：Rust 1.70+（通过 [rustup](https://rustup.rs/) 安装）

```bash
# 构建 release 二进制文件（带 LTO 优化）
make datagen-build

# 运行测试
make datagen-test
```

### 运行 Rust 生成器

```bash
# 完整运行（生成并上传到 GCS）
make datagen-run

# 试运行（本地生成，不上传）
make datagen-dry-run

# 自定义选项
make datagen-custom SF=100 GEN_THREADS=16 UP_THREADS=8 FILE_MB=256

# CLI 帮助
./datagen/target/release/tpcds-datagen --help
```

### Rust 生成器配置

在 `conf.yaml` 的 `datagen:` 部分配置：

```yaml
datagen:
  generator_threads: 8      # 数据生成线程数（默认：CPU 核心数）
  uploader_threads: 4       # GCS 上传线程数
  file_size_mb: 128         # 目标 Parquet 文件大小
  temp_dir: "/tmp/tpcds-datagen"  # 本地临时目录
  batch_size: 50000         # 每批行数（针对 Parquet row groups 优化）
  cleanup_after_upload: true  # 上传后删除本地文件
```

### 性能建议

- **Scale factor 1-10**: 单机，4-8 generator threads
- **Scale factor 100-1000**: 使用 16+ generator threads，8+ uploader threads
- **网络瓶颈**: 如果上传是瓶颈，增加 `uploader_threads`
- **CPU 瓶颈**: `generator_threads` 最多设为 CPU 核心数的 2 倍
- **磁盘空间**: 确保 `temp_dir` 有足够空间存放约 10% 的总数据量

## 项目结构

```
dataproc-tpcds/
├── conf.yaml                 # 统一配置文件
├── main.py                   # 入口脚本（编排所有模块）
├── requirements.txt          # Python 依赖
├── Makefile                  # 所有 make 目标
├── lib/
│   ├── cluster_manager.py    # Dataproc 集群创建/删除
│   ├── data_generator.py     # TPC-DS 数据生成逻辑
│   ├── query_runner.py       # Spark SQL 作业提交
│   └── bq_reporter.py        # 指标收集和 BigQuery 上报
├── datagen/                  # 高性能 Rust 数据生成器
│   ├── Cargo.toml            # Rust 依赖
│   └── src/
│       ├── main.rs           # CLI 入口点
│       ├── config.rs         # 配置解析
│       ├── generator/        # 多线程数据生成
│       ├── schema/           # TPC-DS 表 schema（24 个表）
│       └── uploader.rs       # 并行 GCS 上传
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
| `run_timestamp` | TIMESTAMP | 执行时间 |
| `project_id` | STRING | GCP Project ID |
| `cluster_name` | STRING | 集群名称 |
| `scale_factor` | INTEGER | 数据大小（GB） |
| `query_name` | STRING | 查询名称（如 "q1"） |
| `status` | STRING | SUCCESS/FAILED |
| `duration_sec` | FLOAT | 执行时间（秒） |
| `input_bytes` | INT64 | 扫描的数据量 |
| `shuffle_read_bytes` | INT64 | Shuffle 读取量 |
| `shuffle_write_bytes` | INT64 | Shuffle 写入量 |
| `error_message` | STRING | 失败时的错误详情 |

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
# 安装测试依赖
pip install pytest pytest-cov pytest-mock

# 运行所有测试
pytest tests/ -v

# 运行并生成覆盖率报告
pytest tests/ --cov=lib --cov-report=html
```

## 架构说明

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

### 1TB 基准测试
- 使用至少 4-8 个 worker 节点
- 推荐：`n2-standard-8` workers
- 启用 Spark adaptive query execution

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

# 删除 Rust 生成器的临时文件
rm -rf /tmp/tpcds-datagen
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

# 清理 Rust 构建产物
make datagen-clean

# 清理所有（Python + Rust）
rm -rf __pycache__ .pytest_cache datagen/target
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

# 检查本地磁盘使用
du -sh /tmp/tpcds-datagen 2>/dev/null || echo "临时目录已清理"
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
