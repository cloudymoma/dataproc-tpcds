# GCP Dataproc TPC-DS Auto-Benchmark Tool Makefile
# Usage: make [target]

# Configuration
PYTHON := python3
PIP := pip3
CONFIG := conf.yaml
VENV := .venv

# Colors for terminal output
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

.PHONY: help install install-dev venv clean clean-all clean-local \
        run dry-run run-auto-delete run-verbose \
        test test-cov test-verbose lint format \
        cluster-create cluster-delete cluster-status cluster-info \
        history-create history-delete history-status history-server-delete \
        data-gen data-check data-tables \
        bq-setup bq-query bq-schema \
        check-config check-auth validate \
        datagen-build datagen-build-debug datagen-test datagen-test-release \
        datagen-run datagen-dry-run datagen-verbose datagen-custom \
        datagen-clean datagen-help \
        quick-start full-test status

# Default target
.DEFAULT_GOAL := help

##@ General

help: ## Display this help message
	@awk 'BEGIN {FS = ":.*##"; printf "\n${GREEN}GCP Dataproc TPC-DS Auto-Benchmark Tool${NC}\n\nUsage:\n  make ${YELLOW}<target>${NC}\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  ${YELLOW}%-20s${NC} %s\n", $$1, $$2 } /^##@/ { printf "\n${GREEN}%s${NC}\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Setup & Installation

venv: ## Create Python virtual environment
	@echo "Creating virtual environment..."
	@$(PYTHON) -m venv $(VENV)
	@echo "Virtual environment created. Activate with: source $(VENV)/bin/activate"

install: ## Install production dependencies
	@echo "Installing dependencies..."
	@$(PIP) install -r requirements.txt
	@echo "Dependencies installed successfully."

install-dev: ## Install development dependencies (includes test tools)
	@echo "Installing development dependencies..."
	@$(PIP) install -r requirements.txt
	@$(PIP) install pytest pytest-cov pytest-mock
	@echo "Development dependencies installed successfully."

install-venv: venv ## Install dependencies in virtual environment
	@echo "Installing dependencies in virtual environment..."
	@$(VENV)/bin/pip install -r requirements.txt
	@echo "Dependencies installed in virtual environment."

##@ Running Benchmarks

run: check-config ## Run the full benchmark (interactive mode)
	@echo "Starting TPC-DS benchmark..."
	@$(PYTHON) main.py --config $(CONFIG)

dry-run: check-config ## Validate configuration without executing
	@echo "Running in dry-run mode..."
	@$(PYTHON) main.py --config $(CONFIG) --dry-run

run-auto-delete: check-config ## Run benchmark and auto-delete cluster when done
	@echo "Starting benchmark with auto-delete..."
	@$(PYTHON) main.py --config $(CONFIG) --auto-delete

run-skip-delete: check-config ## Run benchmark without cluster deletion prompt
	@echo "Starting benchmark (skip delete prompt)..."
	@$(PYTHON) main.py --config $(CONFIG) --skip-cluster-delete

run-verbose: check-config ## Run benchmark with verbose logging
	@echo "Starting benchmark with verbose logging..."
	@$(PYTHON) main.py --config $(CONFIG) --verbose

run-custom: check-config ## Run with custom config (usage: make run-custom CONFIG=myconfig.yaml)
	@echo "Starting benchmark with config: $(CONFIG)..."
	@$(PYTHON) main.py --config $(CONFIG)

##@ Testing

test: ## Run all tests
	@echo "Running tests..."
	@$(PYTHON) -m pytest tests/ -v

test-cov: ## Run tests with coverage report
	@echo "Running tests with coverage..."
	@$(PYTHON) -m pytest tests/ -v --cov=lib --cov-report=html --cov-report=term
	@echo "HTML coverage report: htmlcov/index.html"

test-verbose: ## Run tests with extra verbose output
	@echo "Running tests with verbose output..."
	@$(PYTHON) -m pytest tests/ -vv --tb=long

test-unit: ## Run only unit tests (fast)
	@echo "Running unit tests..."
	@$(PYTHON) -m pytest tests/ -v -m "not integration"

test-file: ## Run tests for a specific file (usage: make test-file FILE=test_cluster_manager.py)
	@echo "Running tests for $(FILE)..."
	@$(PYTHON) -m pytest tests/$(FILE) -v

##@ Code Quality

lint: ## Check code style with flake8
	@echo "Checking code style..."
	@$(PYTHON) -m flake8 lib/ main.py --max-line-length=100 --ignore=E501,W503 || true

format: ## Format code with black
	@echo "Formatting code..."
	@$(PYTHON) -m black lib/ main.py tests/ --line-length=100 || echo "Install black: pip install black"

check-syntax: ## Verify Python syntax is correct
	@echo "Checking Python syntax..."
	@$(PYTHON) -m py_compile main.py
	@$(PYTHON) -m py_compile lib/cluster_manager.py
	@$(PYTHON) -m py_compile lib/data_generator.py
	@$(PYTHON) -m py_compile lib/query_runner.py
	@$(PYTHON) -m py_compile lib/bq_reporter.py
	@echo "All Python files have valid syntax."

##@ Cluster Operations

cluster-create: check-config check-auth ## Create Dataproc cluster only
	@echo "Creating Dataproc cluster..."
	@$(PYTHON) -c "import yaml; from lib.cluster_manager import ClusterManager; \
		config = yaml.safe_load(open('$(CONFIG)')); \
		cm = ClusterManager(config); \
		cm.create_cluster(wait=True); \
		print('Cluster created successfully')"

cluster-delete: check-config check-auth ## Delete Dataproc cluster
	@echo "Deleting Dataproc cluster..."
	@$(PYTHON) -c "import yaml; from lib.cluster_manager import ClusterManager; \
		config = yaml.safe_load(open('$(CONFIG)')); \
		cm = ClusterManager(config); \
		cm.delete_cluster(wait=True); \
		print('Cluster deleted successfully')"

cluster-status: check-config check-auth ## Check cluster status
	@echo "Checking cluster status..."
	@$(PYTHON) -c "import yaml; from lib.cluster_manager import ClusterManager; \
		config = yaml.safe_load(open('$(CONFIG)')); \
		cm = ClusterManager(config); \
		info = cm.get_cluster_info(); \
		print('Cluster exists:', info is not None); \
		print('Info:', info) if info else None"

cluster-info: check-config ## Show cluster configuration from config file
	@echo "Cluster configuration:"
	@$(PYTHON) -c "import yaml; \
		config = yaml.safe_load(open('$(CONFIG)')); \
		dp = config['dataproc']; \
		hs = config.get('history_server', {}); \
		print('Job Cluster:'); \
		print('  Name:', dp['cluster_name']); \
		print('  Image:', dp.get('image_version', 'default')); \
		print('  Workers:', dp.get('num_workers', 'default')); \
		print('  Worker Type:', dp.get('worker_machine_type', 'default')); \
		print('  Max Idle:', dp.get('max_idle', '1h')); \
		print('  Tier:', dp.get('tier', 'standard')); \
		print('History Server:'); \
		print('  Enabled:', hs.get('enable', False)); \
		print('  Name:', hs.get('cluster_name', 'N/A')); \
		print('  Log Dir:', hs.get('log_dir', 'N/A'))"

##@ History Server Operations

history-create: check-config check-auth ## Create Spark History Server cluster
	@echo "Creating Spark History Server..."
	@$(PYTHON) -c "import yaml; from lib.cluster_manager import ClusterManager; \
		config = yaml.safe_load(open('$(CONFIG)')); \
		cm = ClusterManager(config); \
		cm.create_history_server(wait=True); \
		print('History server created successfully')"

history-delete: check-config check-auth ## Delete Spark History Server cluster
	@echo "Deleting Spark History Server..."
	@$(PYTHON) -c "import yaml; from lib.cluster_manager import ClusterManager; \
		config = yaml.safe_load(open('$(CONFIG)')); \
		cm = ClusterManager(config); \
		cm.delete_history_server(wait=True); \
		print('History server deleted successfully')"

history-status: check-config check-auth ## Check History Server status
	@echo "Checking history server status..."
	@$(PYTHON) -c "import yaml; from lib.cluster_manager import ClusterManager; \
		config = yaml.safe_load(open('$(CONFIG)')); \
		cm = ClusterManager(config); \
		exists = cm.history_server_exists(); \
		print('History server exists:', exists); \
		if exists: \
			url = cm.get_history_server_url(); \
			print('History UI URL:', url or 'N/A')"

history-server-delete: history-delete ## Alias for history-delete

##@ Data Operations

data-gen: check-config check-auth ## Generate TPC-DS data only
	@echo "Generating TPC-DS data..."
	@$(PYTHON) -c "import yaml; from lib.data_generator import DataGenerator; \
		config = yaml.safe_load(open('$(CONFIG)')); \
		dg = DataGenerator(config); \
		result = dg.generate_data(); \
		print('Result:', result)"

data-check: check-config check-auth ## Check if TPC-DS data exists
	@echo "Checking TPC-DS data..."
	@$(PYTHON) -c "import yaml; from lib.data_generator import DataGenerator; \
		config = yaml.safe_load(open('$(CONFIG)')); \
		dg = DataGenerator(config); \
		exists = dg.data_exists(); \
		print('Data exists:', exists)"

data-tables: check-config check-auth ## List available TPC-DS tables
	@echo "Listing TPC-DS tables..."
	@$(PYTHON) -c "import yaml; from lib.data_generator import DataGenerator; \
		config = yaml.safe_load(open('$(CONFIG)')); \
		dg = DataGenerator(config); \
		tables = dg.list_tables(); \
		print('Tables:', tables)"

##@ BigQuery Operations

bq-setup: check-config check-auth ## Create BigQuery dataset and table (if not exist)
	@echo "Setting up BigQuery dataset and table..."
	@$(PYTHON) -c "import yaml; from lib.bq_reporter import BQReporter; \
		config = yaml.safe_load(open('$(CONFIG)')); \
		reporter = BQReporter(config); \
		success = reporter.setup(); \
		exit(0 if success else 1)"

bq-query: check-config check-auth ## Show BigQuery summary query
	@echo "BigQuery summary query:"
	@$(PYTHON) -c "import yaml; from lib.bq_reporter import BQReporter; \
		config = yaml.safe_load(open('$(CONFIG)')); \
		reporter = BQReporter(config); \
		print(reporter.get_summary_query())"

bq-schema: ## Show BigQuery table schema
	@echo "BigQuery schema:"
	@$(PYTHON) -c "from lib.bq_reporter import BENCHMARK_SCHEMA; \
		for field in BENCHMARK_SCHEMA: print(f'  {field.name}: {field.field_type} ({field.mode})')"

##@ Validation

check-config: ## Verify configuration file exists and is valid
	@if [ ! -f "$(CONFIG)" ]; then \
		echo "$(RED)Error: Configuration file '$(CONFIG)' not found$(NC)"; \
		exit 1; \
	fi
	@$(PYTHON) -c "import yaml; \
		config = yaml.safe_load(open('$(CONFIG)')); \
		required = ['gcp', 'dataproc', 'benchmark']; \
		missing = [r for r in required if r not in config]; \
		exit(1) if missing else print('Configuration file is valid')" || \
		(echo "$(RED)Error: Invalid configuration$(NC)" && exit 1)

check-auth: ## Check Google Cloud authentication
	@echo "Checking Google Cloud authentication..."
	@$(PYTHON) -c "from google.auth import default; \
		credentials, project = default(); \
		print('Authenticated. Project:', project)" 2>/dev/null || \
		echo "$(YELLOW)Warning: Could not verify GCP authentication. Run 'gcloud auth application-default login'$(NC)"

validate: check-config check-syntax ## Run all validation checks
	@echo "$(GREEN)All validations passed$(NC)"

##@ SQL Queries

list-queries: ## List available TPC-DS SQL queries
	@echo "Available TPC-DS queries:"
	@ls -1 sql/*.sql 2>/dev/null | sed 's/sql\//  /' | sed 's/.sql//' || echo "  No queries found"

show-query: ## Show a specific query (usage: make show-query QUERY=q1)
	@if [ -f "sql/$(QUERY).sql" ]; then \
		echo "=== $(QUERY).sql ==="; \
		cat sql/$(QUERY).sql; \
	else \
		echo "Query not found: $(QUERY)"; \
		echo "Available queries:"; \
		ls -1 sql/*.sql 2>/dev/null | sed 's/sql\//  /' | sed 's/.sql//'; \
	fi

##@ Cleanup

clean: ## Remove Python cache files
	@echo "Cleaning Python cache..."
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@find . -type f -name "*.pyo" -delete 2>/dev/null || true
	@find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	@rm -rf htmlcov/ .coverage 2>/dev/null || true
	@echo "Cache cleaned."

clean-venv: ## Remove virtual environment
	@echo "Removing virtual environment..."
	@rm -rf $(VENV)
	@echo "Virtual environment removed."

clean-all: clean datagen-clean cluster-delete ## Clean local files and delete Dataproc cluster
	@rm -rf $(VENV) 2>/dev/null || true
	@echo "$(GREEN)All local files cleaned and cluster deleted.$(NC)"

clean-local: clean clean-venv datagen-clean ## Remove all local generated files (no cluster deletion)
	@echo "All local generated files removed."

##@ Documentation

docs: ## Show project documentation
	@cat README.md | head -100

config-example: ## Show example configuration
	@echo "Example configuration (conf.yaml):"
	@cat conf.yaml

##@ Rust Data Generator

datagen-build: ## Build Rust data generator (release mode)
	@echo "Building Rust data generator..."
	@cd datagen && cargo build --release
	@echo "Build complete. Binary: datagen/target/release/tpcds-datagen"

datagen-build-debug: ## Build Rust data generator (debug mode)
	@echo "Building Rust data generator (debug)..."
	@cd datagen && cargo build
	@echo "Debug build complete."

datagen-test: ## Run Rust data generator tests
	@echo "Running Rust datagen tests..."
	@cd datagen && cargo test

datagen-test-release: ## Run Rust data generator tests (release mode)
	@echo "Running Rust datagen tests (release)..."
	@cd datagen && cargo test --release

datagen-run: datagen-build check-config ## Generate TPC-DS data with Rust generator
	@echo "Generating TPC-DS data with Rust generator..."
	@./datagen/target/release/tpcds-datagen --config $(CONFIG)

datagen-dry-run: datagen-build check-config ## Dry run (generate locally, no upload)
	@echo "Running Rust datagen in dry-run mode..."
	@./datagen/target/release/tpcds-datagen --config $(CONFIG) --dry-run

datagen-verbose: datagen-build check-config ## Generate with verbose logging
	@echo "Generating TPC-DS data with verbose logging..."
	@./datagen/target/release/tpcds-datagen --config $(CONFIG) --verbose

datagen-custom: datagen-build check-config ## Run with custom options (SF, threads, etc.)
	@echo "Running Rust datagen with custom options..."
	@echo "Available options:"
	@echo "  SF=N          Scale factor (overrides config)"
	@echo "  GEN_THREADS=N Number of generator threads"
	@echo "  UP_THREADS=N  Number of uploader threads"
	@echo "  FILE_MB=N     File size in MB"
	@./datagen/target/release/tpcds-datagen --config $(CONFIG) \
		$(if $(SF),--scale-factor $(SF)) \
		$(if $(GEN_THREADS),--generator-threads $(GEN_THREADS)) \
		$(if $(UP_THREADS),--uploader-threads $(UP_THREADS)) \
		$(if $(FILE_MB),--file-size-mb $(FILE_MB))

datagen-clean: ## Clean Rust datagen build artifacts
	@echo "Cleaning Rust datagen build artifacts..."
	@cd datagen && cargo clean
	@rm -rf /tmp/tpcds-datagen 2>/dev/null || true
	@echo "Rust datagen cleaned."

datagen-help: ## Show Rust datagen CLI help
	@cd datagen && cargo run --release -- --help

##@ Quick Commands

quick-start: install dry-run ## Install deps and validate config (first time setup)
	@echo "$(GREEN)Quick start complete! Run 'make run' to start the benchmark.$(NC)"

full-test: check-syntax test datagen-test ## Run all tests (Python + Rust)
	@echo "$(GREEN)All tests passed!$(NC)"

status: check-config cluster-status data-check ## Show current status of cluster and data
	@echo "Status check complete."
