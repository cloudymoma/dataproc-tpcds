#!/bin/bash
# Performance benchmarking script for TPC-DS data generator

set -e

echo "TPC-DS Data Generator Performance Benchmark"
echo "==========================================="
echo ""

# Configuration
SCALE_FACTOR=1
GENERATOR_THREADS=8
UPLOADER_THREADS=4
FILE_SIZE_MB=128
TEMP_DIR="/tmp/tpcds-bench-$$"

# Create temp directory
mkdir -p "$TEMP_DIR"

echo "Configuration:"
echo "  Scale Factor: $SCALE_FACTOR"
echo "  Generator Threads: $GENERATOR_THREADS"
echo "  Uploader Threads: $UPLOADER_THREADS"
echo "  File Size: ${FILE_SIZE_MB}MB"
echo "  Temp Directory: $TEMP_DIR"
echo ""

# Build in release mode
echo "Building release binary..."
cargo build --release

# Run benchmark in dry-run mode (no upload)
echo ""
echo "Running benchmark (dry-run mode)..."
echo "Start time: $(date)"

# Use time command for precise timing
/usr/bin/time -v ./target/release/tpcds-datagen \
    --config ../conf.yaml \
    --scale-factor $SCALE_FACTOR \
    --generator-threads $GENERATOR_THREADS \
    --uploader-threads $UPLOADER_THREADS \
    --file-size-mb $FILE_SIZE_MB \
    --dry-run 2>&1 | tee benchmark_results.txt

echo "End time: $(date)"

# Calculate metrics
echo ""
echo "Performance Metrics:"
echo "===================="

# Extract statistics from output
if [ -f benchmark_results.txt ]; then
    echo "Files Generated: $(grep "Files Generated:" benchmark_results.txt | tail -1 | awk '{print $3}')"
    echo "Rows Generated: $(grep "Rows Generated:" benchmark_results.txt | tail -1 | awk '{print $3}')"
    echo "Data Generated: $(grep "Bytes Written:" benchmark_results.txt | tail -1 | awk '{print $3, $4}')"

    # Extract timing from time command output
    echo "Wall Clock Time: $(grep "Elapsed (wall clock)" benchmark_results.txt | awk '{print $8}')"
    echo "CPU Usage: $(grep "Percent of CPU" benchmark_results.txt | awk '{print $7}')"
    echo "Max Memory: $(grep "Maximum resident" benchmark_results.txt | awk '{print $6}') KB"

    # Calculate throughput
    BYTES=$(grep "Bytes Written:" benchmark_results.txt | tail -1 | awk '{print $3}')
    TIME_SECONDS=$(grep "Elapsed (wall clock)" benchmark_results.txt | awk -F: '{if (NF==2) print $1*60+$2; else print $1*3600+$2*60+$3}')

    if [ -n "$BYTES" ] && [ -n "$TIME_SECONDS" ]; then
        THROUGHPUT_MB=$(echo "scale=2; $BYTES / $TIME_SECONDS" | bc 2>/dev/null || echo "N/A")
        echo "Throughput: ${THROUGHPUT_MB} MB/s"
    fi
fi

# Cleanup
echo ""
echo "Cleaning up temp directory..."
rm -rf "$TEMP_DIR"
rm -f benchmark_results.txt

echo ""
echo "Benchmark complete!"