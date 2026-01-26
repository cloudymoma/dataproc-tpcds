#!/bin/bash
# One-time asset build script for TPC-DS data generation
# This script builds the spark-sql-perf JAR and tpcds-kit binary
# Run this once on a Linux machine before using the tool
#
# The script continues even if one component fails, allowing both
# assets to be built independently. A summary is shown at the end.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
TMP_DIR="$PROJECT_ROOT/tmp"
ASSETS_DIR="$PROJECT_ROOT/assets"

# Version tracking
SPARK_SQL_PERF_VERSION="0.5.1"
TPCDS_KIT_VERSION="1.0.0"
TPCDS_DATAGEN_VERSION="1.0.0"

# Build status tracking
SPARK_SQL_PERF_STATUS="not attempted"
TPCDS_KIT_STATUS="not attempted"
TPCDS_DATAGEN_STATUS="not attempted"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Building TPC-DS Data Generation Assets ===${NC}"
echo "Project root: $PROJECT_ROOT"
echo "Assets will be created in: $ASSETS_DIR"
echo ""

# Check prerequisites
check_prereqs() {
    echo -e "${YELLOW}Checking prerequisites...${NC}"

    local missing=()
    local warnings=()

    if ! command -v git &> /dev/null; then
        missing+=("git")
    fi

    if ! command -v sbt &> /dev/null; then
        warnings+=("sbt (Scala Build Tool) - required for spark-sql-perf JAR")
    fi

    if ! command -v make &> /dev/null; then
        missing+=("make")
    fi

    if ! command -v gcc &> /dev/null; then
        warnings+=("gcc - required for tpcds-kit binary")
    fi

    if [ ${#missing[@]} -ne 0 ]; then
        echo -e "${RED}Error: Missing critical prerequisites:${NC}"
        for prereq in "${missing[@]}"; do
            echo "  - $prereq"
        done
        echo ""
        echo "Please install the missing tools and try again."
        echo "On Debian/Ubuntu: sudo apt-get install git make"
        exit 1
    fi

    if [ ${#warnings[@]} -ne 0 ]; then
        echo -e "${YELLOW}Warning: Some optional tools are missing:${NC}"
        for prereq in "${warnings[@]}"; do
            echo "  - $prereq"
        done
        echo ""
        echo "Some assets may not be built. Continuing anyway..."
        echo ""
    fi

    echo -e "${GREEN}Prerequisite check complete.${NC}"
}

# Create directories
mkdir -p "$TMP_DIR" "$ASSETS_DIR"

# Check prerequisites
check_prereqs

# Build spark-sql-perf (Fat JAR)
build_spark_sql_perf() {
    echo ""
    echo -e "${YELLOW}Building spark-sql-perf...${NC}"

    # Check if sbt is available
    if ! command -v sbt &> /dev/null; then
        echo -e "${RED}Error: sbt not found. Skipping spark-sql-perf build.${NC}"
        echo "Install sbt: https://www.scala-sbt.org/download.html"
        SPARK_SQL_PERF_STATUS="skipped (sbt not found)"
        return 1
    fi

    # Clone or update repository
    if [ ! -d "$TMP_DIR/spark-sql-perf" ]; then
        echo "Cloning spark-sql-perf repository..."
        if ! git clone https://github.com/databricks/spark-sql-perf "$TMP_DIR/spark-sql-perf"; then
            echo -e "${RED}Error: Failed to clone spark-sql-perf repository${NC}"
            SPARK_SQL_PERF_STATUS="failed (git clone error)"
            return 1
        fi
    else
        echo "Updating existing spark-sql-perf clone..."
    fi

    cd "$TMP_DIR/spark-sql-perf"
    git fetch origin
    git checkout master
    git pull origin master

    echo "Building assembly JAR (this may take several minutes)..."
    if ! sbt assembly; then
        echo -e "${RED}Error: sbt assembly failed${NC}"
        SPARK_SQL_PERF_STATUS="failed (sbt assembly error)"
        return 1
    fi

    # Find the built JAR
    JAR_FILE=$(find target -name "spark-sql-perf-assembly-*.jar" 2>/dev/null | head -1)

    if [ -z "$JAR_FILE" ]; then
        echo -e "${RED}Error: Could not find built JAR file${NC}"
        SPARK_SQL_PERF_STATUS="failed (JAR not found)"
        return 1
    fi

    cp "$JAR_FILE" "$ASSETS_DIR/spark-sql-perf-assembly-${SPARK_SQL_PERF_VERSION}.jar"
    echo -e "${GREEN}spark-sql-perf JAR built successfully.${NC}"
    SPARK_SQL_PERF_STATUS="success"
    return 0
}

# Build tpcds-datagen (Custom Main Class JAR)
build_tpcds_datagen() {
    echo ""
    echo -e "${YELLOW}Building tpcds-datagen...${NC}"

    # Check if sbt is available
    if ! command -v sbt &> /dev/null; then
        echo -e "${RED}Error: sbt not found. Skipping tpcds-datagen build.${NC}"
        TPCDS_DATAGEN_STATUS="skipped (sbt not found)"
        return 1
    fi

    # Check if spark-sql-perf JAR exists
    if [ ! -f "$ASSETS_DIR/spark-sql-perf-assembly-${SPARK_SQL_PERF_VERSION}.jar" ]; then
        echo -e "${RED}Error: spark-sql-perf JAR not found. Build it first.${NC}"
        TPCDS_DATAGEN_STATUS="skipped (spark-sql-perf JAR not found)"
        return 1
    fi

    cd "$PROJECT_ROOT/datagen-spark"

    # Create lib directory and copy spark-sql-perf JAR
    mkdir -p lib
    cp "$ASSETS_DIR/spark-sql-perf-assembly-${SPARK_SQL_PERF_VERSION}.jar" lib/

    echo "Compiling tpcds-datagen..."
    if ! sbt clean compile package; then
        echo -e "${RED}Error: sbt compile failed${NC}"
        TPCDS_DATAGEN_STATUS="failed (sbt compile error)"
        return 1
    fi

    # Find the built JAR
    JAR_FILE=$(find target -name "tpcds-datagen_*.jar" 2>/dev/null | head -1)

    if [ -z "$JAR_FILE" ]; then
        echo -e "${RED}Error: Could not find built JAR file${NC}"
        TPCDS_DATAGEN_STATUS="failed (JAR not found)"
        return 1
    fi

    cp "$JAR_FILE" "$ASSETS_DIR/tpcds-datagen-${TPCDS_DATAGEN_VERSION}.jar"
    echo -e "${GREEN}tpcds-datagen JAR built successfully.${NC}"
    TPCDS_DATAGEN_STATUS="success"
    return 0
}

# Build tpcds-kit (Native Binary)
build_tpcds_kit() {
    echo ""
    echo -e "${YELLOW}Building tpcds-kit...${NC}"

    # Check if gcc is available
    if ! command -v gcc &> /dev/null; then
        echo -e "${RED}Error: gcc not found. Skipping tpcds-kit build.${NC}"
        echo "Install gcc: sudo apt-get install gcc"
        TPCDS_KIT_STATUS="skipped (gcc not found)"
        return 1
    fi

    # Clone or update repository
    if [ ! -d "$TMP_DIR/tpcds-kit" ]; then
        echo "Cloning tpcds-kit repository..."
        if ! git clone https://github.com/databricks/tpcds-kit "$TMP_DIR/tpcds-kit"; then
            echo -e "${RED}Error: Failed to clone tpcds-kit repository${NC}"
            TPCDS_KIT_STATUS="failed (git clone error)"
            return 1
        fi
    else
        echo "Updating existing tpcds-kit clone..."
    fi

    cd "$TMP_DIR/tpcds-kit"
    git fetch origin
    git checkout master
    git pull origin master

    cd tools

    echo "Cleaning previous build..."
    make OS=LINUX clean 2>/dev/null || true

    echo "Building dsdgen binary..."
    # Build only dsdgen (not dsqgen which requires yacc/bison)
    # Add CFLAGS to work around stricter GCC 14+ implicit-int errors
    if ! make OS=LINUX CFLAGS="-D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE -DYYDEBUG -DLINUX -O3 -fcommon -Wno-error=implicit-int -Wno-error=implicit-function-declaration" dsdgen; then
        echo -e "${RED}Error: make failed${NC}"
        TPCDS_KIT_STATUS="failed (make error)"
        return 1
    fi

    # Verify dsdgen was built
    if [ ! -f "dsdgen" ]; then
        echo -e "${RED}Error: dsdgen binary was not created${NC}"
        TPCDS_KIT_STATUS="failed (dsdgen not found)"
        return 1
    fi

    echo "Creating tarball..."
    cd "$TMP_DIR/tpcds-kit"
    if ! tar -czf "$ASSETS_DIR/tpcds-kit-${TPCDS_KIT_VERSION}.tar.gz" .; then
        echo -e "${RED}Error: Failed to create tarball${NC}"
        TPCDS_KIT_STATUS="failed (tar error)"
        return 1
    fi

    echo -e "${GREEN}tpcds-kit built successfully.${NC}"
    TPCDS_KIT_STATUS="success"
    return 0
}

# Create version manifest
create_manifest() {
    echo ""
    echo -e "${YELLOW}Creating version manifest...${NC}"

    cat > "$ASSETS_DIR/manifest.json" << EOF
{
    "spark_sql_perf_version": "${SPARK_SQL_PERF_VERSION}",
    "spark_sql_perf_status": "${SPARK_SQL_PERF_STATUS}",
    "tpcds_datagen_version": "${TPCDS_DATAGEN_VERSION}",
    "tpcds_datagen_status": "${TPCDS_DATAGEN_STATUS}",
    "tpcds_kit_version": "${TPCDS_KIT_VERSION}",
    "tpcds_kit_status": "${TPCDS_KIT_STATUS}",
    "build_date": "$(date -Iseconds)",
    "spark_version": "3.x",
    "scala_version": "2.12",
    "platform": "linux-$(uname -m)"
}
EOF

    echo -e "${GREEN}Manifest created.${NC}"
}

# Print troubleshooting tips
print_troubleshooting() {
    echo ""
    echo -e "${YELLOW}=== Troubleshooting ===${NC}"
    echo ""

    if [[ "$SPARK_SQL_PERF_STATUS" == *"sbt"* ]] || [[ "$SPARK_SQL_PERF_STATUS" == *"assembly"* ]]; then
        echo -e "${YELLOW}spark-sql-perf build failed:${NC}"
        echo "  If you see Ivy/Maven resolution errors like 'origin location must be absolute',"
        echo "  try clearing the dependency caches:"
        echo ""
        echo "    # Clear Ivy, SBT, and Maven caches"
        echo "    rm -rf ~/.ivy2/cache ~/.sbt/boot ~/.sbt/1.0/staging ~/.m2/repository"
        echo ""
        echo "    # Clear spark-sql-perf build artifacts"
        echo "    rm -rf $TMP_DIR/spark-sql-perf/target $TMP_DIR/spark-sql-perf/project/target"
        echo ""
        echo "    # Then re-run the build"
        echo "    make build-assets"
        echo ""
    fi

    if [[ "$TPCDS_KIT_STATUS" == *"make"* ]] || [[ "$TPCDS_KIT_STATUS" == *"gcc"* ]]; then
        echo -e "${YELLOW}tpcds-kit build failed:${NC}"
        echo "  - Ensure gcc and make are installed: sudo apt-get install gcc make"
        echo "  - For GCC compilation errors, the script already includes workarounds"
        echo "    for GCC 14+ strictness. If issues persist, try an older GCC version."
        echo ""
    fi

    if [[ "$TPCDS_KIT_STATUS" == *"yacc"* ]] || [[ "$TPCDS_KIT_STATUS" == *"bison"* ]]; then
        echo -e "${YELLOW}yacc/bison error:${NC}"
        echo "  This should not happen as we only build dsdgen. If you see this error,"
        echo "  please report it as a bug."
        echo ""
    fi
}

# Print build summary
print_summary() {
    echo ""
    echo -e "${GREEN}=== Build Summary ===${NC}"
    echo ""

    # spark-sql-perf status
    if [ "$SPARK_SQL_PERF_STATUS" = "success" ]; then
        echo -e "  spark-sql-perf JAR: ${GREEN}SUCCESS${NC}"
        echo "    -> $ASSETS_DIR/spark-sql-perf-assembly-${SPARK_SQL_PERF_VERSION}.jar"
    else
        echo -e "  spark-sql-perf JAR: ${RED}$SPARK_SQL_PERF_STATUS${NC}"
    fi

    # tpcds-datagen status
    if [ "$TPCDS_DATAGEN_STATUS" = "success" ]; then
        echo -e "  tpcds-datagen JAR:  ${GREEN}SUCCESS${NC}"
        echo "    -> $ASSETS_DIR/tpcds-datagen-${TPCDS_DATAGEN_VERSION}.jar"
    else
        echo -e "  tpcds-datagen JAR:  ${RED}$TPCDS_DATAGEN_STATUS${NC}"
    fi

    # tpcds-kit status
    if [ "$TPCDS_KIT_STATUS" = "success" ]; then
        echo -e "  tpcds-kit binary:   ${GREEN}SUCCESS${NC}"
        echo "    -> $ASSETS_DIR/tpcds-kit-${TPCDS_KIT_VERSION}.tar.gz"
    else
        echo -e "  tpcds-kit binary:   ${RED}$TPCDS_KIT_STATUS${NC}"
    fi

    echo ""

    # Show assets directory
    if [ -d "$ASSETS_DIR" ]; then
        echo "Assets directory contents:"
        ls -lh "$ASSETS_DIR"
        echo ""
    fi

    # Determine exit status
    local success_count=0
    [ "$SPARK_SQL_PERF_STATUS" = "success" ] && ((success_count++))
    [ "$TPCDS_DATAGEN_STATUS" = "success" ] && ((success_count++))
    [ "$TPCDS_KIT_STATUS" = "success" ] && ((success_count++))

    if [ $success_count -eq 3 ]; then
        echo -e "${GREEN}All assets built successfully!${NC}"
        echo ""
        echo "Next steps:"
        echo "  1. Commit the assets/ directory to your repository"
        echo "  2. Or upload assets to a GCS bucket for distribution"
        return 0
    elif [ $success_count -ge 1 ]; then
        echo -e "${YELLOW}Partial success: $success_count of 3 assets built.${NC}"
        echo "You may still be able to use the tool with the available assets."
        echo "Fix the errors above and re-run to build the missing assets."
        print_troubleshooting
        return 1
    else
        echo -e "${RED}Build failed: No assets were built.${NC}"
        echo "Please fix the errors above and try again."
        print_troubleshooting
        return 1
    fi
}

# Main execution
build_spark_sql_perf
build_tpcds_datagen
build_tpcds_kit
create_manifest
print_summary

exit $?
