#!/usr/bin/env python3
"""Check TPC-DS data existence and completeness."""

import sys
import yaml
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from lib.data_generator import DataGenerator


def main():
    config_path = sys.argv[1] if len(sys.argv) > 1 else "conf.yaml"

    with open(config_path) as f:
        config = yaml.safe_load(f)

    dg = DataGenerator(config)
    stats = dg.get_data_stats()

    table_count = stats['table_count']
    has_marker = stats.get('has_success_marker', False)
    is_complete = stats.get('is_complete', False)

    print()
    print("=" * 60)
    print("TPC-DS Data Status Report")
    print("=" * 60)
    print(f"Data Path:        {stats['data_path']}")
    print(f"Scale Factor:     {stats['scale_factor']} GB")
    print(f"Tables Found:     {table_count} / 24")
    print(f"_SUCCESS Marker:  {'YES' if has_marker else 'NO'}")
    print(f"Total Size:       {stats['total_size_human']}")
    print("-" * 60)

    # Status summary with actionable guidance
    if table_count == 0:
        print("Status: NO DATA")
        print("  No tables found at the data path.")
        print("  Action: Run 'make data-gen' to generate TPC-DS data.")
    elif table_count < 24:
        print(f"Status: INCOMPLETE ({table_count}/24 tables)")
        print("  Data generation appears to have failed or is in progress.")
        print("  Action: Run 'make data-gen' with 'overwrite: true' to regenerate.")
    elif table_count == 24 and not has_marker:
        print("Status: TABLES COMPLETE, MARKER MISSING")
        print("  All 24 tables exist but _SUCCESS marker is missing.")
        print("  This may be from a previous data generation before marker support.")
        print("  Options:")
        print("    1. Create marker: gsutil touch " + stats['data_path'] + "/_SUCCESS")
        print("    2. Regenerate: Set 'overwrite: true' and run 'make data-gen'")
    elif is_complete:
        print("Status: COMPLETE")
        print("  All 24 tables and _SUCCESS marker present.")
        print("  Data is ready for benchmarking. Run 'make run' to start.")

    print("-" * 60)

    if stats['tables']:
        print("Table Details:")
        for t in stats['tables']:
            print(f"  {t['name']:30} {t['size_human']:>12}")

    print("=" * 60)

    if stats.get('error'):
        print(f"Error: {stats['error']}")

    # Always return 0 - this is a status report, not a validation
    return 0


if __name__ == "__main__":
    sys.exit(main())
