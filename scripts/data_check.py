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

    print()
    print("=" * 60)
    print("TPC-DS Data Status Report")
    print("=" * 60)
    print(f"Data Path:        {stats['data_path']}")
    print(f"Scale Factor:     {stats['scale_factor']} GB")
    print(f"Data Complete:    {'YES' if stats.get('is_complete') else 'NO'}")
    print(f"_SUCCESS Marker:  {'YES' if stats.get('has_success_marker') else 'NO'}")
    print(f"Total Size:       {stats['total_size_human']}")
    print(f"Tables Found:     {stats['table_count']} / 24")
    print("-" * 60)

    if stats['tables']:
        print("Table Details:")
        for t in stats['tables']:
            print(f"  {t['name']:30} {t['size_human']:>12}")
    else:
        print('No tables found. Run "make data-gen" to generate data.')

    print("=" * 60)

    if stats.get('error'):
        print(f"Error: {stats['error']}")

    # Always return 0 - this is a status report, not a validation
    return 0


if __name__ == "__main__":
    sys.exit(main())
