#!/usr/bin/env python3
from pathlib import Path
import pandas as pd
from collections import defaultdict

# adjust as needed
BASE_DIRS = {
    'crypto'      : Path('data/global_crypto/1MINUTE_BARS'),
    'us_stocks'   : Path('data/us_stocks_sip/1MINUTE_BARS'),
    'us_indices'  : Path('data/us_indices/1MINUTE_BARS'),
}

# collect column sets
col_sets = defaultdict(set)
file_counts = {}

for name, folder in BASE_DIRS.items():
    csv_files = list(folder.glob('*.csv'))
    file_counts[name] = len(csv_files)
    
    print(f"Scanning {name}: {len(csv_files)} files...")
    
    # Sample a few files to get column info
    sample_size = min(10, len(csv_files))  # Check up to 10 files
    for i, fp in enumerate(csv_files[:sample_size]):
        df = pd.read_csv(fp, nrows=0)  # read only header
        col_sets[name].update(df.columns.tolist())

# union of all columns
all_cols = set().union(*col_sets.values())

# print report
print("\n" + "="*60)
print("SCHEMA DISCOVERY REPORT")
print("="*60)

print("\nFile counts:")
for name, count in file_counts.items():
    print(f"  {name}: {count} files")

print("\nColumns by dataset:")
for name, cols in sorted(col_sets.items()):
    missing = all_cols - cols
    print(f"\n{name} ({len(cols)} columns):")
    print("  Columns present:", sorted(cols))
    if missing:
        print("  Missing columns (vs union):", sorted(missing))

# Show column overlap
print("\n" + "-"*60)
print("COLUMN ANALYSIS:")
print("-"*60)

# Common columns across all datasets
common_cols = set.intersection(*col_sets.values()) if col_sets else set()
print(f"\nColumns in ALL datasets ({len(common_cols)}):")
print("  ", sorted(common_cols))

# Unique columns per dataset
print("\nUnique columns per dataset:")
for name, cols in sorted(col_sets.items()):
    other_cols = set().union(*[c for n, c in col_sets.items() if n != name])
    unique = cols - other_cols
    if unique:
        print(f"  {name} only: {sorted(unique)}")

# Column presence matrix
print("\n" + "-"*60)
print("COLUMN PRESENCE MATRIX:")
print("-"*60)
print(f"{'Column':<20} | " + " | ".join(f"{name:<10}" for name in sorted(col_sets.keys())))
print("-" * 60)

for col in sorted(all_cols):
    row = f"{col:<20} | "
    for name in sorted(col_sets.keys()):
        presence = "✓" if col in col_sets[name] else "✗"
        row += f"{presence:<10} | "
    print(row)
