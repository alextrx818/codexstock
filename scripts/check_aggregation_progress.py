#!/usr/bin/env python3
"""Check progress of aggregation generation"""

from pathlib import Path

datasets = ["global_crypto", "us_stocks_sip", "us_indices"]
intervals = [5, 15, 30, 60]

project_root = Path(__file__).parent.parent

print("Aggregation Progress Report")
print("=" * 60)

for dataset in datasets:
    print(f"\n{dataset}:")
    
    # Count source files
    source_dir = project_root / "data" / dataset / "1MINUTE_BARS"
    if source_dir.exists():
        source_count = len(list(source_dir.glob("*.csv")))
        print(f"  Source files (1-minute): {source_count}")
    else:
        print(f"  Source directory not found!")
        continue
    
    # Count generated files for each interval
    for interval in intervals:
        output_dir = project_root / "data" / dataset / f"{interval}MINUTE_BARS"
        if output_dir.exists():
            output_count = len(list(output_dir.glob("*.csv")))
            progress = (output_count / source_count * 100) if source_count > 0 else 0
            print(f"  {interval:2d}-minute bars: {output_count:4d} files ({progress:5.1f}% complete)")
        else:
            print(f"  {interval:2d}-minute bars: Directory not created yet")

print("\n" + "=" * 60)

# Check if process is still running
import subprocess
try:
    result = subprocess.run(['pgrep', '-f', 'generate_and_validate_aggregates.py'], 
                          capture_output=True, text=True)
    if result.returncode == 0:
        print("✓ Aggregation script is still running")
    else:
        print("✗ Aggregation script has finished or stopped")
except:
    print("Could not check process status")
