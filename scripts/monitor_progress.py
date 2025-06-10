#!/usr/bin/env python3
"""
Real-time progress monitor for aggregation jobs
"""
import time
from pathlib import Path
import os

ROOT = Path("/root/stock_project/data/us_indices")
INTERVALS = [5, 15, 30, 60]

def count_files(directory):
    """Count CSV files in a directory"""
    if directory.exists():
        return len(list(directory.glob("*.csv")))
    return 0

def main():
    # Count source files
    source_dir = ROOT / "1MINUTE_BARS"
    total_files = count_files(source_dir)
    
    print(f"Monitoring aggregation progress...")
    print(f"Total source files: {total_files}")
    print("-" * 60)
    
    while True:
        os.system('clear')
        print(f"AGGREGATION PROGRESS MONITOR - {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total source files: {total_files}")
        print("-" * 60)
        
        total_completed = 0
        for interval in INTERVALS:
            out_dir = ROOT / f"{interval}MINUTE_BARS"
            count = count_files(out_dir)
            pct = (count / total_files * 100) if total_files > 0 else 0
            bar = "█" * int(pct / 2) + "░" * (50 - int(pct / 2))
            print(f"{interval:2d}-min: [{bar}] {count:3d}/{total_files} ({pct:5.1f}%)")
            total_completed += count
        
        avg_pct = (total_completed / (total_files * len(INTERVALS)) * 100) if total_files > 0 else 0
        print("-" * 60)
        print(f"Overall: {avg_pct:.1f}% complete")
        
        # Check if processes are still running
        print("\nActive processes:")
        os.system("ps aux | grep aggregate | grep -v grep | grep -v monitor | awk '{print $2, $11, $12}'")
        
        time.sleep(2)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nMonitoring stopped.")
