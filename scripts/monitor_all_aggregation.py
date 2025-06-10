#!/usr/bin/env python3
"""
Comprehensive real-time progress monitor for all aggregation jobs
"""
import time
from pathlib import Path
import os
import subprocess

ROOT = Path("/root/stock_project/data")
DATASETS = ["global_crypto", "us_stocks_sip", "us_indices"]
INTERVALS = [5, 15, 30, 60]

def count_files(directory):
    """Count CSV files in a directory"""
    if directory.exists():
        return len(list(directory.glob("*.csv")))
    return 0

def get_dir_size(directory):
    """Get directory size in MB"""
    if directory.exists():
        result = subprocess.run(['du', '-sm', str(directory)], capture_output=True, text=True)
        if result.returncode == 0:
            return int(result.stdout.split()[0])
    return 0

def main():
    print("Initializing aggregation monitor...")
    
    # Count source files for each dataset
    source_counts = {}
    for dataset in DATASETS:
        source_dir = ROOT / dataset / "1MINUTE_BARS"
        source_counts[dataset] = count_files(source_dir)
    
    while True:
        os.system('clear')
        print(f"AGGREGATION PROGRESS MONITOR - {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        for dataset in DATASETS:
            print(f"\n{dataset.upper()}")
            print("-" * 60)
            
            source_count = source_counts[dataset]
            print(f"Source files: {source_count}")
            
            if source_count == 0:
                print("No source files found")
                continue
            
            for interval in INTERVALS:
                out_dir = ROOT / dataset / f"{interval}MINUTE_BARS"
                count = count_files(out_dir)
                size_mb = get_dir_size(out_dir)
                pct = (count / source_count * 100) if source_count > 0 else 0
                bar = "█" * int(pct / 2) + "░" * (50 - int(pct / 2))
                print(f"{interval:2d}-min: [{bar}] {count:4d}/{source_count} ({pct:5.1f}%) {size_mb:6d} MB")
        
        # Check active processes
        print("\n" + "=" * 80)
        print("ACTIVE AGGREGATION PROCESSES:")
        print("-" * 80)
        
        # Get process info
        result = subprocess.run(
            ["ps", "aux"], 
            capture_output=True, 
            text=True
        )
        
        if result.returncode == 0:
            processes = []
            for line in result.stdout.split('\n'):
                if 'aggregate' in line and 'python' in line and 'monitor' not in line:
                    parts = line.split()
                    if len(parts) >= 11:
                        pid = parts[1]
                        cpu = parts[2]
                        mem = parts[3]
                        start_time = parts[8]
                        script = parts[10].split('/')[-1] if '/' in parts[10] else parts[10]
                        processes.append((pid, cpu, mem, start_time, script))
            
            if processes:
                print(f"{'PID':>8} {'CPU%':>6} {'MEM%':>6} {'START':>8} {'SCRIPT'}")
                for pid, cpu, mem, start_time, script in processes:
                    print(f"{pid:>8} {cpu:>6} {mem:>6} {start_time:>8} {script}")
            else:
                print("No active aggregation processes found")
        
        # Calculate overall progress
        print("\n" + "=" * 80)
        total_expected = sum(source_counts.values()) * len(INTERVALS)
        total_completed = 0
        
        for dataset in DATASETS:
            for interval in INTERVALS:
                out_dir = ROOT / dataset / f"{interval}MINUTE_BARS"
                total_completed += count_files(out_dir)
        
        overall_pct = (total_completed / total_expected * 100) if total_expected > 0 else 0
        print(f"OVERALL PROGRESS: {total_completed}/{total_expected} files ({overall_pct:.1f}%)")
        
        # Estimate completion time based on current rate
        # This is a rough estimate - would need historical data for accuracy
        print("\nPress Ctrl+C to stop monitoring")
        
        time.sleep(5)  # Update every 5 seconds

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped.")
