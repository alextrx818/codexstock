#!/usr/bin/env python3
import time
import os
from pathlib import Path
from datetime import datetime

def count_files(directory):
    """Count CSV files in a directory"""
    if directory.exists():
        return len(list(directory.glob("*.csv")))
    return 0

def main():
    project_root = Path(__file__).parent.parent
    dataset = "data/us_stocks_sip"
    
    # Total expected files
    total_files = 500
    
    print("\n" + "="*60)
    print("AGGREGATION PROGRESS MONITOR")
    print("="*60)
    print(f"Dataset: {dataset}")
    print(f"Total files to process: {total_files}")
    print("="*60 + "\n")
    
    while True:
        os.system('clear')  # Clear screen
        
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] AGGREGATION PROGRESS")
        print("="*60)
        
        for interval in [5, 15, 30, 60]:
            out_dir = project_root / dataset / f"{interval}MINUTE_BARS"
            count = count_files(out_dir)
            percentage = (count / total_files) * 100
            
            # Progress bar
            bar_length = 40
            filled = int(bar_length * count / total_files)
            bar = "█" * filled + "░" * (bar_length - filled)
            
            print(f"{interval:2d}-min: [{bar}] {count:3d}/{total_files} ({percentage:5.1f}%)")
        
        print("\n" + "="*60)
        
        # Check if all complete
        all_complete = all(
            count_files(project_root / dataset / f"{i}MINUTE_BARS") >= total_files 
            for i in [5, 15, 30, 60]
        )
        
        if all_complete:
            print("\n✅ ALL AGGREGATIONS COMPLETE!")
            break
        else:
            # Estimate completion
            min_count = min(
                count_files(project_root / dataset / f"{i}MINUTE_BARS") 
                for i in [5, 15, 30, 60]
            )
            remaining = total_files - min_count
            print(f"\nFiles remaining: ~{remaining}")
            print("\nPress Ctrl+C to stop monitoring")
        
        time.sleep(5)  # Update every 5 seconds

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped.")
