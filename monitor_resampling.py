#!/usr/bin/env python3
"""
Monitor 5-minute resampling progress
Shows files remaining for each asset class
"""

import os
import glob
import time
from datetime import datetime

def get_progress(asset_class, base_dir='/root/stock_project/data'):
    """Get progress for a specific asset class"""
    input_dir = os.path.join(base_dir, asset_class, '1MINUTE_BARS')
    output_dir = os.path.join(base_dir, asset_class, '5MINUTE_BARS')
    
    # Count input files
    input_files = glob.glob(os.path.join(input_dir, '*.csv'))
    total_files = len(input_files)
    
    # Count output files
    output_files = glob.glob(os.path.join(output_dir, '*.csv'))
    processed_files = len(output_files)
    
    remaining = total_files - processed_files
    
    return {
        'total': total_files,
        'processed': processed_files,
        'remaining': remaining,
        'percentage': (processed_files / total_files * 100) if total_files > 0 else 0
    }

def main():
    """Main monitoring loop"""
    asset_classes = ['global_crypto', 'us_indices', 'us_stocks_sip']
    
    print("\033[2J\033[H")  # Clear screen
    
    while True:
        print("\033[H")  # Move cursor to top
        print("="*70)
        print(f"5-MINUTE RESAMPLING PROGRESS MONITOR")
        print(f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)
        print()
        
        total_remaining = 0
        
        for asset_class in asset_classes:
            progress = get_progress(asset_class)
            total_remaining += progress['remaining']
            
            # Progress bar
            bar_length = 40
            filled = int(bar_length * progress['percentage'] / 100)
            bar = '█' * filled + '░' * (bar_length - filled)
            
            status = "✓ COMPLETE" if progress['remaining'] == 0 else f"⚡ PROCESSING"
            
            print(f"{asset_class.upper():<20} {status}")
            print(f"[{bar}] {progress['percentage']:.1f}%")
            print(f"Processed: {progress['processed']:,}/{progress['total']:,} files")
            print(f"Remaining: {progress['remaining']:,} files")
            print()
        
        print("="*70)
        print(f"TOTAL FILES REMAINING: {total_remaining:,}")
        
        if total_remaining == 0:
            print("\n🎉 ALL RESAMPLING COMPLETE! 🎉")
            break
        else:
            # Estimate time remaining (rough estimate based on crypto processing rate)
            # Assuming ~2 files per second based on observed rate
            est_minutes = total_remaining / 120  # 2 files/sec = 120 files/min
            print(f"Estimated time remaining: ~{est_minutes:.1f} minutes")
        
        print("\nPress Ctrl+C to exit monitor")
        
        time.sleep(5)  # Update every 5 seconds

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nMonitor stopped.")
