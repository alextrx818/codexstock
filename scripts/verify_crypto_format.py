#!/usr/bin/env python3
"""
Quick verification that aggregated bars maintain the same format as 1-minute bars.
"""

import pandas as pd
import os

BASE_DIR = "/root/stock_project/data/global_crypto"

def check_format(interval):
    """Check format of aggregated bars."""
    print(f"\nChecking {interval}-minute bars format:")
    
    # Get a sample file
    dir_path = os.path.join(BASE_DIR, f"{interval}MINUTE_BARS")
    if not os.path.exists(dir_path):
        print(f"  Directory doesn't exist yet")
        return
    
    files = [f for f in os.listdir(dir_path) if f.endswith('.csv')]
    if not files:
        print(f"  No files found yet")
        return
    
    # Read first file
    sample_file = os.path.join(dir_path, files[0])
    df = pd.read_csv(sample_file)
    
    print(f"  Columns: {list(df.columns)}")
    print(f"  Expected: ['ticker', 'volume', 'open', 'close', 'high', 'low', 'window_start', 'transactions']")
    print(f"  Column order matches: {list(df.columns) == ['ticker', 'volume', 'open', 'close', 'high', 'low', 'window_start', 'transactions']}")
    
    # Show sample data
    print(f"\n  Sample data from {files[0]}:")
    print(df.head(3).to_string(index=False))
    
    # Check data types
    print(f"\n  Data types:")
    print(f"    window_start type: {df['window_start'].dtype}")
    print(f"    window_start sample: {df['window_start'].iloc[0]}")
    print(f"    Is nanoseconds: {df['window_start'].iloc[0] > 10**15}")

# Check 1-minute format first
print("Original 1-minute bars format:")
df_1min = pd.read_csv(os.path.join(BASE_DIR, "1MINUTE_BARS/2025-03-01.csv"))
print(f"  Columns: {list(df_1min.columns)}")
print(f"  Sample data:")
print(df_1min.head(3).to_string(index=False))

# Check each interval
for interval in [5, 15, 30, 60]:
    check_format(interval)
