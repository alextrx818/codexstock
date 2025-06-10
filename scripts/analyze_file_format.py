#!/usr/bin/env python3
"""
Analyze the exact format of aggregated files
"""
import pandas as pd

def analyze_format():
    # Check 1-minute format
    print("1-MINUTE BAR FORMAT:")
    print("="*60)
    df1 = pd.read_csv("/root/stock_project/data/us_stocks_sip/1MINUTE_BARS/2024-09-19.csv", nrows=5)
    print("Columns:", list(df1.columns))
    print("\nFirst row data:")
    for col in df1.columns:
        print(f"  {col}: {df1.iloc[0][col]}")
    
    # Check 5-minute format (no header)
    print("\n\n5-MINUTE BAR FORMAT (RAW):")
    print("="*60)
    df5_raw = pd.read_csv("/root/stock_project/data/us_stocks_sip/5MINUTE_BARS/2024-09-19.csv", header=None, nrows=5)
    print("Number of columns:", len(df5_raw.columns))
    print("\nFirst row values:")
    for i, val in enumerate(df5_raw.iloc[0]):
        print(f"  Column {i}: {val}")
    
    # Let's check the actual aggregation script output format
    print("\n\nCHECKING AGGREGATION SCRIPT:")
    print("="*60)
    
    # Look at generate_and_validate_aggregates.py to understand column order
    import subprocess
    result = subprocess.run(['grep', '-A5', '-B5', 'columns.*=', '/root/stock_project/scripts/generate_and_validate_aggregates.py'], 
                          capture_output=True, text=True)
    if result.returncode == 0:
        print("Column definitions in aggregation script:")
        print(result.stdout)

if __name__ == "__main__":
    analyze_format()
