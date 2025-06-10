#!/usr/bin/env python3
"""
Investigate validation failures by examining specific failed cases
"""

import pandas as pd
import numpy as np
from pathlib import Path
import random

def investigate_aggregation_failure(dataset: str, date: str, interval: int):
    """Investigate why aggregation failed for a specific date and interval"""
    
    print(f"\nInvestigating {dataset} - {date} - {interval}-minute aggregation")
    print("="*60)
    
    # Paths
    one_min_file = Path(f"/root/stock_project/data/{dataset}/1MINUTE_BARS/{date}.csv")
    agg_file = Path(f"/root/stock_project/data/{dataset}/{interval}MINUTE_BARS/{date}.csv")
    
    if not one_min_file.exists():
        print(f"ERROR: 1-minute file not found: {one_min_file}")
        return
        
    if not agg_file.exists():
        print(f"ERROR: {interval}-minute file not found: {agg_file}")
        return
    
    # Read files
    df_1min = pd.read_csv(one_min_file)
    df_agg = pd.read_csv(agg_file)
    
    print(f"\n1-minute file columns: {list(df_1min.columns)}")
    print(f"{interval}-minute file columns: {list(df_agg.columns)}")
    
    print(f"\n1-minute file shape: {df_1min.shape}")
    print(f"{interval}-minute file shape: {df_agg.shape}")
    
    # Sample a ticker to investigate
    common_tickers = set(df_1min['ticker'].unique()) & set(df_agg['ticker'].unique())
    if not common_tickers:
        print("ERROR: No common tickers between files!")
        return
        
    ticker = random.choice(list(common_tickers))
    print(f"\nInvestigating ticker: {ticker}")
    
    # Get data for this ticker
    ticker_1min = df_1min[df_1min['ticker'] == ticker].copy()
    ticker_agg = df_agg[df_agg['ticker'] == ticker].copy()
    
    # Sort by timestamp
    ticker_1min = ticker_1min.sort_values('window_start')
    ticker_agg = ticker_agg.sort_values('window_start')
    
    print(f"\n1-minute bars for {ticker}: {len(ticker_1min)}")
    print(f"{interval}-minute bars for {ticker}: {len(ticker_agg)}")
    
    # Check first aggregated bar
    if len(ticker_agg) > 0:
        first_agg = ticker_agg.iloc[0]
        agg_start = first_agg['window_start']
        agg_end = agg_start + (interval * 60 * 1_000_000_000)  # nanoseconds
        
        # Get corresponding 1-minute bars
        mask = (ticker_1min['window_start'] >= agg_start) & (ticker_1min['window_start'] < agg_end)
        corresponding_1min = ticker_1min[mask]
        
        print(f"\nFirst {interval}-minute bar:")
        print(f"  Start: {agg_start}")
        print(f"  Open: {first_agg['open']}")
        print(f"  High: {first_agg['high']}")
        print(f"  Low: {first_agg['low']}")
        print(f"  Close: {first_agg['close']}")
        if 'volume' in first_agg:
            print(f"  Volume: {first_agg['volume']}")
        if 'transactions' in first_agg:
            print(f"  Transactions: {first_agg['transactions']}")
        
        print(f"\nCorresponding 1-minute bars: {len(corresponding_1min)}")
        if len(corresponding_1min) > 0:
            print(f"  Expected Open: {corresponding_1min.iloc[0]['open']}")
            print(f"  Expected High: {corresponding_1min['high'].max()}")
            print(f"  Expected Low: {corresponding_1min['low'].min()}")
            print(f"  Expected Close: {corresponding_1min.iloc[-1]['close']}")
            if 'volume' in corresponding_1min.columns:
                print(f"  Expected Volume: {corresponding_1min['volume'].sum()}")
            if 'transactions' in corresponding_1min.columns:
                print(f"  Expected Transactions: {corresponding_1min['transactions'].sum()}")
            
            # Check for mismatches
            tolerance = 1e-8
            if abs(first_agg['open'] - corresponding_1min.iloc[0]['open']) > tolerance:
                print("  ❌ OPEN MISMATCH!")
            if abs(first_agg['high'] - corresponding_1min['high'].max()) > tolerance:
                print("  ❌ HIGH MISMATCH!")
            if abs(first_agg['low'] - corresponding_1min['low'].min()) > tolerance:
                print("  ❌ LOW MISMATCH!")
            if abs(first_agg['close'] - corresponding_1min.iloc[-1]['close']) > tolerance:
                print("  ❌ CLOSE MISMATCH!")
            if 'volume' in first_agg and 'volume' in corresponding_1min.columns:
                if abs(first_agg['volume'] - corresponding_1min['volume'].sum()) > tolerance:
                    print("  ❌ VOLUME MISMATCH!")
    
    # Check timestamp alignment
    print(f"\nChecking timestamp alignment...")
    if len(ticker_1min) > 0:
        first_ts = ticker_1min.iloc[0]['window_start']
        # Convert to pandas timestamp
        first_dt = pd.to_datetime(first_ts, unit='ns')
        print(f"  First 1-minute timestamp: {first_ts} ({first_dt})")
        print(f"  Minute of hour: {first_dt.minute}")
        print(f"  Should align to {interval}-minute boundary: {first_dt.minute % interval == 0}")

def main():
    # Investigate some specific failures
    
    # Check global_crypto failures
    print("Investigating global_crypto failures...")
    
    # Pick some dates to investigate
    dates_to_check = [
        "2023-06-09",  # First date
        "2024-01-15",  # Random middle date
        "2025-06-07"   # Last date
    ]
    
    for date in dates_to_check:
        for interval in [5, 15, 30, 60]:
            investigate_aggregation_failure("global_crypto", date, interval)
            print("\n" + "-"*80)

if __name__ == "__main__":
    main()
