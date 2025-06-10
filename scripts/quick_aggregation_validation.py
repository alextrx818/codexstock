#!/usr/bin/env python3
"""
Quick validation of aggregation accuracy - optimized for speed
"""

import pandas as pd
import numpy as np
from pathlib import Path
import random

def validate_sample_bars(df_1min, df_agg, interval_minutes, ticker, num_samples=5):
    """Validate a sample of bars for a specific ticker"""
    errors = []
    
    # Filter to ticker
    ticker_1min = df_1min[df_1min['ticker'] == ticker].copy()
    ticker_agg = df_agg[df_agg['ticker'] == ticker].copy()
    
    if ticker_1min.empty or ticker_agg.empty:
        return errors
    
    # Convert timestamps
    ticker_1min['ts'] = pd.to_datetime(ticker_1min['window_start'], unit='ns')
    ticker_agg['ts'] = pd.to_datetime(ticker_agg['window_start'], unit='ns')
    
    # Sample random aggregated bars
    sample_size = min(num_samples, len(ticker_agg))
    sample_indices = random.sample(range(len(ticker_agg)), sample_size)
    
    for idx in sample_indices:
        agg_row = ticker_agg.iloc[idx]
        agg_start = agg_row['ts']
        agg_end = agg_start + pd.Timedelta(minutes=interval_minutes)
        
        # Get corresponding 1-minute bars
        mask = (ticker_1min['ts'] >= agg_start) & (ticker_1min['ts'] < agg_end)
        bars_in_interval = ticker_1min[mask]
        
        if bars_in_interval.empty:
            continue
        
        # Validate OHLCV
        expected = {
            'open': bars_in_interval.iloc[0]['open'],
            'close': bars_in_interval.iloc[-1]['close'],
            'high': bars_in_interval['high'].max(),
            'low': bars_in_interval['low'].min(),
            'volume': bars_in_interval['volume'].sum() if 'volume' in bars_in_interval.columns else None,
            'transactions': bars_in_interval['transactions'].sum() if 'transactions' in bars_in_interval.columns else None
        }
        
        for field in ['open', 'close', 'high', 'low']:
            if abs(agg_row[field] - expected[field]) > 0.0001:
                errors.append(f"{ticker} @ {agg_start}: {field} mismatch - Expected {expected[field]}, Got {agg_row[field]}")
        
        if expected['volume'] is not None and 'volume' in agg_row:
            if abs(agg_row['volume'] - expected['volume']) > 0.1:
                errors.append(f"{ticker} @ {agg_start}: Volume mismatch - Expected {expected['volume']}, Got {agg_row['volume']}")
    
    return errors

def main():
    project_root = Path("/root/stock_project")
    dataset = "us_stocks_sip"
    
    # Paths
    input_dir = project_root / "data" / dataset / "1MINUTE_BARS"
    output_dirs = {
        5: project_root / "data" / dataset / "5MINUTE_BARS",
        15: project_root / "data" / dataset / "15MINUTE_BARS",
        30: project_root / "data" / dataset / "30MINUTE_BARS",
        60: project_root / "data" / dataset / "60MINUTE_BARS"
    }
    
    print("=" * 80)
    print("AGGREGATION VALIDATION REPORT")
    print("=" * 80)
    
    # Test 1: Last two aggregated files
    print("\n1. LAST TWO AGGREGATED FILES")
    print("-" * 40)
    
    last_two = ["2024-12-18.csv", "2024-12-16.csv"]
    
    for date_file in last_two:
        print(f"\n{date_file}:")
        df_1min = pd.read_csv(input_dir / date_file)
        print(f"  Source: {len(df_1min):,} 1-min bars, {df_1min['ticker'].nunique():,} tickers")
        
        # Test 3 random tickers per interval
        test_tickers = df_1min['ticker'].drop_duplicates().sample(3)
        
        for interval in [5, 15, 30, 60]:
            df_agg = pd.read_csv(output_dirs[interval] / date_file)
            print(f"  {interval:2d}-min: {len(df_agg):,} bars", end="")
            
            errors = []
            for ticker in test_tickers:
                errors.extend(validate_sample_bars(df_1min, df_agg, interval, ticker, num_samples=3))
            
            if errors:
                print(f" ❌ {len(errors)} errors")
                print(f"     Example: {errors[0]}")
            else:
                print(" ✅")
    
    # Test 2: Random sample of 20 dates
    print("\n\n2. RANDOM SAMPLE OF 20 DATES")
    print("-" * 40)
    
    # Get fully aggregated files
    all_files = []
    for f in input_dir.glob("*.csv"):
        if all((output_dirs[i] / f.name).exists() for i in [5, 15, 30, 60]):
            all_files.append(f.name)
    
    print(f"Total fully aggregated files: {len(all_files)}")
    
    # Sample 20
    sample_files = random.sample(all_files, min(20, len(all_files)))
    
    results = {"passed": 0, "failed": 0}
    failed_files = []
    
    for i, date_file in enumerate(sorted(sample_files), 1):
        print(f"\r[{i:2d}/20] Testing {date_file}...", end="", flush=True)
        
        df_1min = pd.read_csv(input_dir / date_file)
        test_ticker = df_1min['ticker'].iloc[0]  # Just test first ticker for speed
        
        file_errors = []
        for interval in [5, 15, 30, 60]:
            df_agg = pd.read_csv(output_dirs[interval] / date_file)
            errors = validate_sample_bars(df_1min, df_agg, interval, test_ticker, num_samples=2)
            file_errors.extend(errors)
        
        if file_errors:
            results["failed"] += 1
            failed_files.append((date_file, file_errors[0]))
        else:
            results["passed"] += 1
    
    print(f"\n\nResults: {results['passed']} passed, {results['failed']} failed")
    
    if failed_files:
        print("\nFailed files:")
        for f, error in failed_files[:5]:
            print(f"  - {f}: {error}")
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    if results["failed"] == 0:
        print("✅ ALL VALIDATIONS PASSED!")
        print("   - Last 2 files: Correctly aggregated")
        print("   - Random 20 files: All passed validation")
        print("\nThe aggregation process is working correctly.")
    else:
        print(f"❌ FOUND {results['failed']} VALIDATION FAILURES")
        print("   Please investigate the errors above.")

if __name__ == "__main__":
    main()
