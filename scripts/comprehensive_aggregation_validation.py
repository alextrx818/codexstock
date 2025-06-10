#!/usr/bin/env python3
"""
Comprehensive validation of aggregation accuracy across all intervals.
Tests the last two aggregated files and 20 random dates.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import random
from datetime import datetime

def validate_aggregation(df_1min, df_agg, interval_minutes, date_str, ticker=None):
    """Validate aggregation accuracy for a specific ticker or all tickers"""
    errors = []
    
    # If ticker specified, filter to that ticker
    if ticker:
        df_1min = df_1min[df_1min['ticker'] == ticker].copy()
        df_agg = df_agg[df_agg['ticker'] == ticker].copy()
        
    if df_1min.empty or df_agg.empty:
        return errors
    
    # Convert timestamps
    df_1min['ts'] = pd.to_datetime(df_1min['window_start'], unit='ns')
    df_agg['ts'] = pd.to_datetime(df_agg['window_start'], unit='ns')
    
    # Group by ticker for validation
    for ticker in df_1min['ticker'].unique():
        ticker_1min = df_1min[df_1min['ticker'] == ticker].sort_values('ts')
        ticker_agg = df_agg[df_agg['ticker'] == ticker].sort_values('ts')
        
        if ticker_agg.empty:
            continue
            
        # Validate each aggregated bar
        for idx, agg_row in ticker_agg.iterrows():
            agg_start = agg_row['ts']
            agg_end = agg_start + pd.Timedelta(minutes=interval_minutes)
            
            # Get corresponding 1-minute bars
            mask = (ticker_1min['ts'] >= agg_start) & (ticker_1min['ts'] < agg_end)
            bars_in_interval = ticker_1min[mask]
            
            if bars_in_interval.empty:
                continue
            
            # Validate OHLCV
            expected_open = bars_in_interval.iloc[0]['open']
            expected_close = bars_in_interval.iloc[-1]['close']
            expected_high = bars_in_interval['high'].max()
            expected_low = bars_in_interval['low'].min()
            
            if abs(agg_row['open'] - expected_open) > 0.0001:
                errors.append(f"{date_str} {ticker} {agg_start}: Open mismatch - Expected {expected_open}, Got {agg_row['open']}")
            if abs(agg_row['close'] - expected_close) > 0.0001:
                errors.append(f"{date_str} {ticker} {agg_start}: Close mismatch - Expected {expected_close}, Got {agg_row['close']}")
            if abs(agg_row['high'] - expected_high) > 0.0001:
                errors.append(f"{date_str} {ticker} {agg_start}: High mismatch - Expected {expected_high}, Got {agg_row['high']}")
            if abs(agg_row['low'] - expected_low) > 0.0001:
                errors.append(f"{date_str} {ticker} {agg_start}: Low mismatch - Expected {expected_low}, Got {agg_row['low']}")
            
            # Validate volume if present
            if 'volume' in bars_in_interval.columns and 'volume' in agg_row:
                expected_volume = bars_in_interval['volume'].sum()
                if abs(agg_row['volume'] - expected_volume) > 0.1:
                    errors.append(f"{date_str} {ticker} {agg_start}: Volume mismatch - Expected {expected_volume}, Got {agg_row['volume']}")
            
            # Validate transactions if present
            if 'transactions' in bars_in_interval.columns and 'transactions' in agg_row:
                expected_transactions = bars_in_interval['transactions'].sum()
                if abs(agg_row['transactions'] - expected_transactions) > 0.1:
                    errors.append(f"{date_str} {ticker} {agg_start}: Transactions mismatch - Expected {expected_transactions}, Got {agg_row['transactions']}")
    
    return errors

def validate_file_format(df_1min, df_agg, interval):
    """Validate that aggregated file has correct format"""
    format_issues = []
    
    # Check columns
    expected_cols = ['window_start', 'ticker', 'open', 'high', 'low', 'close', 'volume', 'transactions']
    for col in expected_cols:
        if col not in df_agg.columns:
            format_issues.append(f"{interval}-min missing column: {col}")
    
    # Check data types
    if df_agg['window_start'].dtype != np.int64:
        format_issues.append(f"{interval}-min window_start should be int64, got {df_agg['window_start'].dtype}")
    
    # Check timestamp alignment
    df_agg['ts'] = pd.to_datetime(df_agg['window_start'], unit='ns')
    for idx, row in df_agg.iterrows():
        if row['ts'].minute % interval != 0:
            format_issues.append(f"{interval}-min has misaligned timestamp: {row['ts']}")
            break
    
    return format_issues

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
    print("COMPREHENSIVE AGGREGATION VALIDATION")
    print("=" * 80)
    
    # Test 1: Validate last two aggregated files
    print("\n1. VALIDATING LAST TWO AGGREGATED FILES")
    print("-" * 40)
    
    last_two_files = ["2024-12-18.csv", "2024-12-16.csv"]
    
    for date_file in last_two_files:
        print(f"\nValidating {date_file}:")
        
        # Read 1-minute data
        df_1min = pd.read_csv(input_dir / date_file)
        print(f"  1-minute bars: {len(df_1min)} rows, {df_1min['ticker'].nunique()} tickers")
        
        all_errors = []
        
        for interval in [5, 15, 30, 60]:
            df_agg = pd.read_csv(output_dirs[interval] / date_file)
            print(f"  {interval}-minute bars: {len(df_agg)} rows")
            
            # Format validation
            format_issues = validate_file_format(df_1min, df_agg, interval)
            if format_issues:
                all_errors.extend(format_issues)
            
            # Pick 5 random tickers for detailed validation
            random_tickers = df_1min['ticker'].drop_duplicates().sample(min(5, df_1min['ticker'].nunique()))
            
            for ticker in random_tickers:
                errors = validate_aggregation(df_1min, df_agg, interval, date_file, ticker)
                all_errors.extend(errors)
        
        if all_errors:
            print(f"  ❌ ERRORS FOUND:")
            for error in all_errors[:5]:  # Show first 5 errors
                print(f"    - {error}")
            if len(all_errors) > 5:
                print(f"    ... and {len(all_errors) - 5} more errors")
        else:
            print(f"  ✅ All validations passed!")
    
    # Test 2: Validate 20 random dates
    print("\n\n2. VALIDATING 20 RANDOM DATES")
    print("-" * 40)
    
    # Get all available dates
    all_files = sorted([f.name for f in input_dir.glob("*.csv")])
    
    # Filter to only files that have been aggregated (exist in all interval directories)
    aggregated_files = []
    for f in all_files:
        if all((output_dirs[interval] / f).exists() for interval in [5, 15, 30, 60]):
            aggregated_files.append(f)
    
    print(f"Total aggregated files available: {len(aggregated_files)}")
    
    # Sample 20 random dates
    random_dates = random.sample(aggregated_files, min(20, len(aggregated_files)))
    
    summary = {"passed": 0, "failed": 0}
    
    for i, date_file in enumerate(sorted(random_dates), 1):
        print(f"\n[{i}/20] Testing {date_file}:", end=" ")
        
        # Read 1-minute data
        df_1min = pd.read_csv(input_dir / date_file)
        
        all_errors = []
        
        for interval in [5, 15, 30, 60]:
            df_agg = pd.read_csv(output_dirs[interval] / date_file)
            
            # Format validation
            format_issues = validate_file_format(df_1min, df_agg, interval)
            all_errors.extend(format_issues)
            
            # Sample 3 random tickers for validation
            if df_1min['ticker'].nunique() > 0:
                sample_size = min(3, df_1min['ticker'].nunique())
                random_tickers = df_1min['ticker'].drop_duplicates().sample(sample_size)
                
                for ticker in random_tickers:
                    errors = validate_aggregation(df_1min, df_agg, interval, date_file, ticker)
                    all_errors.extend(errors)
        
        if all_errors:
            print(f"❌ FAILED ({len(all_errors)} errors)")
            summary["failed"] += 1
            # Show first error as example
            print(f"    Example: {all_errors[0]}")
        else:
            print("✅ PASSED")
            summary["passed"] += 1
    
    # Final summary
    print("\n" + "=" * 80)
    print("VALIDATION SUMMARY")
    print("=" * 80)
    print(f"Last 2 files: Detailed validation completed")
    print(f"Random sample: {summary['passed']} passed, {summary['failed']} failed out of {len(random_dates)} files")
    
    if summary["failed"] == 0:
        print("\n✅ ALL VALIDATIONS PASSED! Aggregation is working correctly.")
    else:
        print(f"\n❌ VALIDATION FAILURES DETECTED! {summary['failed']} files have issues.")

if __name__ == "__main__":
    main()
