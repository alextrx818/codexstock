#!/usr/bin/env python3
"""Validate 20 random dates for aggregation accuracy"""

import pandas as pd
import numpy as np
from pathlib import Path
import random

def validate_aggregation_for_ticker(df_1min, df_agg, interval, ticker):
    """Validate aggregation for a specific ticker"""
    
    # Filter to ticker
    t1 = df_1min[df_1min['ticker'] == ticker].copy()
    ta = df_agg[df_agg['ticker'] == ticker].copy()
    
    if t1.empty or ta.empty:
        return True, "No data"
    
    # Convert timestamps
    t1['ts'] = pd.to_datetime(t1['window_start'], unit='ns')
    ta['ts'] = pd.to_datetime(ta['window_start'], unit='ns')
    
    # Sample first 3 aggregated bars
    for i in range(min(3, len(ta))):
        agg_bar = ta.iloc[i]
        start_ts = agg_bar['ts']
        end_ts = start_ts + pd.Timedelta(minutes=interval)
        
        # Get 1-min bars in this window
        mask = (t1['ts'] >= start_ts) & (t1['ts'] < end_ts)
        bars = t1[mask]
        
        if bars.empty:
            continue
        
        # Validate
        expected = {
            'open': bars.iloc[0]['open'],
            'close': bars.iloc[-1]['close'],
            'high': bars['high'].max(),
            'low': bars['low'].min(),
            'volume': bars['volume'].sum()
        }
        
        for field in ['open', 'close', 'high', 'low']:
            if abs(agg_bar[field] - expected[field]) > 0.0001:
                return False, f"{field} mismatch at {start_ts}: expected {expected[field]}, got {agg_bar[field]}"
        
        if abs(agg_bar['volume'] - expected['volume']) > 1:
            return False, f"volume mismatch at {start_ts}: expected {expected['volume']}, got {agg_bar['volume']}"
    
    return True, "OK"

def main():
    project_root = Path("/root/stock_project")
    dataset = "us_stocks_sip"
    
    # Get all fully aggregated files
    input_dir = project_root / "data" / dataset / "1MINUTE_BARS"
    all_files = []
    
    for f in input_dir.glob("*.csv"):
        if all((project_root / "data" / dataset / f"{i}MINUTE_BARS" / f.name).exists() 
               for i in [5, 15, 30, 60]):
            all_files.append(f.name)
    
    print("="*80)
    print("VALIDATING 20 RANDOM DATES")
    print("="*80)
    print(f"Total fully aggregated files available: {len(all_files)}")
    
    # Sample 20 random dates
    test_files = sorted(random.sample(all_files, min(20, len(all_files))))
    
    results = []
    
    for i, date_file in enumerate(test_files, 1):
        print(f"\n[{i:2d}/20] {date_file}")
        
        # Read 1-minute data
        df_1min = pd.read_csv(input_dir / date_file)
        print(f"  Source: {len(df_1min):,} rows, {df_1min['ticker'].nunique():,} tickers")
        
        # Pick 2 random tickers to test
        test_tickers = df_1min['ticker'].drop_duplicates().sample(min(2, df_1min['ticker'].nunique()))
        
        file_results = {"passed": 0, "failed": 0, "errors": []}
        
        for interval in [5, 15, 30, 60]:
            df_agg = pd.read_csv(project_root / "data" / dataset / f"{interval}MINUTE_BARS" / date_file)
            
            for ticker in test_tickers:
                passed, msg = validate_aggregation_for_ticker(df_1min, df_agg, interval, ticker)
                
                if passed:
                    file_results["passed"] += 1
                else:
                    file_results["failed"] += 1
                    file_results["errors"].append(f"{interval}-min {ticker}: {msg}")
        
        # Summary for this file
        total_tests = file_results["passed"] + file_results["failed"]
        if file_results["failed"] == 0:
            print(f"  ✅ All {total_tests} tests passed")
            results.append((date_file, True, None))
        else:
            print(f"  ❌ {file_results['failed']}/{total_tests} tests failed")
            print(f"     Example: {file_results['errors'][0]}")
            results.append((date_file, False, file_results['errors'][0]))
    
    # Overall summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    
    passed_files = sum(1 for _, passed, _ in results if passed)
    failed_files = len(results) - passed_files
    
    print(f"Files tested: {len(results)}")
    print(f"Passed: {passed_files}")
    print(f"Failed: {failed_files}")
    
    if failed_files > 0:
        print("\nFailed files:")
        for fname, passed, error in results:
            if not passed:
                print(f"  - {fname}: {error}")
    else:
        print("\n✅ ALL 20 RANDOM FILES PASSED VALIDATION!")
        print("The aggregation process is working correctly across all tested dates.")

if __name__ == "__main__":
    main()
