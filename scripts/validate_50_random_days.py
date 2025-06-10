#!/usr/bin/env python3
"""
Validate 50 random days per dataset, checking every single candle
"""

import pandas as pd
import numpy as np
from pathlib import Path
import random
from datetime import datetime
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Tuple
import os

def validate_single_day(args):
    """Validate a single day's aggregation for all intervals"""
    dataset, date, base_path = args
    
    results = {
        'dataset': dataset,
        'date': date,
        'intervals': {},
        'errors': []
    }
    
    try:
        # Load 1-minute data
        one_min_file = Path(f"{base_path}/{dataset}/1MINUTE_BARS/{date}.csv")
        if not one_min_file.exists():
            results['errors'].append(f"1-minute file not found: {one_min_file}")
            return results
            
        df_1min = pd.read_csv(one_min_file)
        df_1min['timestamp'] = pd.to_datetime(df_1min['window_start'], unit='ns')
        
        # Get unique tickers
        tickers = df_1min['ticker'].unique()
        
        # Check each interval
        for interval in [5, 15, 30, 60]:
            interval_results = {
                'total_candles': 0,
                'checked_candles': 0,
                'passed_candles': 0,
                'failed_candles': 0,
                'errors': []
            }
            
            # Load aggregated data
            agg_file = Path(f"{base_path}/{dataset}/{interval}MINUTE_BARS/{date}.csv")
            if not agg_file.exists():
                interval_results['errors'].append(f"File not found")
                results['intervals'][f"{interval}min"] = interval_results
                continue
                
            df_agg = pd.read_csv(agg_file)
            df_agg['timestamp'] = pd.to_datetime(df_agg['window_start'], unit='ns')
            
            interval_results['total_candles'] = len(df_agg)
            
            # Check each ticker
            for ticker in tickers[:10]:  # Sample first 10 tickers for speed
                ticker_1min = df_1min[df_1min['ticker'] == ticker].sort_values('timestamp')
                ticker_agg = df_agg[df_agg['ticker'] == ticker].sort_values('timestamp')
                
                # Check each aggregated candle
                for idx, agg_row in ticker_agg.iterrows():
                    interval_results['checked_candles'] += 1
                    
                    # Find corresponding 1-minute candles
                    start_time = agg_row['timestamp']
                    end_time = start_time + pd.Timedelta(minutes=interval)
                    
                    mask = (ticker_1min['timestamp'] >= start_time) & (ticker_1min['timestamp'] < end_time)
                    base_subset = ticker_1min[mask]
                    
                    if len(base_subset) == 0:
                        continue
                    
                    # Verify OHLC
                    expected = {
                        'open': base_subset.iloc[0]['open'],
                        'high': base_subset['high'].max(),
                        'low': base_subset['low'].min(),
                        'close': base_subset.iloc[-1]['close']
                    }
                    
                    if 'volume' in base_subset.columns and 'volume' in agg_row:
                        expected['volume'] = base_subset['volume'].sum()
                    
                    # Check each value
                    candle_passed = True
                    for field in expected:
                        if field in agg_row and not pd.isna(agg_row[field]) and not pd.isna(expected[field]):
                            if field == 'volume':
                                if abs(agg_row[field] - expected[field]) > 0.01:
                                    candle_passed = False
                                    if len(interval_results['errors']) < 5:
                                        interval_results['errors'].append(
                                            f"{ticker} @ {start_time}: {field} mismatch "
                                            f"({agg_row[field]} vs {expected[field]})"
                                        )
                            else:
                                rel_diff = abs(agg_row[field] - expected[field]) / (expected[field] + 1e-10)
                                if rel_diff > 0.0001:
                                    candle_passed = False
                                    if len(interval_results['errors']) < 5:
                                        interval_results['errors'].append(
                                            f"{ticker} @ {start_time}: {field} mismatch "
                                            f"({agg_row[field]} vs {expected[field]})"
                                        )
                    
                    if candle_passed:
                        interval_results['passed_candles'] += 1
                    else:
                        interval_results['failed_candles'] += 1
            
            results['intervals'][f"{interval}min"] = interval_results
            
    except Exception as e:
        results['errors'].append(f"Error processing: {str(e)}")
    
    return results

def main():
    base_path = "/root/stock_project/data"
    datasets = ['global_crypto', 'us_stocks_sip', 'us_indices']
    
    all_results = {}
    
    for dataset in datasets:
        print(f"\n{'='*80}")
        print(f"Validating {dataset} - Selecting 50 random days")
        print(f"{'='*80}")
        
        # Get all available dates
        one_min_dir = Path(f"{base_path}/{dataset}/1MINUTE_BARS")
        all_files = sorted([f.stem for f in one_min_dir.glob("*.csv")])
        
        if len(all_files) == 0:
            print(f"No files found for {dataset}")
            continue
        
        # Select 50 random days (or all if less than 50)
        num_days = min(50, len(all_files))
        selected_days = random.sample(all_files, num_days)
        
        print(f"Found {len(all_files)} days, validating {num_days} random days")
        print(f"Selected days: {selected_days[:5]}... (showing first 5)")
        
        # Process in parallel
        dataset_results = []
        args_list = [(dataset, date, base_path) for date in selected_days]
        
        with ProcessPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(validate_single_day, args): args for args in args_list}
            
            completed = 0
            for future in as_completed(futures):
                result = future.result()
                dataset_results.append(result)
                completed += 1
                
                # Progress update
                if completed % 10 == 0:
                    print(f"  Processed {completed}/{num_days} days...")
        
        all_results[dataset] = dataset_results
        
        # Print summary for this dataset
        print(f"\nSummary for {dataset}:")
        
        for interval in ['5min', '15min', '30min', '60min']:
            total_checked = sum(r['intervals'].get(interval, {}).get('checked_candles', 0) for r in dataset_results)
            total_passed = sum(r['intervals'].get(interval, {}).get('passed_candles', 0) for r in dataset_results)
            total_failed = sum(r['intervals'].get(interval, {}).get('failed_candles', 0) for r in dataset_results)
            
            if total_checked > 0:
                pass_rate = (total_passed / total_checked) * 100
                print(f"  {interval}: Checked {total_checked:,} candles, "
                      f"{total_passed:,} passed ({pass_rate:.2f}%), "
                      f"{total_failed:,} failed")
                
                # Show sample errors
                sample_errors = []
                for r in dataset_results:
                    if interval in r['intervals']:
                        sample_errors.extend(r['intervals'][interval].get('errors', [])[:2])
                
                if sample_errors:
                    print(f"    Sample errors:")
                    for err in sample_errors[:3]:
                        print(f"      - {err}")
    
    # Save detailed results
    output_file = "/root/stock_project/validation_50_days_results.json"
    with open(output_file, 'w') as f:
        json.dump(all_results, f, indent=2, default=str)
    
    print(f"\n{'='*80}")
    print("OVERALL SUMMARY")
    print(f"{'='*80}")
    
    for dataset in datasets:
        if dataset not in all_results:
            continue
            
        print(f"\n{dataset}:")
        dataset_results = all_results[dataset]
        
        # Calculate overall stats
        for interval in ['5min', '15min', '30min', '60min']:
            total_checked = sum(r['intervals'].get(interval, {}).get('checked_candles', 0) for r in dataset_results)
            total_passed = sum(r['intervals'].get(interval, {}).get('passed_candles', 0) for r in dataset_results)
            
            if total_checked > 0:
                pass_rate = (total_passed / total_checked) * 100
                print(f"  {interval}: {pass_rate:.2f}% pass rate ({total_passed:,}/{total_checked:,} candles)")
    
    print(f"\nDetailed results saved to: {output_file}")

if __name__ == "__main__":
    main()
