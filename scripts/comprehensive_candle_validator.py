#!/usr/bin/env python3
"""
Comprehensive validation script that checks every single candle for 50 random days per dataset
Validates that aggregated bars (5, 15, 30, 60 min) match the source 1-minute data
"""

import pandas as pd
import numpy as np
from pathlib import Path
import random
import json
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import logging
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/root/stock_project/candle_validation.log')
    ]
)

class CandleValidator:
    def __init__(self, base_path="/root/stock_project/data"):
        self.base_path = Path(base_path)
        self.tolerance = 1e-8  # Floating point comparison tolerance
        
    def validate_candle(self, agg_candle, one_min_candles, interval):
        """Validate a single aggregated candle against its source 1-minute candles"""
        if len(one_min_candles) == 0:
            return True, "No 1-minute data"
        
        errors = []
        
        # Expected values from 1-minute data
        expected = {
            'open': one_min_candles.iloc[0]['open'],
            'high': one_min_candles['high'].max(),
            'low': one_min_candles['low'].min(),
            'close': one_min_candles.iloc[-1]['close']
        }
        
        # Add volume if present
        if 'volume' in one_min_candles.columns and 'volume' in agg_candle:
            expected['volume'] = one_min_candles['volume'].sum()
            
        # Add transactions if present
        if 'transactions' in one_min_candles.columns and 'transactions' in agg_candle:
            expected['transactions'] = one_min_candles['transactions'].sum()
        
        # Compare each field
        for field, expected_val in expected.items():
            if field not in agg_candle or pd.isna(agg_candle[field]) or pd.isna(expected_val):
                continue
                
            actual_val = agg_candle[field]
            
            # Use absolute tolerance for volume/transactions, relative for prices
            if field in ['volume', 'transactions']:
                if abs(actual_val - expected_val) > self.tolerance:
                    errors.append(f"{field}: {actual_val} != {expected_val}")
            else:
                if abs(actual_val - expected_val) > self.tolerance:
                    errors.append(f"{field}: {actual_val} != {expected_val}")
        
        return len(errors) == 0, errors

    def validate_day_interval(self, dataset, date, interval):
        """Validate all candles for a specific day and interval"""
        results = {
            'dataset': dataset,
            'date': date,
            'interval': interval,
            'total_candles': 0,
            'valid_candles': 0,
            'invalid_candles': 0,
            'sample_errors': [],
            'validation_rate': 0.0
        }
        
        try:
            # Load 1-minute data
            one_min_path = self.base_path / dataset / "1MINUTE_BARS" / f"{date}.csv"
            if not one_min_path.exists():
                results['error'] = f"1-minute file not found: {one_min_path}"
                return results
                
            df_1min = pd.read_csv(one_min_path)
            df_1min['timestamp'] = pd.to_datetime(df_1min['window_start'], unit='ns')
            
            # Load aggregated data
            agg_path = self.base_path / dataset / f"{interval}MINUTE_BARS" / f"{date}.csv"
            if not agg_path.exists():
                results['error'] = f"Aggregated file not found: {agg_path}"
                return results
                
            df_agg = pd.read_csv(agg_path)
            df_agg['timestamp'] = pd.to_datetime(df_agg['window_start'], unit='ns')
            
            results['total_candles'] = len(df_agg)
            
            # Group by ticker for efficient processing
            for ticker in df_agg['ticker'].unique():
                ticker_1min = df_1min[df_1min['ticker'] == ticker].sort_values('timestamp')
                ticker_agg = df_agg[df_agg['ticker'] == ticker].sort_values('timestamp')
                
                # Validate each aggregated candle
                for idx, agg_row in ticker_agg.iterrows():
                    # Find corresponding 1-minute candles
                    start_time = agg_row['timestamp']
                    end_time = start_time + pd.Timedelta(minutes=interval)
                    
                    mask = (ticker_1min['timestamp'] >= start_time) & (ticker_1min['timestamp'] < end_time)
                    one_min_subset = ticker_1min[mask]
                    
                    # Validate the candle
                    is_valid, errors = self.validate_candle(agg_row, one_min_subset, interval)
                    
                    if is_valid:
                        results['valid_candles'] += 1
                    else:
                        results['invalid_candles'] += 1
                        # Store first few errors as samples
                        if len(results['sample_errors']) < 5:
                            results['sample_errors'].append({
                                'ticker': ticker,
                                'timestamp': str(start_time),
                                'errors': errors
                            })
            
            # Calculate validation rate
            if results['total_candles'] > 0:
                results['validation_rate'] = (results['valid_candles'] / results['total_candles']) * 100
                
        except Exception as e:
            results['error'] = str(e)
            logging.error(f"Error validating {dataset}/{date}/{interval}min: {e}")
            
        return results

def validate_dataset_day(args):
    """Worker function to validate all intervals for a single day"""
    dataset, date, base_path = args
    validator = CandleValidator(base_path)
    
    day_results = {
        'dataset': dataset,
        'date': date,
        'intervals': {}
    }
    
    for interval in [5, 15, 30, 60]:
        interval_results = validator.validate_day_interval(dataset, date, interval)
        day_results['intervals'][f"{interval}min"] = interval_results
        
    return day_results

def main():
    base_path = "/root/stock_project/data"
    datasets = ['global_crypto', 'us_stocks_sip', 'us_indices']
    days_per_dataset = 50
    
    overall_results = {}
    
    print("="*80)
    print("COMPREHENSIVE CANDLE VALIDATION")
    print(f"Validating {days_per_dataset} random days per dataset")
    print("="*80)
    
    for dataset in datasets:
        print(f"\n[{dataset.upper()}]")
        
        # Get all available dates
        one_min_dir = Path(base_path) / dataset / "1MINUTE_BARS"
        all_dates = sorted([f.stem for f in one_min_dir.glob("*.csv")])
        
        if not all_dates:
            print(f"  No data files found for {dataset}")
            continue
            
        # Select random days
        num_days = min(days_per_dataset, len(all_dates))
        selected_days = random.sample(all_dates, num_days)
        
        print(f"  Found {len(all_dates)} days total")
        print(f"  Validating {num_days} random days")
        print(f"  Date range: {all_dates[0]} to {all_dates[-1]}")
        
        # Process days in parallel
        dataset_results = []
        args_list = [(dataset, date, base_path) for date in selected_days]
        
        with ProcessPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(validate_dataset_day, args): args for args in args_list}
            
            completed = 0
            for future in as_completed(futures):
                try:
                    result = future.result()
                    dataset_results.append(result)
                    completed += 1
                    
                    # Progress update
                    if completed % 10 == 0 or completed == num_days:
                        print(f"  Progress: {completed}/{num_days} days processed")
                        
                except Exception as e:
                    logging.error(f"Error processing day: {e}")
                    
        overall_results[dataset] = dataset_results
        
        # Print dataset summary
        print(f"\n  Summary for {dataset}:")
        for interval in [5, 15, 30, 60]:
            interval_key = f"{interval}min"
            
            # Aggregate stats across all days
            total_candles = 0
            valid_candles = 0
            invalid_candles = 0
            
            for day_result in dataset_results:
                if interval_key in day_result['intervals']:
                    interval_data = day_result['intervals'][interval_key]
                    total_candles += interval_data.get('total_candles', 0)
                    valid_candles += interval_data.get('valid_candles', 0)
                    invalid_candles += interval_data.get('invalid_candles', 0)
            
            if total_candles > 0:
                validation_rate = (valid_candles / total_candles) * 100
                print(f"    {interval:2d}-minute: {valid_candles:,}/{total_candles:,} candles valid ({validation_rate:.2f}%)")
                
                if invalid_candles > 0:
                    print(f"               {invalid_candles:,} candles failed validation")
                    
                    # Show sample errors
                    sample_errors = []
                    for day_result in dataset_results[:5]:  # Check first 5 days
                        if interval_key in day_result['intervals']:
                            sample_errors.extend(day_result['intervals'][interval_key].get('sample_errors', [])[:2])
                    
                    if sample_errors:
                        print("               Sample errors:")
                        for error in sample_errors[:3]:
                            print(f"                 - {error['ticker']} @ {error['timestamp']}: {error['errors']}")
    
    # Save detailed results
    output_file = Path("/root/stock_project/comprehensive_validation_results.json")
    with open(output_file, 'w') as f:
        json.dump(overall_results, f, indent=2, default=str)
    
    # Print final summary
    print("\n" + "="*80)
    print("FINAL VALIDATION SUMMARY")
    print("="*80)
    
    for dataset in datasets:
        if dataset not in overall_results:
            continue
            
        print(f"\n{dataset}:")
        
        # Calculate overall stats
        grand_total = 0
        grand_valid = 0
        
        for interval in [5, 15, 30, 60]:
            interval_key = f"{interval}min"
            
            total = sum(
                day['intervals'].get(interval_key, {}).get('total_candles', 0)
                for day in overall_results[dataset]
            )
            valid = sum(
                day['intervals'].get(interval_key, {}).get('valid_candles', 0)
                for day in overall_results[dataset]
            )
            
            grand_total += total
            grand_valid += valid
            
            if total > 0:
                rate = (valid / total) * 100
                print(f"  {interval:2d}-min: {rate:6.2f}% ({valid:,}/{total:,})")
        
        if grand_total > 0:
            overall_rate = (grand_valid / grand_total) * 100
            print(f"  TOTAL: {overall_rate:6.2f}% ({grand_valid:,}/{grand_total:,})")
    
    print(f"\nDetailed results saved to: {output_file}")
    print(f"Log file: /root/stock_project/candle_validation.log")

if __name__ == "__main__":
    main()
