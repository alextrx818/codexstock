#!/usr/bin/env python3
"""
Verify that the aggregation from 1-minute bars to 5, 15, 30, and 60-minute bars
was done correctly for the global_crypto dataset.
"""

import pandas as pd
import numpy as np
import os
import random
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

# Data directory
BASE_DIR = "/root/stock_project/data/global_crypto"

def read_csv_with_headers(filepath: str) -> pd.DataFrame:
    """Read CSV file and handle potential header issues."""
    try:
        # Read with headers
        df = pd.read_csv(filepath)
        
        # Rename window_start to timestamp for consistency
        if 'window_start' in df.columns:
            df['timestamp'] = df['window_start']
        
        return df
    except Exception as e:
        print(f"Error reading {filepath}: {str(e)}")
        raise

def parse_timestamp(ts):
    """Parse timestamp handling both milliseconds and nanoseconds."""
    try:
        if isinstance(ts, str):
            ts = int(float(ts))
        else:
            ts = int(ts)
        
        # If timestamp is too large, assume it's in nanoseconds
        if ts > 10**15:
            ts = ts // 1000000  # Convert nanoseconds to milliseconds
        
        return pd.Timestamp(ts, unit='ms', tz='UTC')
    except:
        return None

def aggregate_1min_to_interval(df_1min: pd.DataFrame, interval_minutes: int) -> pd.DataFrame:
    """Aggregate 1-minute data to specified interval."""
    # Parse timestamps
    df_1min['datetime'] = df_1min['timestamp'].apply(parse_timestamp)
    df_1min = df_1min.dropna(subset=['datetime'])
    
    # Sort by datetime
    df_1min = df_1min.sort_values('datetime')
    
    # Create interval groups
    df_1min['interval'] = df_1min['datetime'].dt.floor(f'{interval_minutes}min')
    
    # Aggregate by ticker and interval
    aggregated = []
    
    for ticker in df_1min['ticker'].unique():
        ticker_data = df_1min[df_1min['ticker'] == ticker]
        
        for interval, group in ticker_data.groupby('interval'):
            if len(group) > 0:
                agg_row = {
                    'ticker': ticker,
                    'volume': group['volume'].sum(),
                    'open': group.iloc[0]['open'],
                    'close': group.iloc[-1]['close'],
                    'high': group['high'].max(),
                    'low': group['low'].min(),
                    'window_start': int(interval.timestamp() * 1000000000),  # Convert to nanoseconds
                    'transactions': group['transactions'].sum() if 'transactions' in group.columns else 0
                }
                aggregated.append(agg_row)
    
    return pd.DataFrame(aggregated)

def compare_aggregations(date: str, sample_tickers: List[str], sample_intervals: List[Tuple[datetime, datetime]]) -> Dict:
    """Compare aggregations for a specific date and sample of tickers/intervals."""
    results = {
        '5min': {'matches': 0, 'mismatches': 0, 'errors': []},
        '15min': {'matches': 0, 'mismatches': 0, 'errors': []},
        '30min': {'matches': 0, 'mismatches': 0, 'errors': []},
        '60min': {'matches': 0, 'mismatches': 0, 'errors': []}
    }
    
    # Read 1-minute data
    file_1min = os.path.join(BASE_DIR, "1MINUTE_BARS", f"{date}.csv")
    if not os.path.exists(file_1min):
        return results
    
    try:
        df_1min = read_csv_with_headers(file_1min)
    except Exception as e:
        for interval in results:
            results[interval]['errors'].append(f"Error reading 1-minute file: {str(e)}")
        return results
    
    # Check each interval
    for interval_name, interval_minutes in [('5min', 5), ('15min', 15), ('30min', 30), ('60min', 60)]:
        file_interval = os.path.join(BASE_DIR, f"{interval_minutes}MINUTE_BARS", f"{date}.csv")
        
        if not os.path.exists(file_interval):
            results[interval_name]['errors'].append(f"File not found: {file_interval}")
            continue
        
        try:
            # Read actual aggregated data
            df_actual = read_csv_with_headers(file_interval)
            df_actual['datetime'] = df_actual['timestamp'].apply(parse_timestamp)
            df_actual = df_actual.dropna(subset=['datetime'])
            
            # Calculate expected aggregation from 1-minute data
            df_expected = aggregate_1min_to_interval(df_1min.copy(), interval_minutes)
            df_expected['timestamp'] = df_expected['window_start']
            df_expected['datetime'] = df_expected['timestamp'].apply(parse_timestamp)
            
            # Compare for sample tickers and intervals
            for ticker in sample_tickers:
                # Filter data for this ticker
                actual_ticker = df_actual[df_actual['ticker'] == ticker]
                expected_ticker = df_expected[df_expected['ticker'] == ticker]
                
                if len(actual_ticker) == 0 or len(expected_ticker) == 0:
                    continue
                
                # Sample some time intervals
                for start_time, end_time in sample_intervals:
                    # Find matching rows in actual data
                    actual_interval = actual_ticker[
                        (actual_ticker['datetime'] >= start_time) & 
                        (actual_ticker['datetime'] < end_time)
                    ]
                    
                    # Find matching rows in expected data
                    expected_interval = expected_ticker[
                        (expected_ticker['datetime'] >= start_time) & 
                        (expected_ticker['datetime'] < end_time)
                    ]
                    
                    # Compare each row
                    for _, expected_row in expected_interval.iterrows():
                        # Find corresponding actual row
                        actual_rows = actual_interval[
                            actual_interval['window_start'] == expected_row['window_start']
                        ]
                        
                        if len(actual_rows) == 0:
                            results[interval_name]['mismatches'] += 1
                            results[interval_name]['errors'].append(
                                f"Missing row for {ticker} at {expected_row['datetime']}"
                            )
                            continue
                        
                        actual_row = actual_rows.iloc[0]
                        
                        # Compare OHLCV values
                        tolerance = 0.0001  # Small tolerance for floating point comparison
                        
                        checks = {
                            'open': abs(actual_row['open'] - expected_row['open']) < tolerance,
                            'high': abs(actual_row['high'] - expected_row['high']) < tolerance,
                            'low': abs(actual_row['low'] - expected_row['low']) < tolerance,
                            'close': abs(actual_row['close'] - expected_row['close']) < tolerance,
                            'volume': abs(actual_row['volume'] - expected_row['volume']) < tolerance
                        }
                        
                        if all(checks.values()):
                            results[interval_name]['matches'] += 1
                        else:
                            results[interval_name]['mismatches'] += 1
                            failed_checks = [k for k, v in checks.items() if not v]
                            results[interval_name]['errors'].append(
                                f"Mismatch for {ticker} at {actual_row['datetime']}: "
                                f"Failed checks: {failed_checks}, "
                                f"Expected: O={expected_row['open']:.6f} H={expected_row['high']:.6f} "
                                f"L={expected_row['low']:.6f} C={expected_row['close']:.6f} V={expected_row['volume']:.6f}, "
                                f"Actual: O={actual_row['open']:.6f} H={actual_row['high']:.6f} "
                                f"L={actual_row['low']:.6f} C={actual_row['close']:.6f} V={actual_row['volume']:.6f}"
                            )
        
        except Exception as e:
            results[interval_name]['errors'].append(f"Error processing {interval_name}: {str(e)}")
    
    return results

def main():
    """Main verification function."""
    print("Crypto Aggregation Verification")
    print("=" * 80)
    
    # Get list of all 1-minute files
    files_1min = [f for f in os.listdir(os.path.join(BASE_DIR, "1MINUTE_BARS")) if f.endswith('.csv')]
    
    # Sample 50 random files (or all if less than 50)
    sample_size = min(50, len(files_1min))
    sampled_files = random.sample(files_1min, sample_size)
    
    print(f"Sampling {sample_size} files from {len(files_1min)} total files")
    print()
    
    overall_results = {
        '5min': {'total_matches': 0, 'total_mismatches': 0},
        '15min': {'total_matches': 0, 'total_mismatches': 0},
        '30min': {'total_matches': 0, 'total_mismatches': 0},
        '60min': {'total_matches': 0, 'total_mismatches': 0}
    }
    
    error_summary = []
    
    for i, filename in enumerate(sampled_files[:10]):  # Process first 10 files for now
        date = filename.replace('.csv', '')
        print(f"Processing file {i+1}/10: {date}")
        
        # Read sample of data to get tickers
        try:
            df_sample = read_csv_with_headers(os.path.join(BASE_DIR, "1MINUTE_BARS", filename))
            df_sample['datetime'] = df_sample['timestamp'].apply(parse_timestamp)
            df_sample = df_sample.dropna(subset=['datetime'])
            
            # Sample some tickers
            unique_tickers = df_sample['ticker'].unique()
            sample_tickers = random.sample(list(unique_tickers), min(5, len(unique_tickers)))
            
            # Sample some time intervals
            min_time = df_sample['datetime'].min()
            max_time = df_sample['datetime'].max()
            
            sample_intervals = []
            for _ in range(5):  # Sample 5 time intervals
                start = min_time + timedelta(hours=random.randint(0, 20))
                end = start + timedelta(hours=2)
                if end <= max_time:
                    sample_intervals.append((start, end))
            
            if not sample_intervals:
                sample_intervals = [(min_time, min_time + timedelta(hours=2))]
            
            # Compare aggregations
            results = compare_aggregations(date, sample_tickers, sample_intervals)
            
            # Update overall results
            for interval in results:
                overall_results[interval]['total_matches'] += results[interval]['matches']
                overall_results[interval]['total_mismatches'] += results[interval]['mismatches']
                
                # Collect first few errors for each interval
                if results[interval]['errors'] and len(error_summary) < 20:
                    error_summary.extend([
                        f"{date} - {interval}: {err}" 
                        for err in results[interval]['errors'][:2]
                    ])
            
        except Exception as e:
            print(f"  Error processing {filename}: {str(e)}")
    
    # Print summary
    print("\n" + "=" * 80)
    print("VERIFICATION SUMMARY")
    print("=" * 80)
    
    for interval in ['5min', '15min', '30min', '60min']:
        total = overall_results[interval]['total_matches'] + overall_results[interval]['total_mismatches']
        if total > 0:
            accuracy = (overall_results[interval]['total_matches'] / total) * 100
            print(f"\n{interval} Aggregation:")
            print(f"  Matches: {overall_results[interval]['total_matches']}")
            print(f"  Mismatches: {overall_results[interval]['total_mismatches']}")
            print(f"  Accuracy: {accuracy:.2f}%")
        else:
            print(f"\n{interval} Aggregation: No data to compare")
    
    if error_summary:
        print("\n" + "=" * 80)
        print("SAMPLE ERRORS (first 20):")
        print("=" * 80)
        for error in error_summary[:20]:
            print(f"- {error}")

if __name__ == "__main__":
    main()
