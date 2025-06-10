#!/usr/bin/env python3
"""
Fast verification of crypto aggregation from 1-minute to 5, 15, 30, and 60-minute bars.
This version samples specific rows instead of processing entire files.
"""

import pandas as pd
import numpy as np
import os
import random
from datetime import datetime
import sys

# Data directory
BASE_DIR = "/root/stock_project/data/global_crypto"

def parse_timestamp(ts):
    """Parse timestamp from nanoseconds to datetime."""
    if isinstance(ts, str):
        ts = int(float(ts))
    else:
        ts = int(ts)
    
    # Convert nanoseconds to milliseconds
    if ts > 10**15:
        ts = ts // 1000000
    
    return pd.Timestamp(ts, unit='ms', tz='UTC')

def verify_single_aggregation(date: str, ticker: str, start_time_ns: int, interval_minutes: int):
    """Verify a single aggregation point."""
    # Read only the needed rows from 1-minute data
    file_1min = os.path.join(BASE_DIR, "1MINUTE_BARS", f"{date}.csv")
    
    # Calculate the time range for this interval
    start_dt = parse_timestamp(start_time_ns)
    end_time_ns = start_time_ns + (interval_minutes * 60 * 1000000000)
    
    # Read 1-minute data for this ticker and time range
    df_1min = pd.read_csv(file_1min)
    df_1min = df_1min[
        (df_1min['ticker'] == ticker) & 
        (df_1min['window_start'] >= start_time_ns) & 
        (df_1min['window_start'] < end_time_ns)
    ]
    
    if len(df_1min) == 0:
        return None, "No 1-minute data found"
    
    # Calculate expected aggregation
    expected = {
        'open': df_1min.iloc[0]['open'],
        'high': df_1min['high'].max(),
        'low': df_1min['low'].min(),
        'close': df_1min.iloc[-1]['close'],
        'volume': df_1min['volume'].sum(),
        'transactions': df_1min['transactions'].sum() if 'transactions' in df_1min.columns else 0
    }
    
    # Read the aggregated data
    file_interval = os.path.join(BASE_DIR, f"{interval_minutes}MINUTE_BARS", f"{date}.csv")
    df_actual = pd.read_csv(file_interval)
    actual_row = df_actual[
        (df_actual['ticker'] == ticker) & 
        (df_actual['window_start'] == start_time_ns)
    ]
    
    if len(actual_row) == 0:
        return expected, "No aggregated data found"
    
    actual = actual_row.iloc[0]
    
    # Compare values
    tolerance = 0.0001
    errors = []
    
    if abs(actual['open'] - expected['open']) > tolerance:
        errors.append(f"Open: expected {expected['open']:.6f}, got {actual['open']:.6f}")
    if abs(actual['high'] - expected['high']) > tolerance:
        errors.append(f"High: expected {expected['high']:.6f}, got {actual['high']:.6f}")
    if abs(actual['low'] - expected['low']) > tolerance:
        errors.append(f"Low: expected {expected['low']:.6f}, got {actual['low']:.6f}")
    if abs(actual['close'] - expected['close']) > tolerance:
        errors.append(f"Close: expected {expected['close']:.6f}, got {actual['close']:.6f}")
    if abs(actual['volume'] - expected['volume']) > tolerance:
        errors.append(f"Volume: expected {expected['volume']:.6f}, got {actual['volume']:.6f}")
    
    return expected, errors if errors else "OK"

def main():
    """Main verification function."""
    print("Fast Crypto Aggregation Verification")
    print("=" * 80)
    
    # Get list of available dates
    dates = [f.replace('.csv', '') for f in os.listdir(os.path.join(BASE_DIR, "1MINUTE_BARS")) if f.endswith('.csv')]
    dates.sort()
    
    # Sample dates
    sample_dates = random.sample(dates, min(50, len(dates)))
    
    print(f"Checking {len(sample_dates)} random dates from {len(dates)} total")
    print()
    
    results = {
        5: {'correct': 0, 'errors': 0, 'samples': []},
        15: {'correct': 0, 'errors': 0, 'samples': []},
        30: {'correct': 0, 'errors': 0, 'samples': []},
        60: {'correct': 0, 'errors': 0, 'samples': []},
    }
    
    for i, date in enumerate(sample_dates):
        if i % 10 == 0:
            print(f"Progress: {i}/{len(sample_dates)} dates checked...")
        
        try:
            # Read a sample of tickers from this date
            file_1min = os.path.join(BASE_DIR, "1MINUTE_BARS", f"{date}.csv")
            df_sample = pd.read_csv(file_1min, nrows=1000)  # Read first 1000 rows for speed
            
            # Get unique tickers
            tickers = df_sample['ticker'].unique()
            if len(tickers) == 0:
                continue
            
            # Sample 1-2 tickers
            sample_tickers = random.sample(list(tickers), min(2, len(tickers)))
            
            for ticker in sample_tickers:
                ticker_data = df_sample[df_sample['ticker'] == ticker]
                if len(ticker_data) == 0:
                    continue
                
                # Sample a random time for each interval
                for interval in [5, 15, 30, 60]:
                    # Find a time that aligns with the interval
                    sample_row = ticker_data.sample(1).iloc[0]
                    ts_ns = sample_row['window_start']
                    
                    # Align to interval boundary
                    ts_ms = ts_ns // 1000000
                    interval_ms = interval * 60 * 1000
                    aligned_ms = (ts_ms // interval_ms) * interval_ms
                    aligned_ns = aligned_ms * 1000000
                    
                    # Verify this aggregation
                    expected, result = verify_single_aggregation(date, ticker, aligned_ns, interval)
                    
                    if result == "OK":
                        results[interval]['correct'] += 1
                    else:
                        results[interval]['errors'] += 1
                        if len(results[interval]['samples']) < 5:  # Keep first 5 error samples
                            results[interval]['samples'].append({
                                'date': date,
                                'ticker': ticker,
                                'time': parse_timestamp(aligned_ns).strftime('%Y-%m-%d %H:%M'),
                                'error': result
                            })
        
        except Exception as e:
            if i < 5:  # Only print first few errors
                print(f"Error processing {date}: {str(e)}")
    
    # Print results
    print("\n" + "=" * 80)
    print("VERIFICATION RESULTS")
    print("=" * 80)
    
    for interval in [5, 15, 30, 60]:
        total = results[interval]['correct'] + results[interval]['errors']
        if total > 0:
            accuracy = (results[interval]['correct'] / total) * 100
            print(f"\n{interval}-minute aggregation:")
            print(f"  Correct: {results[interval]['correct']}")
            print(f"  Errors: {results[interval]['errors']}")
            print(f"  Accuracy: {accuracy:.1f}%")
            
            if results[interval]['samples']:
                print(f"  Sample errors:")
                for sample in results[interval]['samples'][:3]:
                    print(f"    - {sample['date']} {sample['ticker']} @ {sample['time']}: {sample['error']}")
        else:
            print(f"\n{interval}-minute aggregation: No data checked")
    
    print("\nNote: This is a sampling verification. Full verification would take much longer.")

if __name__ == "__main__":
    main()
