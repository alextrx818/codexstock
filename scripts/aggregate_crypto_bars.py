#!/usr/bin/env python3
"""
Aggregate 1-minute crypto bars to 5, 15, 30, and 60-minute bars.
Maintains the exact format of the source 1-minute data.
"""

import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime
from pathlib import Path

# Data directory
BASE_DIR = "/root/stock_project/data/global_crypto"

def aggregate_bars(df_1min, interval_minutes):
    """
    Aggregate 1-minute bars to specified interval.
    Maintains exact column order and data types from source.
    """
    # Convert window_start to datetime for grouping
    df_1min['datetime'] = pd.to_datetime(df_1min['window_start'], unit='ns', utc=True)
    
    # Create interval groups
    df_1min['interval'] = df_1min['datetime'].dt.floor(f'{interval_minutes}min')
    
    # Aggregate by ticker and interval
    aggregated = []
    
    for ticker in df_1min['ticker'].unique():
        ticker_data = df_1min[df_1min['ticker'] == ticker].copy()
        
        for interval, group in ticker_data.groupby('interval'):
            if len(group) > 0:
                # Sort by window_start to ensure correct open/close
                group = group.sort_values('window_start')
                
                agg_row = {
                    'ticker': ticker,
                    'volume': group['volume'].sum(),
                    'open': group.iloc[0]['open'],
                    'close': group.iloc[-1]['close'],
                    'high': group['high'].max(),
                    'low': group['low'].min(),
                    'window_start': int(interval.timestamp() * 1000000000),  # Convert to nanoseconds
                    'transactions': group['transactions'].sum()
                }
                aggregated.append(agg_row)
    
    # Create DataFrame with exact column order
    result_df = pd.DataFrame(aggregated, columns=[
        'ticker', 'volume', 'open', 'close', 'high', 'low', 'window_start', 'transactions'
    ])
    
    # Sort by ticker and window_start
    result_df = result_df.sort_values(['ticker', 'window_start'])
    
    return result_df

def process_file(input_file, date_str):
    """Process a single date file."""
    print(f"Processing {date_str}...")
    
    try:
        # Read 1-minute data
        df_1min = pd.read_csv(input_file)
        
        # Process each interval
        for interval in [5, 15, 30, 60]:
            # Create output directory if it doesn't exist
            output_dir = os.path.join(BASE_DIR, f"{interval}MINUTE_BARS")
            os.makedirs(output_dir, exist_ok=True)
            
            # Aggregate data
            df_agg = aggregate_bars(df_1min, interval)
            
            # Write to file
            output_file = os.path.join(output_dir, f"{date_str}.csv")
            df_agg.to_csv(output_file, index=False)
            
            print(f"  Created {interval}-minute bars: {len(df_agg)} rows")
    
    except Exception as e:
        print(f"  Error processing {date_str}: {str(e)}")
        return False
    
    return True

def main():
    """Main processing function."""
    print("Crypto Bar Aggregation Script")
    print("=" * 60)
    print(f"Source: {BASE_DIR}/1MINUTE_BARS")
    print(f"Output: {BASE_DIR}/[5,15,30,60]MINUTE_BARS")
    print("=" * 60)
    
    # Get list of all 1-minute files
    input_dir = os.path.join(BASE_DIR, "1MINUTE_BARS")
    files = sorted([f for f in os.listdir(input_dir) if f.endswith('.csv')])
    
    print(f"\nFound {len(files)} files to process")
    
    # Process each file
    success_count = 0
    error_count = 0
    
    for i, filename in enumerate(files):
        date_str = filename.replace('.csv', '')
        input_file = os.path.join(input_dir, filename)
        
        if i % 50 == 0:
            print(f"\nProgress: {i}/{len(files)} files processed...")
        
        if process_file(input_file, date_str):
            success_count += 1
        else:
            error_count += 1
    
    print("\n" + "=" * 60)
    print("AGGREGATION COMPLETE")
    print("=" * 60)
    print(f"Successfully processed: {success_count} files")
    print(f"Errors: {error_count} files")
    
    # Verify output directories
    print("\nOutput verification:")
    for interval in [5, 15, 30, 60]:
        output_dir = os.path.join(BASE_DIR, f"{interval}MINUTE_BARS")
        if os.path.exists(output_dir):
            file_count = len([f for f in os.listdir(output_dir) if f.endswith('.csv')])
            print(f"  {interval}-minute bars: {file_count} files")

if __name__ == "__main__":
    main()
