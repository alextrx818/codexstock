import pandas as pd
import numpy as np
import os
import sys
import glob
from pathlib import Path
from datetime import datetime
import time

def process_file_fast(file_path, file_num, total_files, dataset, output_dir):
    """Process a single file efficiently - read once, aggregate all tickers"""
    filename = os.path.basename(file_path)
    output_file = f"{output_dir}/{filename}"
    
    start_time = time.time()
    print(f"\n[{file_num}/{total_files}] Processing: {filename}", end='', flush=True)
    
    try:
        # Read CSV with optimized settings
        df = pd.read_csv(file_path, low_memory=False, engine='c')
        
        # Quick header check
        if not pd.api.types.is_numeric_dtype(df.iloc[:, 0]):
            df = pd.read_csv(file_path, header=0, low_memory=False, engine='c')
        
        # Identify timestamp column
        timestamp_col = 'timestamp' if 'timestamp' in df.columns else 'window_start'
        
        # Convert timestamp efficiently
        df['datetime'] = pd.to_datetime(pd.to_numeric(df[timestamp_col], errors='coerce'), unit='ns')
        df = df.dropna(subset=['datetime'])
        
        if df.empty:
            print(" ⚠️ No valid data")
            return 0, 0
        
        # Count unique tickers
        n_tickers = df['ticker'].nunique()
        n_1min_bars = len(df)
        
        print(f" ({n_tickers} tickers, {n_1min_bars:,} bars)", end='', flush=True)
        
        # Process all tickers at once using groupby
        df.set_index('datetime', inplace=True)
        
        # Define aggregation rules
        agg_dict = {
            'open': 'first',
            'close': 'last',
            'high': 'max',
            'low': 'min'
        }
        
        # Add optional columns
        if 'volume' in df.columns:
            agg_dict['volume'] = 'sum'
        if 'transactions' in df.columns:
            agg_dict['transactions'] = 'sum'
        
        # Keep timestamp for output
        df['timestamp_numeric'] = df[timestamp_col]
        agg_dict['timestamp_numeric'] = 'first'
        
        # Aggregate all tickers at once
        aggregated = []
        for ticker, group in df.groupby('ticker'):
            resampled = group.resample('15min').agg(agg_dict)
            resampled = resampled.dropna(subset=['open'])
            if not resampled.empty:
                resampled['ticker'] = ticker
                aggregated.append(resampled)
        
        if aggregated:
            # Combine all results
            result = pd.concat(aggregated)
            n_15min_bars = len(result)
            
            # Prepare output columns
            result[timestamp_col] = result.index.astype('int64')
            result = result.reset_index(drop=True)
            
            # Reorder columns for output
            col_order = ['ticker', 'volume', 'open', 'close', 'high', 'low', timestamp_col]
            if 'transactions' in result.columns:
                col_order.append('transactions')
            col_order = [c for c in col_order if c in result.columns]
            result = result[col_order]
            
            # Save to file
            result.to_csv(output_file, header=False, index=False)
            
            elapsed = time.time() - start_time
            print(f" ✓ {n_15min_bars} bars in {elapsed:.1f}s")
            return n_1min_bars, n_15min_bars
        else:
            print(" ⚠️ No bars created")
            return n_1min_bars, 0
            
    except Exception as e:
        print(f" ❌ Error: {str(e)}")
        return 0, 0

def aggregate_fast(dataset):
    """Fast aggregation for all instruments"""
    
    # Setup paths
    input_dir = f"/root/stock_project/data/{dataset}/1MINUTE_BARS"
    output_dir = f"/root/stock_project/data/{dataset}/15MINUTE_BARS"
    
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Get all CSV files
    csv_files = sorted(glob.glob(f"{input_dir}/*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {input_dir}")
        return
    
    print(f"\n{'='*70}")
    print(f"FAST 15-Minute Bar Aggregation - ALL INSTRUMENTS")
    print(f"Dataset: {dataset}")
    print(f"Files to process: {len(csv_files)}")
    print(f"{'='*70}")
    
    # Process counters
    total_start = time.time()
    total_1min_bars = 0
    total_15min_bars = 0
    successful_files = 0
    
    # Process each file
    for i, csv_file in enumerate(csv_files, 1):
        bars_1min, bars_15min = process_file_fast(
            csv_file, i, len(csv_files), dataset, output_dir
        )
        
        total_1min_bars += bars_1min
        total_15min_bars += bars_15min
        if bars_15min > 0:
            successful_files += 1
    
    # Final summary
    total_elapsed = time.time() - total_start
    compression_ratio = total_1min_bars / total_15min_bars if total_15min_bars > 0 else 0
    
    print(f"\n{'='*70}")
    print(f"AGGREGATION COMPLETE")
    print(f"{'='*70}")
    print(f"✓ Time elapsed: {total_elapsed:.1f} seconds")
    print(f"✓ Files processed: {len(csv_files)}")
    print(f"✓ Successful files: {successful_files}")
    print(f"✓ Total 1-minute bars: {total_1min_bars:,}")
    print(f"✓ Total 15-minute bars: {total_15min_bars:,}")
    print(f"✓ Compression ratio: {compression_ratio:.1f}:1")
    print(f"✓ Output location: {output_dir}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 aggregate_15min_fast.py <dataset>")
        print("Example: python3 aggregate_15min_fast.py us_stocks_sip")
        sys.exit(1)
    
    dataset = sys.argv[1]
    
    valid_datasets = ['us_stocks_sip', 'us_indices', 'global_crypto']
    if dataset not in valid_datasets:
        print(f"Invalid dataset. Must be one of: {', '.join(valid_datasets)}")
        sys.exit(1)
    
    aggregate_fast(dataset)
