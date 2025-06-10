import pandas as pd
import numpy as np
import os
import sys
import glob
from pathlib import Path
from datetime import datetime
import random

def verify_aggregation(original_bars, aggregated_bar):
    """Quick sanity check on one aggregated bar"""
    checks = {
        'volume': original_bars['volume'].sum() == aggregated_bar['volume'] if 'volume' in original_bars.columns else True,
        'open': original_bars.iloc[0]['open'] == aggregated_bar['open'],
        'close': original_bars.iloc[-1]['close'] == aggregated_bar['close'],
        'high': original_bars['high'].max() == aggregated_bar['high'],
        'low': original_bars['low'].min() == aggregated_bar['low']
    }
    
    failed_checks = [field for field, passed in checks.items() if not passed]
    return len(failed_checks) == 0, failed_checks

def process_single_file(file_path, file_num, total_files, dataset, verify_only=False):
    """Process a single CSV file and aggregate ALL instruments to 15-min bars"""
    filename = os.path.basename(file_path)
    output_dir = f"/root/stock_project/data/{dataset}/15MINUTE_BARS"
    output_file = f"{output_dir}/{filename}"
    
    print(f"\nProcessing file {file_num} of {total_files}: {filename}")
    
    try:
        # Read CSV once
        df = pd.read_csv(file_path, low_memory=False)
        
        # Detect header
        first_row = df.iloc[0] if len(df) > 0 else None
        has_header = False
        
        if first_row is not None:
            try:
                float(first_row.iloc[0])
            except (ValueError, TypeError):
                has_header = True
                df = pd.read_csv(file_path, header=0, low_memory=False)
        
        # Assign column names if no header
        if not has_header:
            if len(df.columns) == 8:
                df.columns = ['ticker', 'volume', 'open', 'close', 'high', 'low', 'timestamp', 'transactions']
            elif len(df.columns) == 7:
                df.columns = ['ticker', 'open', 'close', 'high', 'low', 'timestamp', 'volume']
            elif len(df.columns) == 6:
                df.columns = ['ticker', 'open', 'close', 'high', 'low', 'timestamp']
            else:
                print(f"❌ Failed: {filename} - unexpected columns ({len(df.columns)})")
                return 0, 0, False
        
        # Handle timestamp column
        timestamp_col = 'timestamp' if 'timestamp' in df.columns else 'window_start'
        if timestamp_col not in df.columns:
            print(f"❌ Failed: {filename} - no timestamp column")
            return 0, 0, False
        
        # Convert timestamp to datetime
        df[timestamp_col] = pd.to_numeric(df[timestamp_col], errors='coerce')
        df = df.dropna(subset=[timestamp_col])
        df['datetime'] = pd.to_datetime(df[timestamp_col], unit='ns')
        
        total_1min_bars = len(df)
        total_15min_bars = 0
        all_aggregated = []
        verification_passed = True
        
        # Get unique tickers
        tickers = df['ticker'].unique()
        
        # Process each ticker
        for ticker in tickers:
            ticker_data = df[df['ticker'] == ticker].copy()
            if ticker_data.empty:
                continue
            
            # Set datetime index
            ticker_data.set_index('datetime', inplace=True)
            ticker_data.sort_index(inplace=True)
            
            # Store original for verification
            ticker_data['timestamp_numeric'] = ticker_data[timestamp_col]
            
            # Aggregation rules
            agg_dict = {
                'open': 'first',
                'close': 'last',
                'high': 'max',
                'low': 'min',
                'timestamp_numeric': 'first'
            }
            
            if 'volume' in ticker_data.columns:
                agg_dict['volume'] = 'sum'
            if 'transactions' in ticker_data.columns:
                agg_dict['transactions'] = 'sum'
            
            # Resample to 15-minute bars
            resampled = ticker_data.resample(pd.Timedelta(minutes=15)).agg(agg_dict)
            resampled = resampled.dropna(subset=['open'])
            
            if not resampled.empty:
                # Add ticker column
                resampled['ticker'] = ticker
                resampled[timestamp_col] = resampled.index.astype('int64')
                
                # Quick verification on one random bar
                if len(resampled) > 0 and verification_passed:
                    # Pick a random bar to verify
                    verify_idx = random.randint(0, len(resampled) - 1)
                    verify_time = resampled.index[verify_idx]
                    
                    # Get the original 1-min bars for this 15-min period
                    period_start = verify_time
                    period_end = verify_time + pd.Timedelta(minutes=15)
                    original = ticker_data[(ticker_data.index >= period_start) & (ticker_data.index < period_end)]
                    
                    if len(original) > 0:
                        passed, failed_fields = verify_aggregation(original, resampled.iloc[verify_idx])
                        if not passed:
                            print(f"  ⚠️ Verification failed for {ticker} - mismatch in {', '.join(failed_fields)}")
                            verification_passed = False
                
                total_15min_bars += len(resampled)
                all_aggregated.append(resampled)
        
        # Combine all tickers and save
        if all_aggregated and not verify_only:
            combined = pd.concat(all_aggregated)
            combined.reset_index(drop=True).to_csv(output_file, header=False, index=False)
        
        if verification_passed:
            print(f"✅ Successful: {filename} - {total_1min_bars} bars → {total_15min_bars} bars ({len(tickers)} tickers)")
        else:
            print(f"❌ Failed: {filename} - verification issues")
        
        return total_1min_bars, total_15min_bars, verification_passed
        
    except Exception as e:
        print(f"❌ Failed: {filename} - {str(e)}")
        return 0, 0, False

def aggregate_all_instruments(dataset, verify_only=False):
    """Main function to aggregate all instruments from 1-min to 15-min bars"""
    
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
    
    print(f"\n{'='*60}")
    print(f"15-Minute Bar Aggregation - ALL INSTRUMENTS")
    print(f"Dataset: {dataset}")
    print(f"Mode: {'VERIFY ONLY (no files saved)' if verify_only else 'FULL PROCESSING'}")
    print(f"Total files to process: {len(csv_files)}")
    print(f"{'='*60}")
    
    # Process counters
    total_files_processed = 0
    total_1min_bars = 0
    total_15min_bars = 0
    successful_files = 0
    
    # Process each file
    for i, csv_file in enumerate(csv_files, 1):
        bars_1min, bars_15min, success = process_single_file(
            csv_file, i, len(csv_files), dataset, verify_only
        )
        
        total_files_processed += 1
        total_1min_bars += bars_1min
        total_15min_bars += bars_15min
        if success:
            successful_files += 1
    
    # Calculate compression ratio
    compression_ratio = total_1min_bars / total_15min_bars if total_15min_bars > 0 else 0
    
    # Final summary
    print(f"\n{'='*60}")
    print(f"AGGREGATION COMPLETE")
    print(f"{'='*60}")
    print(f"✓ Files processed: {total_files_processed}")
    print(f"✓ Successful files: {successful_files}")
    print(f"✓ Failed files: {total_files_processed - successful_files}")
    print(f"✓ Total 1-minute bars: {total_1min_bars:,}")
    print(f"✓ Total 15-minute bars created: {total_15min_bars:,}")
    print(f"✓ Compression ratio: {compression_ratio:.1f}:1")
    
    if verify_only:
        print(f"\n📝 Note: Running in VERIFY mode - no files were saved")
    else:
        print(f"\n💾 Output saved to: {output_dir}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 aggregate_15min_all.py <dataset> [verify]")
        print("Example: python3 aggregate_15min_all.py us_stocks_sip")
        print("Example: python3 aggregate_15min_all.py us_stocks_sip verify")
        sys.exit(1)
    
    dataset = sys.argv[1]
    verify_only = len(sys.argv) > 2 and sys.argv[2] == "verify"
    
    valid_datasets = ['us_stocks_sip', 'us_indices', 'global_crypto']
    if dataset not in valid_datasets:
        print(f"Invalid dataset. Must be one of: {', '.join(valid_datasets)}")
        sys.exit(1)
    
    aggregate_all_instruments(dataset, verify_only)
