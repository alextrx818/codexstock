#!/usr/bin/env python3
"""
Resample 1-minute bars to 5-minute bars for Polygon.io data
NOTE: This script maintains the same format as the input files.
The same approach will be used for 15, 30, and 60 minute aggregations.
"""

import os
import glob
import pandas as pd
from datetime import datetime

def process_csv_file(input_file, output_file):
    """Process a single CSV file with proper format"""
    try:
        print(f"  Processing: {os.path.basename(input_file)}")
        
        # Read CSV with headers
        df = pd.read_csv(input_file)
        
        # Floor to 5-minute buckets
        df['window_start_5m'] = (
            (df['window_start'] // int(5*60 * 1e9))  # 5*60 seconds → nanoseconds
            * int(5*60 * 1e9)
        )
        
        # Set up aggregation columns
        agg_cols = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last'
        }
        
        # Add optional columns if they exist
        if 'volume' in df.columns:
            agg_cols['volume'] = 'sum'
        if 'transactions' in df.columns:
            agg_cols['transactions'] = 'sum'
        
        # Group by ticker and 5-minute window
        df5 = (
            df
            .groupby(['ticker', 'window_start_5m'], sort=False)
            .agg(agg_cols)
            .reset_index()
        )
        
        # Rename window_start_5m back to window_start to keep same format
        df5.rename(columns={'window_start_5m': 'window_start'}, inplace=True)
        
        # Reorder columns to match original format
        if 'volume' in df.columns and 'transactions' in df.columns:
            # Format for stocks/crypto: ticker,volume,open,close,high,low,window_start,transactions
            df5 = df5[['ticker', 'volume', 'open', 'close', 'high', 'low', 'window_start', 'transactions']]
        elif 'volume' not in df.columns and 'transactions' not in df.columns:
            # Format for indices: ticker,open,close,high,low,window_start
            df5 = df5[['ticker', 'open', 'close', 'high', 'low', 'window_start']]
        
        # Write to output file
        df5.to_csv(output_file, index=False)
        
        print(f"    ✓ Success: {len(df)} rows -> {len(df5)} 5-min bars")
        return True
        
    except Exception as e:
        print(f"    ✗ Error: {str(e)}")
        return False

def main():
    """Main function to process all asset classes"""
    print("5-Minute Bar Resampling Script")
    print("Maintaining same format as input files")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    base_dir = '/root/stock_project/data'
    asset_classes = ['global_crypto', 'us_indices', 'us_stocks_sip']
    
    total_processed = 0
    total_failed = 0
    
    for asset_class in asset_classes:
        print(f"\nProcessing {asset_class}...")
        
        # Create 5MINUTE_BARS if missing
        input_dir = os.path.join(base_dir, asset_class, '1MINUTE_BARS')
        output_dir = os.path.join(base_dir, asset_class, '5MINUTE_BARS')
        os.makedirs(output_dir, exist_ok=True)
        
        # Get all CSV files
        csv_files = sorted(glob.glob(os.path.join(input_dir, '*.csv')))
        
        if not csv_files:
            print(f"  No CSV files found in {input_dir}")
            continue
        
        print(f"  Found {len(csv_files)} files to process")
        
        # Process files one by one in a single loop
        processed = 0
        failed = 0
        
        for csv_file in csv_files:
            output_file = os.path.join(output_dir, os.path.basename(csv_file))
            
            if process_csv_file(csv_file, output_file):
                processed += 1
            else:
                failed += 1
            
            # Progress indicator every 50 files
            if (processed + failed) % 50 == 0:
                print(f"  Progress: {processed + failed}/{len(csv_files)} files...")
        
        print(f"\n  Summary for {asset_class}:")
        print(f"    - Processed successfully: {processed}")
        print(f"    - Failed: {failed}")
        
        total_processed += processed
        total_failed += failed
    
    print("\n" + "="*80)
    print("COMPLETE!")
    print(f"Total files processed: {total_processed}")
    print(f"Total files failed: {total_failed}")
    print(f"Ended at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == '__main__':
    main()
