#!/usr/bin/env python3
"""
Resample 1-minute bars to 5-minute bars following specific requirements
"""

import os
import glob
import pandas as pd
from datetime import datetime

def process_csv_file(input_file, output_file):
    """Process a single CSV file according to specifications"""
    try:
        print(f"  Processing: {os.path.basename(input_file)}")
        
        # Read CSV with specific date parsing
        df = pd.read_csv(
            input_file,
            parse_dates=['timestamp'],
            date_parser=lambda s: pd.to_datetime(s, format='%Y-%m-%d %H:%M:%S'),
            index_col='timestamp'
        )
        
        # Sort by index
        df = df.sort_index()
        
        # Create complete 5-minute date range
        date_range = pd.date_range(
            start=df.index.min(),
            end=df.index.max(),
            freq='5T'
        )
        
        # Reindex the DataFrame to that 5-minute range
        df_reindexed = df.reindex(date_range)
        
        # Resample with specified aggregation
        df_5min = df_reindexed.resample('5T').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        })
        
        # Remove rows where all values are NaN
        df_5min = df_5min.dropna(how='all')
        
        # Reset index to make timestamp a column again
        df_5min = df_5min.reset_index()
        df_5min.rename(columns={'index': 'timestamp'}, inplace=True)
        
        # Write to output file
        df_5min.to_csv(output_file, index=False)
        
        print(f"    ✓ Success: {len(df)} rows -> {len(df_5min)} 5-min bars")
        return True
        
    except Exception as e:
        print(f"    ✗ Error: {str(e)}")
        return False

def main():
    """Main function to process all asset classes"""
    print("5-Minute Bar Resampling Script")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    base_dir = '/root/stock_project/data'
    asset_classes = ['global_crypto', 'us_indices', 'us_stocks_sip']
    
    total_processed = 0
    total_failed = 0
    
    for asset_class in asset_classes:
        print(f"\nProcessing {asset_class}...")
        
        # Ensure 5MINUTE_BARS subfolder exists
        input_dir = os.path.join(base_dir, asset_class, '1MINUTE_BARS')
        output_dir = os.path.join(base_dir, asset_class, '5MINUTE_BARS')
        os.makedirs(output_dir, exist_ok=True)
        
        # Get all CSV files
        csv_files = sorted(glob.glob(os.path.join(input_dir, '*.csv')))
        
        if not csv_files:
            print(f"  No CSV files found in {input_dir}")
            continue
        
        print(f"  Found {len(csv_files)} files to process")
        
        # Process files one by one
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
