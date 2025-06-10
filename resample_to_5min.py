#!/usr/bin/env python3
"""
Resample 1-minute bars to 5-minute bars for all asset classes
"""

import os
import glob
import pandas as pd
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

def resample_file(input_file, output_file):
    """Resample a single 1-minute bar file to 5-minute bars"""
    try:
        # Read the CSV file
        df = pd.read_csv(input_file)
        
        # Convert timestamp column to datetime if it exists
        timestamp_col = None
        for col in ['timestamp', 'time', 't', 'datetime']:
            if col in df.columns:
                timestamp_col = col
                break
        
        if timestamp_col:
            df[timestamp_col] = pd.to_datetime(df[timestamp_col])
            df.set_index(timestamp_col, inplace=True)
        else:
            # If no timestamp column, assume the first column is the timestamp
            df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0])
            df.set_index(df.columns[0], inplace=True)
        
        # Identify OHLCV columns
        ohlcv_mapping = {}
        
        # Common column name patterns
        patterns = {
            'open': ['open', 'o', 'Open'],
            'high': ['high', 'h', 'High'],
            'low': ['low', 'l', 'Low'],
            'close': ['close', 'c', 'Close'],
            'volume': ['volume', 'v', 'Volume', 'vol']
        }
        
        for key, possible_names in patterns.items():
            for col in df.columns:
                if col.lower() in [name.lower() for name in possible_names]:
                    ohlcv_mapping[key] = col
                    break
        
        # Prepare aggregation rules
        agg_rules = {}
        if 'open' in ohlcv_mapping:
            agg_rules[ohlcv_mapping['open']] = 'first'
        if 'high' in ohlcv_mapping:
            agg_rules[ohlcv_mapping['high']] = 'max'
        if 'low' in ohlcv_mapping:
            agg_rules[ohlcv_mapping['low']] = 'min'
        if 'close' in ohlcv_mapping:
            agg_rules[ohlcv_mapping['close']] = 'last'
        if 'volume' in ohlcv_mapping:
            agg_rules[ohlcv_mapping['volume']] = 'sum'
        
        # Add any other numeric columns with 'last' aggregation
        for col in df.columns:
            if col not in agg_rules and pd.api.types.is_numeric_dtype(df[col]):
                agg_rules[col] = 'last'
        
        # Resample to 5-minute bars
        df_5min = df.resample('5T').agg(agg_rules)
        
        # Remove rows where all OHLCV values are NaN
        df_5min = df_5min.dropna(how='all')
        
        # Reset index to make timestamp a column again
        df_5min.reset_index(inplace=True)
        
        # Save to output file
        df_5min.to_csv(output_file, index=False)
        
        return True, f"Resampled {os.path.basename(input_file)}: {len(df)} -> {len(df_5min)} rows"
        
    except Exception as e:
        return False, f"Error processing {os.path.basename(input_file)}: {str(e)}"

def process_asset_class(asset_class, base_dir='/root/stock_project/data'):
    """Process all 1-minute files for an asset class"""
    print(f"\n{'='*80}")
    print(f"Processing {asset_class}")
    print(f"{'='*80}")
    
    # Create 5MINUTE_BARS directory
    input_dir = os.path.join(base_dir, asset_class, '1MINUTE_BARS')
    output_dir = os.path.join(base_dir, asset_class, '5MINUTE_BARS')
    os.makedirs(output_dir, exist_ok=True)
    
    # Get all CSV files
    input_files = glob.glob(os.path.join(input_dir, '*.csv'))
    
    if not input_files:
        print(f"No CSV files found in {input_dir}")
        return
    
    print(f"Found {len(input_files)} files to process")
    
    # Process files in parallel
    successful = 0
    failed = 0
    
    # Use fewer workers to avoid memory issues
    max_workers = min(4, multiprocessing.cpu_count())
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_file = {}
        for input_file in input_files:
            output_file = os.path.join(output_dir, os.path.basename(input_file))
            future = executor.submit(resample_file, input_file, output_file)
            future_to_file[future] = input_file
        
        # Process completed tasks
        for i, future in enumerate(as_completed(future_to_file)):
            success, message = future.result()
            if success:
                successful += 1
            else:
                failed += 1
                print(f"  ❌ {message}")
            
            # Progress indicator
            if (i + 1) % 50 == 0:
                print(f"  Progress: {i + 1}/{len(input_files)} files processed...")
    
    print(f"\nCompleted {asset_class}:")
    print(f"  ✓ Successful: {successful} files")
    if failed > 0:
        print(f"  ✗ Failed: {failed} files")
    
    # Check output directory size
    output_files = glob.glob(os.path.join(output_dir, '*.csv'))
    if output_files:
        total_size = sum(os.path.getsize(f) for f in output_files) / (1024**3)  # GB
        print(f"  📁 Output size: {total_size:.2f} GB")

def main():
    """Main function to resample all asset classes"""
    print("5-Minute Bar Resampling Script")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check if pandas is installed
    try:
        import pandas
    except ImportError:
        print("\nError: pandas is required but not installed.")
        print("Installing pandas...")
        import subprocess
        subprocess.check_call(['pip3', 'install', 'pandas'])
        print("Pandas installed. Please run the script again.")
        return
    
    asset_classes = ['global_crypto', 'us_indices', 'us_stocks_sip']
    
    for asset_class in asset_classes:
        process_asset_class(asset_class)
    
    print(f"\n{'='*80}")
    print("All processing complete!")
    print(f"Ended at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")

if __name__ == '__main__':
    main()
