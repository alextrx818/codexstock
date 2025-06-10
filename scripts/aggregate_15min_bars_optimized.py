import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime
import glob
from pathlib import Path

def process_file(csv_file, tickers, dataset, verify_only=False):
    """
    Process a single CSV file for multiple tickers at once.
    """
    filename = os.path.basename(csv_file)
    output_path = f"/root/stock_project/data/{dataset}/15MINUTE_BARS"
    
    print(f"\nProcessing file: {filename}")
    print("-" * 50)
    
    try:
        # Read the CSV file ONCE
        df = pd.read_csv(csv_file, low_memory=False)
        
        # Check if first row might be a header
        first_row = df.iloc[0] if len(df) > 0 else None
        has_header = False
        
        if first_row is not None:
            try:
                float(first_row.iloc[0])
            except (ValueError, TypeError):
                has_header = True
                df = pd.read_csv(csv_file, header=0, low_memory=False)
        
        # Assign column names if no header
        if not has_header:
            if len(df.columns) == 8:
                df.columns = ['ticker', 'open', 'close', 'high', 'low', 'timestamp', 'volume', 'transactions']
            elif len(df.columns) == 7:
                df.columns = ['ticker', 'open', 'close', 'high', 'low', 'timestamp', 'volume']
            elif len(df.columns) == 6:
                df.columns = ['ticker', 'open', 'close', 'high', 'low', 'timestamp']
            else:
                print(f"⚠️  Unexpected number of columns ({len(df.columns)})")
                return
        
        # Handle timestamp column names
        timestamp_col = 'timestamp' if 'timestamp' in df.columns else 'window_start'
        if timestamp_col not in df.columns:
            print(f"⚠️  No timestamp column found")
            return
        
        # Convert timestamps once for entire dataframe
        df[timestamp_col] = pd.to_numeric(df[timestamp_col], errors='coerce')
        df = df.dropna(subset=[timestamp_col])
        
        if df.empty:
            print(f"⚠️  No valid data after timestamp conversion")
            return
        
        # Convert to datetime once
        df['datetime'] = pd.to_datetime(df[timestamp_col], unit='ns')
        
        # Process each ticker
        for ticker in tickers:
            ticker_data = df[df['ticker'] == ticker].copy()
            
            if ticker_data.empty:
                continue
            
            print(f"  {ticker}: Found {len(ticker_data)} 1-minute bars", end='')
            
            # Set datetime as index
            ticker_data.set_index('datetime', inplace=True)
            ticker_data.sort_index(inplace=True)
            
            # Aggregation rules
            agg_dict = {
                'open': 'first',
                'close': 'last',
                'high': 'max',
                'low': 'min'
            }
            
            if 'volume' in ticker_data.columns:
                agg_dict['volume'] = 'sum'
            if 'transactions' in ticker_data.columns:
                agg_dict['transactions'] = 'sum'
            
            # Keep timestamp for conversion back
            ticker_data['timestamp_numeric'] = ticker_data[timestamp_col]
            agg_dict['timestamp_numeric'] = 'first'
            
            # Resample to 15-minute bars
            resampled = ticker_data.resample(pd.Timedelta(minutes=15)).agg(agg_dict)
            resampled = resampled.dropna(subset=['open'])
            
            if resampled.empty:
                print(" → No 15-minute bars created")
                continue
            
            print(f" → {len(resampled)} 15-minute bars")
            
            # Prepare for output
            resampled['ticker'] = ticker
            resampled[timestamp_col] = resampled.index.astype('int64')
            
            if 'timestamp_numeric' in resampled.columns:
                resampled.rename(columns={'timestamp_numeric': timestamp_col}, inplace=True)
            
            if not verify_only:
                # Save to output file
                output_file = f"{output_path}/{filename}"
                
                if os.path.exists(output_file):
                    resampled.reset_index(drop=True).to_csv(output_file, mode='a', header=False, index=False)
                else:
                    resampled.reset_index(drop=True).to_csv(output_file, header=False, index=False)
        
        print(f"✓ Completed {filename}")
        
    except Exception as e:
        print(f"❌ ERROR processing {filename}: {str(e)}")
        import traceback
        traceback.print_exc()

def aggregate_multiple_tickers(dataset, tickers, verify_only=False):
    """
    Aggregate 1-minute bars to 15-minute bars for multiple tickers efficiently.
    """
    # Define paths
    base_path = f"/root/stock_project/data/{dataset}"
    input_path = f"{base_path}/1MINUTE_BARS"
    output_path = f"{base_path}/15MINUTE_BARS"
    
    # Create output directory
    Path(output_path).mkdir(parents=True, exist_ok=True)
    
    # Get all CSV files
    csv_files = sorted(glob.glob(f"{input_path}/*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {input_path}")
        return
    
    print(f"\n{'='*60}")
    print(f"Starting aggregation for {len(tickers)} tickers")
    print(f"Dataset: {dataset}")
    print(f"Mode: {'VERIFICATION ONLY' if verify_only else 'FULL PROCESSING'}")
    print(f"Total files: {len(csv_files)}")
    print(f"{'='*60}")
    
    # Process each file (reading once, processing all tickers)
    for i, csv_file in enumerate(csv_files, 1):
        print(f"\n[{i}/{len(csv_files)}]", end='')
        process_file(csv_file, tickers, dataset, verify_only)
    
    print(f"\n{'='*60}")
    print("AGGREGATION COMPLETE")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 aggregate_15min_bars_optimized.py <dataset> <ticker1> [ticker2] ...")
        print("Example: python3 aggregate_15min_bars_optimized.py us_stocks_sip A AAPL MSFT")
        sys.exit(1)
    
    dataset = sys.argv[1]
    tickers = sys.argv[2:]  # All remaining arguments are tickers
    
    valid_datasets = ['us_stocks_sip', 'us_indices', 'global_crypto']
    if dataset not in valid_datasets:
        print(f"Invalid dataset. Must be one of: {', '.join(valid_datasets)}")
        sys.exit(1)
    
    aggregate_multiple_tickers(dataset, tickers, verify_only=False)
