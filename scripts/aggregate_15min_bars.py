import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime
import glob
from pathlib import Path

def aggregate_to_15min(dataset, ticker, verify_only=False):
    """
    Aggregate 1-minute bars to 15-minute bars for a specific ticker and dataset.
    
    Args:
        dataset: Dataset name (us_stocks_sip, us_indices, global_crypto)
        ticker: Ticker symbol
        verify_only: If True, only print results without saving
    """
    # Define paths
    base_path = f"/root/stock_project/data/{dataset}"
    input_path = f"{base_path}/1MINUTE_BARS"
    output_path = f"{base_path}/15MINUTE_BARS"
    
    # Create output directory if it doesn't exist
    Path(output_path).mkdir(parents=True, exist_ok=True)
    
    # Get all CSV files in the input directory
    csv_files = sorted(glob.glob(f"{input_path}/*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {input_path}")
        return
    
    print(f"\n{'='*60}")
    print(f"Starting aggregation for ticker: {ticker}")
    print(f"Dataset: {dataset}")
    print(f"Mode: {'VERIFICATION ONLY (NOT SAVING)' if verify_only else 'FULL PROCESSING (SAVING)'}")
    print(f"Total files to process: {len(csv_files)}")
    print(f"{'='*60}\n")
    
    total_bars_processed = 0
    total_bars_created = 0
    files_with_data = 0
    
    # Process each file
    for file_num, csv_file in enumerate(csv_files, 1):
        filename = os.path.basename(csv_file)
        
        print(f"\n[{file_num}/{len(csv_files)}] Processing: {filename}")
        print("-" * 50)
        
        try:
            # Read the CSV file
            df = pd.read_csv(csv_file)
            
            # Check if first row might be a header
            first_row = df.iloc[0] if len(df) > 0 else None
            has_header = False
            
            if first_row is not None:
                try:
                    # Try to convert first value to float - if it fails, it's likely a header
                    float(first_row[0])
                except (ValueError, TypeError):
                    has_header = True
                    # Re-read with first row as header
                    df = pd.read_csv(csv_file, header=0)
            
            # If no header, assign column names based on number of columns
            if not has_header:
                if len(df.columns) == 8:
                    df.columns = ['ticker', 'open', 'close', 'high', 'low', 'timestamp', 'volume', 'transactions']
                elif len(df.columns) == 7:
                    df.columns = ['ticker', 'open', 'close', 'high', 'low', 'timestamp', 'volume']
                elif len(df.columns) == 6:
                    df.columns = ['ticker', 'open', 'close', 'high', 'low', 'timestamp']
                else:
                    print(f"⚠️  Unexpected number of columns ({len(df.columns)})")
                    continue
            
            # Handle different timestamp column names
            timestamp_col = None
            if 'timestamp' in df.columns:
                timestamp_col = 'timestamp'
            elif 'window_start' in df.columns:
                timestamp_col = 'window_start'
            else:
                print(f"⚠️  No timestamp column found")
                continue
            
            # Filter for the specific ticker
            ticker_data = df[df['ticker'] == ticker].copy()
            
            if ticker_data.empty:
                print(f"⚠️  No data found for ticker {ticker}")
                continue
            
            print(f"✓ Found {len(ticker_data)} 1-minute bars for {ticker}")
            
            # Convert timestamp to numeric
            ticker_data[timestamp_col] = pd.to_numeric(ticker_data[timestamp_col], errors='coerce')
            ticker_data = ticker_data.dropna(subset=[timestamp_col])
            
            if ticker_data.empty:
                print(f"⚠️  No valid timestamps after conversion")
                continue
            
            # Convert nanosecond timestamps to datetime
            ticker_data['datetime'] = pd.to_datetime(ticker_data[timestamp_col], unit='ns')
            ticker_data.set_index('datetime', inplace=True)
            ticker_data.sort_index(inplace=True)
            
            # Get time range
            start_time = ticker_data.index.min()
            end_time = ticker_data.index.max()
            print(f"✓ Time range: {start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%H:%M')}")
            
            # Resample to 15-minute bars
            agg_dict = {
                'open': 'first',
                'close': 'last', 
                'high': 'max',
                'low': 'min'
            }
            
            # Add optional columns
            if 'volume' in ticker_data.columns:
                agg_dict['volume'] = 'sum'
            if 'transactions' in ticker_data.columns:
                agg_dict['transactions'] = 'sum'
            
            # Keep the timestamp column for conversion back
            agg_dict['timestamp_numeric'] = 'first'
            ticker_data['timestamp_numeric'] = ticker_data[timestamp_col]
            
            # Perform aggregation
            print(f"→ Aggregating to 15-minute bars...")
            resampled = ticker_data.resample(pd.Timedelta(minutes=15)).agg(agg_dict)
            resampled = resampled.dropna(subset=['open'])
            
            if resampled.empty:
                print(f"⚠️  No 15-minute bars created after aggregation")
                continue
            
            print(f"✓ Created {len(resampled)} 15-minute bars")
            
            # Show details of first few aggregations
            if verify_only or file_num <= 2:
                print(f"\n  Sample aggregations:")
                for i, (idx, row) in enumerate(resampled.head(3).iterrows()):
                    print(f"  [{i+1}] {idx.strftime('%H:%M')}: O={row['open']:.2f}, H={row['high']:.2f}, L={row['low']:.2f}, C={row['close']:.2f}", end='')
                    if 'volume' in row:
                        print(f", V={int(row['volume'])}", end='')
                    print()
            
            # Prepare final dataframe
            resampled['ticker'] = ticker
            resampled[timestamp_col] = resampled.index.astype('int64')
            
            # Rename timestamp_numeric back if needed
            if 'timestamp_numeric' in resampled.columns:
                resampled.rename(columns={'timestamp_numeric': timestamp_col}, inplace=True)
            
            # Update counters
            total_bars_processed += len(ticker_data)
            total_bars_created += len(resampled)
            files_with_data += 1
            
            if not verify_only:
                # Save to output file
                output_file = f"{output_path}/{filename}"
                
                # Check if output file exists
                if os.path.exists(output_file):
                    # Append to existing file
                    existing_data = pd.read_csv(output_file)
                    combined_data = pd.concat([existing_data, resampled.reset_index(drop=True)])
                    combined_data.to_csv(output_file, index=False, header=False)
                    print(f"✓ Appended to existing file: {output_file}")
                else:
                    # Create new file
                    resampled.reset_index(drop=True).to_csv(output_file, index=False, header=False)
                    print(f"✓ Created new file: {output_file}")
            else:
                print(f"ℹ️  VERIFICATION MODE - Not saving to disk")
            
            print(f"✓ COMPLETED: {filename}")
            
        except Exception as e:
            print(f"❌ ERROR processing {filename}: {str(e)}")
            continue
    
    # Final summary
    print(f"\n{'='*60}")
    print(f"AGGREGATION SUMMARY")
    print(f"{'='*60}")
    print(f"Total files processed: {len(csv_files)}")
    print(f"Files with {ticker} data: {files_with_data}")
    print(f"Total 1-minute bars processed: {total_bars_processed:,}")
    print(f"Total 15-minute bars created: {total_bars_created:,}")
    print(f"Compression ratio: {total_bars_processed/max(total_bars_created, 1):.1f}:1")
    print(f"Status: {'VERIFICATION COMPLETE (no files saved)' if verify_only else 'ALL FILES SAVED SUCCESSFULLY'}")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 aggregate_15min_bars.py <dataset> <ticker> [verify]")
        print("Example: python3 aggregate_15min_bars.py us_stocks_sip A")
        print("         python3 aggregate_15min_bars.py us_stocks_sip A verify")
        sys.exit(1)
    
    dataset = sys.argv[1]
    ticker = sys.argv[2]
    verify_only = len(sys.argv) > 3 and sys.argv[3] == "verify"
    
    valid_datasets = ['us_stocks_sip', 'us_indices', 'global_crypto']
    if dataset not in valid_datasets:
        print(f"Invalid dataset. Must be one of: {', '.join(valid_datasets)}")
        sys.exit(1)
    
    aggregate_to_15min(dataset, ticker, verify_only)
