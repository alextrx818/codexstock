#!/usr/bin/env python3
"""
Test aggregation accuracy for the last completed date across all intervals
"""
import pandas as pd
import os

def test_aggregation_for_date(date_str):
    """Test aggregation for a specific date across all intervals"""
    
    print(f"AGGREGATION TEST FOR US_STOCKS_SIP - DATE: {date_str}")
    print("=" * 80)
    
    base_path = "/root/stock_project/data/us_stocks_sip"
    intervals = [1, 5, 15, 30, 60]
    
    # Dictionary to store dataframes
    dfs = {}
    
    # Load data for each interval
    for interval in intervals:
        if interval == 1:
            file_path = f"{base_path}/1MINUTE_BARS/{date_str}.csv"
        else:
            file_path = f"{base_path}/{interval}MINUTE_BARS/{date_str}.csv"
        
        if not os.path.exists(file_path):
            print(f"\n❌ File not found: {file_path}")
            continue
            
        print(f"\n{'='*60}")
        print(f"Loading {interval}-MINUTE data from: {file_path}")
        
        # Read the file
        df = pd.read_csv(file_path, nrows=100)  # Read extra to ensure we get 50 valid rows
        
        # Check if first row is header
        first_row = df.iloc[0] if len(df) > 0 else None
        has_header = False
        
        if first_row is not None:
            try:
                float(first_row.iloc[0])
            except (ValueError, TypeError):
                has_header = True
                df = pd.read_csv(file_path, header=0, nrows=100)
        
        # Assign column names if no header
        if not has_header:
            if len(df.columns) == 8:
                # Check if this is an aggregated file by looking at the data pattern
                # In aggregated files, ticker is typically the last column
                sample_last_col = str(df.iloc[0, -1]) if len(df) > 0 else ""
                sample_first_col = str(df.iloc[0, 0]) if len(df) > 0 else ""
                
                # If last column looks like a ticker (letters) and first looks like timestamp
                if sample_last_col.isalpha() and sample_first_col.isdigit():
                    df.columns = ['timestamp', 'open', 'close', 'high', 'low', 'volume', 'transactions', 'ticker']
                else:
                    df.columns = ['ticker', 'volume', 'open', 'close', 'high', 'low', 'timestamp', 'transactions']
            elif len(df.columns) == 7:
                df.columns = ['ticker', 'open', 'close', 'high', 'low', 'timestamp', 'volume']
            elif len(df.columns) == 6:
                df.columns = ['ticker', 'open', 'close', 'high', 'low', 'timestamp']
        
        print(f"Columns: {list(df.columns)}")
        print(f"Total rows in sample: {len(df)}")
        print(f"Has header: {has_header}")
        
        # Store the dataframe
        dfs[interval] = df.head(50)
        
        # Display first 5 rows
        print(f"\nFirst 5 rows of {interval}-minute data:")
        print("-" * 60)
        print(df.head(5).to_string(index=False))
    
    # Now let's test aggregation accuracy
    print("\n" + "="*80)
    print("AGGREGATION ACCURACY TEST")
    print("="*80)
    
    if 1 in dfs and 5 in dfs:
        print("\nTesting 1-min to 5-min aggregation for ticker 'A':")
        test_ticker_aggregation(dfs[1], dfs[5], 'A', 1, 5)
    
    if 1 in dfs and 15 in dfs:
        print("\nTesting 1-min to 15-min aggregation for ticker 'A':")
        test_ticker_aggregation(dfs[1], dfs[15], 'A', 1, 15)
    
    if 1 in dfs and 60 in dfs:
        print("\nTesting 1-min to 60-min aggregation for ticker 'A':")
        test_ticker_aggregation(dfs[1], dfs[60], 'A', 1, 60)

def test_ticker_aggregation(df_source, df_target, ticker, source_interval, target_interval):
    """Test aggregation accuracy for a specific ticker"""
    
    # Filter for the ticker
    source_data = df_source[df_source['ticker'] == ticker].copy()
    target_data = df_target[df_target['ticker'] == ticker].copy()
    
    if source_data.empty:
        print(f"  ❌ No data found for ticker {ticker} in {source_interval}-min data")
        return
    
    if target_data.empty:
        print(f"  ❌ No data found for ticker {ticker} in {target_interval}-min data")
        return
    
    print(f"  Source bars: {len(source_data)}")
    print(f"  Target bars: {len(target_data)}")
    
    # Find timestamp column
    source_timestamp_col = 'timestamp' if 'timestamp' in source_data.columns else 'window_start'
    target_timestamp_col = 'timestamp' if 'timestamp' in target_data.columns else 'window_start'
    
    # Convert timestamps
    source_data[source_timestamp_col] = pd.to_numeric(source_data[source_timestamp_col], errors='coerce')
    target_data[target_timestamp_col] = pd.to_numeric(target_data[target_timestamp_col], errors='coerce')
    
    source_data['datetime'] = pd.to_datetime(source_data[source_timestamp_col], unit='ns')
    target_data['datetime'] = pd.to_datetime(target_data[target_timestamp_col], unit='ns')
    
    # Take first target bar for detailed comparison
    if len(target_data) > 0:
        first_target = target_data.iloc[0]
        target_time = first_target['datetime']
        
        # Find corresponding source bars
        ratio = target_interval // source_interval
        start_time = target_time
        end_time = target_time + pd.Timedelta(minutes=target_interval)
        
        source_subset = source_data[
            (source_data['datetime'] >= start_time) & 
            (source_data['datetime'] < end_time)
        ]
        
        if len(source_subset) > 0:
            print(f"\n  Detailed check for first {target_interval}-min bar at {target_time.strftime('%H:%M')}:")
            print(f"  Found {len(source_subset)} corresponding {source_interval}-min bars")
            
            # Calculate expected values
            expected = {
                'open': source_subset.iloc[0]['open'],
                'close': source_subset.iloc[-1]['close'],
                'high': source_subset['high'].max(),
                'low': source_subset['low'].min()
            }
            
            if 'volume' in source_subset.columns:
                expected['volume'] = source_subset['volume'].sum()
            
            # Compare
            print(f"  Open:  Expected={expected['open']:.2f}, Actual={first_target['open']:.2f}, Match={'✓' if abs(expected['open'] - first_target['open']) < 0.01 else '✗'}")
            print(f"  Close: Expected={expected['close']:.2f}, Actual={first_target['close']:.2f}, Match={'✓' if abs(expected['close'] - first_target['close']) < 0.01 else '✗'}")
            print(f"  High:  Expected={expected['high']:.2f}, Actual={first_target['high']:.2f}, Match={'✓' if abs(expected['high'] - first_target['high']) < 0.01 else '✗'}")
            print(f"  Low:   Expected={expected['low']:.2f}, Actual={first_target['low']:.2f}, Match={'✓' if abs(expected['low'] - first_target['low']) < 0.01 else '✗'}")
            
            if 'volume' in expected and 'volume' in first_target:
                print(f"  Volume: Expected={int(expected['volume'])}, Actual={int(first_target['volume'])}, Match={'✓' if expected['volume'] == first_target['volume'] else '✗'}")

if __name__ == "__main__":
    # Test the last date where all intervals are complete
    test_aggregation_for_date("2024-09-19")
