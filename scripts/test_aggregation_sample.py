import pandas as pd
import numpy as np
import os
import glob

def test_aggregation_on_sample(dataset):
    """Test aggregation on 10-line samples from each file"""
    
    input_dir = f"/root/stock_project/data/{dataset}/1MINUTE_BARS"
    csv_files = sorted(glob.glob(f"{input_dir}/*.csv"))[:5]  # Test first 5 files only
    
    print(f"\n{'='*60}")
    print(f"Testing 15-min aggregation for: {dataset}")
    print(f"Using 10-line samples from first 5 files")
    print(f"{'='*60}")
    
    for csv_file in csv_files:
        filename = os.path.basename(csv_file)
        print(f"\n📄 Testing: {filename}")
        print("-" * 40)
        
        try:
            # Read just first 10 lines (plus header)
            df_sample = pd.read_csv(csv_file, nrows=10)
            print(f"✓ Read {len(df_sample)} sample lines")
            
            # Show columns
            print(f"  Columns: {', '.join(df_sample.columns)}")
            
            # Detect timestamp column
            timestamp_col = 'timestamp' if 'timestamp' in df_sample.columns else 'window_start'
            
            # Get first ticker
            first_ticker = df_sample['ticker'].iloc[0]
            ticker_data = df_sample[df_sample['ticker'] == first_ticker].copy()
            print(f"  Testing ticker: {first_ticker} ({len(ticker_data)} bars)")
            
            # Convert timestamp
            ticker_data['datetime'] = pd.to_datetime(pd.to_numeric(ticker_data[timestamp_col]), unit='ns')
            ticker_data.set_index('datetime', inplace=True)
            
            # Show time range
            print(f"  Time range: {ticker_data.index.min().strftime('%H:%M:%S')} - {ticker_data.index.max().strftime('%H:%M:%S')}")
            
            # Show original 1-min data
            print(f"\n  Original 1-minute bars:")
            for idx, row in ticker_data.head(5).iterrows():
                print(f"    {idx.strftime('%H:%M')}: O={row['open']:.2f}, H={row['high']:.2f}, L={row['low']:.2f}, C={row['close']:.2f}, V={row.get('volume', 0)}")
            
            # Aggregate to 15-min
            agg_dict = {
                'open': 'first',
                'close': 'last', 
                'high': 'max',
                'low': 'min'
            }
            if 'volume' in ticker_data.columns:
                agg_dict['volume'] = 'sum'
            
            resampled = ticker_data.resample('15min').agg(agg_dict)
            resampled = resampled.dropna(subset=['open'])
            
            if not resampled.empty:
                print(f"\n  ✅ Created {len(resampled)} 15-minute bar(s):")
                for idx, row in resampled.iterrows():
                    # Find corresponding 1-min bars
                    start_time = idx
                    end_time = idx + pd.Timedelta(minutes=15)
                    original = ticker_data[(ticker_data.index >= start_time) & (ticker_data.index < end_time)]
                    
                    print(f"\n    15-min bar at {idx.strftime('%H:%M')}:")
                    print(f"      Aggregated from {len(original)} 1-min bars")
                    print(f"      O={row['open']:.2f}, H={row['high']:.2f}, L={row['low']:.2f}, C={row['close']:.2f}")
                    
                    # Verify
                    if len(original) > 0:
                        expected_open = original.iloc[0]['open']
                        expected_close = original.iloc[-1]['close']
                        expected_high = original['high'].max()
                        expected_low = original['low'].min()
                        
                        checks = [
                            ('Open', row['open'], expected_open),
                            ('Close', row['close'], expected_close),
                            ('High', row['high'], expected_high),
                            ('Low', row['low'], expected_low)
                        ]
                        
                        all_passed = True
                        for field, actual, expected in checks:
                            if actual == expected:
                                print(f"      ✓ {field}: {actual:.2f} = {expected:.2f}")
                            else:
                                print(f"      ❌ {field}: {actual:.2f} ≠ {expected:.2f}")
                                all_passed = False
                        
                        if 'volume' in row and 'volume' in original.columns:
                            expected_vol = original['volume'].sum()
                            if row['volume'] == expected_vol:
                                print(f"      ✓ Volume: {row['volume']} = {expected_vol}")
                            else:
                                print(f"      ❌ Volume: {row['volume']} ≠ {expected_vol}")
                                all_passed = False
                        
                        if all_passed:
                            print(f"      🎯 All checks passed!")
            else:
                print(f"  ⚠️  No 15-min bars created (sample too small)")
                
        except Exception as e:
            print(f"  ❌ Error: {str(e)}")

# Test all datasets
datasets = ['us_stocks_sip', 'us_indices', 'global_crypto']

for dataset in datasets:
    if os.path.exists(f"/root/stock_project/data/{dataset}/1MINUTE_BARS"):
        test_aggregation_on_sample(dataset)
    else:
        print(f"\n⚠️  Skipping {dataset} - no 1MINUTE_BARS folder found")
