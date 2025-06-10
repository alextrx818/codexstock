#!/usr/bin/env python3
"""
Quick validation test for 1-minute CSV files and aggregations
Tests a sample from each dataset
"""

import pandas as pd
import numpy as np
from pathlib import Path

# Project root
ROOT = Path(__file__).parent.parent

def test_sample_files():
    """Test one file from each dataset"""
    test_files = [
        ROOT / "data" / "global_crypto" / "1MINUTE_BARS" / "2023-06-09.csv",
        ROOT / "data" / "us_stocks_sip" / "1MINUTE_BARS" / "2023-06-09.csv", 
        ROOT / "data" / "us_indices" / "1MINUTE_BARS" / "2023-06-09.csv"
    ]
    
    for filepath in test_files:
        if not filepath.exists():
            print(f"\n\nSkipping {filepath} - file does not exist")
            continue
            
        print(f"\n\nTesting {filepath.parent.parent.name}/{filepath.name}")
        print("=" * 60)
        
        # Read file
        df = pd.read_csv(filepath)
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        
        # Check required columns
        required_cols = {'ticker', 'volume', 'open', 'close', 'high', 'low', 'window_start', 'transactions'}
        missing = required_cols - set(df.columns)
        if missing:
            print(f"❌ Missing columns: {missing}")
        else:
            print("✓ All required columns present")
        
        # Check timestamps
        try:
            ts = pd.to_datetime(df['window_start'], unit='ns')
            print("✓ Timestamps parseable")
            
            # Check for duplicates per ticker
            unique_tickers = df['ticker'].nunique()
            print(f"Number of tickers: {unique_tickers}")
            
            # Sample a few tickers
            sample_tickers = df['ticker'].unique()[:3]
            for ticker in sample_tickers:
                ticker_df = df[df['ticker'] == ticker]
                ticker_ts = pd.to_datetime(ticker_df['window_start'], unit='ns')
                if ticker_ts.is_unique:
                    print(f"  ✓ Ticker {ticker}: {len(ticker_df)} rows, no duplicate timestamps")
                else:
                    dups = ticker_df[ticker_ts.duplicated(keep=False)]
                    print(f"  ❌ Ticker {ticker}: {len(dups)} duplicate timestamps")
                    
        except Exception as e:
            print(f"❌ Error parsing timestamps: {e}")
        
        # Data quality checks
        print("\nData quality checks:")
        if 'volume' in df.columns:
            print(f"  Volume range: {df['volume'].min():.0f} - {df['volume'].max():.0f}")
        else:
            print("  Volume: N/A (column not present)")
            
        print(f"  Price range: {df[['open', 'high', 'low', 'close']].min().min():.2f} - {df[['open', 'high', 'low', 'close']].max().max():.2f}")
        
        # OHLC consistency
        ohlc_issues = 0
        ohlc_issues += (df['high'] < df['low']).sum()
        ohlc_issues += (df['high'] < df['open']).sum() 
        ohlc_issues += (df['high'] < df['close']).sum()
        ohlc_issues += (df['low'] > df['open']).sum()
        ohlc_issues += (df['low'] > df['close']).sum()
        
        if ohlc_issues == 0:
            print("  ✓ OHLC relationships valid")
        else:
            print(f"  ❌ {ohlc_issues} OHLC relationship violations")

def test_aggregation_sample():
    """Test aggregation on a small sample"""
    print("\n\nTesting Aggregation")
    print("=" * 60)
    
    # Use a crypto file for testing
    filepath = ROOT / "data" / "global_crypto" / "1MINUTE_BARS" / "2023-06-09.csv"
    if not filepath.exists():
        print(f"Skipping aggregation test - {filepath} does not exist")
        return
        
    df = pd.read_csv(filepath)
    
    # Pick one ticker
    ticker = df['ticker'].value_counts().index[0]
    ticker_df = df[df['ticker'] == ticker].copy()
    print(f"Testing aggregation for ticker: {ticker}")
    print(f"1-minute bars: {len(ticker_df)}")
    
    # Convert timestamp and sort
    ticker_df['timestamp'] = pd.to_datetime(ticker_df['window_start'], unit='ns')
    ticker_df = ticker_df.sort_values('timestamp')
    ticker_df.set_index('timestamp', inplace=True)
    
    # Aggregate to 5 minutes
    agg_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last'
    }
    
    # Add volume and transactions if present
    if 'volume' in ticker_df.columns:
        agg_dict['volume'] = 'sum'
    if 'transactions' in ticker_df.columns:
        agg_dict['transactions'] = 'sum'
        
    agg_5min = ticker_df.resample('5min', label='left', closed='left').agg(agg_dict).dropna(subset=['open'])
    
    print(f"5-minute bars: {len(agg_5min)}")
    
    # Verify first few bars
    print("\nFirst 3 aggregated bars:")
    for i, (ts, row) in enumerate(agg_5min.head(3).iterrows()):
        vol_str = f" V={row['volume']:.0f}" if 'volume' in row else ""
        print(f"\n{ts}: O={row['open']:.4f} H={row['high']:.4f} L={row['low']:.4f} C={row['close']:.4f}{vol_str}")
        
        # Get corresponding 1-minute bars
        window_end = ts + pd.Timedelta(minutes=5)
        window_data = ticker_df[ts:window_end].iloc[:-1]  # Exclude endpoint
        
        if len(window_data) > 0:
            expected_open = window_data['open'].iloc[0]
            expected_close = window_data['close'].iloc[-1]
            expected_high = window_data['high'].max()
            expected_low = window_data['low'].min()
            
            # Verify
            checks = []
            checks.append(("Open", expected_open == row['open']))
            checks.append(("Close", expected_close == row['close']))
            checks.append(("High", expected_high == row['high']))
            checks.append(("Low", expected_low == row['low']))
            
            if 'volume' in window_data.columns and 'volume' in row:
                expected_vol = window_data['volume'].sum()
                checks.append(("Volume", abs(expected_vol - row['volume']) < 0.01))
            
            for check_name, passed in checks:
                print(f"  {check_name}: {'✓' if passed else '❌'}")

if __name__ == "__main__":
    test_sample_files()
    test_aggregation_sample()
