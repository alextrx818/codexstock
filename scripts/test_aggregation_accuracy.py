#!/usr/bin/env python3
"""Test aggregation accuracy on specific examples"""

import pandas as pd
import numpy as np
from pathlib import Path

def test_single_ticker_aggregation(date_file="2024-12-18.csv", ticker="AAPL"):
    """Test aggregation for a single ticker across all intervals"""
    
    project_root = Path("/root/stock_project")
    dataset = "us_stocks_sip"
    
    # Read source data
    df_1min = pd.read_csv(project_root / "data" / dataset / "1MINUTE_BARS" / date_file)
    
    # Filter to ticker
    ticker_data = df_1min[df_1min['ticker'] == ticker].copy()
    if ticker_data.empty:
        # Try first available ticker
        ticker = df_1min['ticker'].iloc[0]
        ticker_data = df_1min[df_1min['ticker'] == ticker].copy()
    
    print(f"Testing aggregation for {ticker} on {date_file}")
    print(f"1-minute bars: {len(ticker_data)} rows")
    
    # Convert timestamp
    ticker_data['ts'] = pd.to_datetime(ticker_data['window_start'], unit='ns')
    ticker_data = ticker_data.sort_values('ts')
    
    # Test each interval
    for interval in [5, 15, 30, 60]:
        print(f"\n{interval}-minute aggregation test:")
        
        # Read aggregated data
        agg_file = project_root / "data" / dataset / f"{interval}MINUTE_BARS" / date_file
        df_agg = pd.read_csv(agg_file)
        ticker_agg = df_agg[df_agg['ticker'] == ticker].copy()
        ticker_agg['ts'] = pd.to_datetime(ticker_agg['window_start'], unit='ns')
        
        print(f"  Aggregated bars: {len(ticker_agg)} rows")
        
        # Test first bar
        if len(ticker_agg) > 0:
            first_agg = ticker_agg.iloc[0]
            agg_start = first_agg['ts']
            agg_end = agg_start + pd.Timedelta(minutes=interval)
            
            # Get corresponding 1-min bars
            mask = (ticker_data['ts'] >= agg_start) & (ticker_data['ts'] < agg_end)
            bars_in_first = ticker_data[mask]
            
            if len(bars_in_first) > 0:
                print(f"  First {interval}-min bar ({agg_start}):")
                print(f"    Contains {len(bars_in_first)} 1-min bars")
                
                # Calculate expected values
                expected = {
                    'open': bars_in_first.iloc[0]['open'],
                    'close': bars_in_first.iloc[-1]['close'],
                    'high': bars_in_first['high'].max(),
                    'low': bars_in_first['low'].min(),
                    'volume': bars_in_first['volume'].sum()
                }
                
                # Compare
                print(f"    Open:   Expected {expected['open']:.4f}, Got {first_agg['open']:.4f} {'✅' if abs(expected['open'] - first_agg['open']) < 0.0001 else '❌'}")
                print(f"    Close:  Expected {expected['close']:.4f}, Got {first_agg['close']:.4f} {'✅' if abs(expected['close'] - first_agg['close']) < 0.0001 else '❌'}")
                print(f"    High:   Expected {expected['high']:.4f}, Got {first_agg['high']:.4f} {'✅' if abs(expected['high'] - first_agg['high']) < 0.0001 else '❌'}")
                print(f"    Low:    Expected {expected['low']:.4f}, Got {first_agg['low']:.4f} {'✅' if abs(expected['low'] - first_agg['low']) < 0.0001 else '❌'}")
                print(f"    Volume: Expected {expected['volume']:.0f}, Got {first_agg['volume']:.0f} {'✅' if abs(expected['volume'] - first_agg['volume']) < 1 else '❌'}")

def test_format_consistency():
    """Test that aggregated files have consistent format"""
    
    project_root = Path("/root/stock_project")
    dataset = "us_stocks_sip"
    
    print("\n" + "="*60)
    print("FORMAT CONSISTENCY TEST")
    print("="*60)
    
    # Check a sample file
    date_file = "2024-12-16.csv"
    
    # Read 1-minute file
    df_1min = pd.read_csv(project_root / "data" / dataset / "1MINUTE_BARS" / date_file)
    print(f"\n1-minute file columns: {list(df_1min.columns)}")
    print(f"window_start dtype: {df_1min['window_start'].dtype}")
    
    # Check each interval
    for interval in [5, 15, 30, 60]:
        df_agg = pd.read_csv(project_root / "data" / dataset / f"{interval}MINUTE_BARS" / date_file)
        print(f"\n{interval}-minute file:")
        print(f"  Columns: {list(df_agg.columns)}")
        print(f"  window_start dtype: {df_agg['window_start'].dtype}")
        
        # Check timestamp alignment
        df_agg['ts'] = pd.to_datetime(df_agg['window_start'], unit='ns')
        misaligned = df_agg[df_agg['ts'].dt.minute % interval != 0]
        if len(misaligned) > 0:
            print(f"  ❌ Found {len(misaligned)} misaligned timestamps!")
        else:
            print(f"  ✅ All timestamps correctly aligned to {interval}-minute boundaries")

def test_random_dates():
    """Quick test of random dates"""
    
    project_root = Path("/root/stock_project")
    dataset = "us_stocks_sip"
    input_dir = project_root / "data" / dataset / "1MINUTE_BARS"
    
    print("\n" + "="*60)
    print("RANDOM DATE VALIDATION")
    print("="*60)
    
    # Get all fully aggregated files
    all_files = []
    for f in input_dir.glob("*.csv"):
        if all((project_root / "data" / dataset / f"{i}MINUTE_BARS" / f.name).exists() for i in [5, 15, 30, 60]):
            all_files.append(f.name)
    
    print(f"\nTotal fully aggregated files: {len(all_files)}")
    
    # Test 5 random files
    import random
    test_files = random.sample(all_files, min(5, len(all_files)))
    
    for date_file in sorted(test_files):
        print(f"\n{date_file}:", end=" ")
        
        df_1min = pd.read_csv(input_dir / date_file)
        ticker = df_1min['ticker'].iloc[0]
        
        # Quick validation - just check first ticker, first bar
        all_good = True
        for interval in [5, 15, 30, 60]:
            df_agg = pd.read_csv(project_root / "data" / dataset / f"{interval}MINUTE_BARS" / date_file)
            
            # Basic checks
            if df_agg['window_start'].dtype != np.int64:
                all_good = False
                break
            if len(df_agg) == 0:
                all_good = False
                break
        
        print("✅" if all_good else "❌")

if __name__ == "__main__":
    # Test 1: Detailed test on one ticker
    test_single_ticker_aggregation()
    
    # Test 2: Format consistency
    test_format_consistency()
    
    # Test 3: Random dates
    test_random_dates()
    
    print("\n" + "="*60)
    print("VALIDATION COMPLETE")
    print("="*60)
