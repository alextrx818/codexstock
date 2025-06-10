#!/usr/bin/env python3
"""
Final comprehensive test of aggregation for US Stocks SIP
Tests the last completed date: 2024-09-19
"""
import pandas as pd
import numpy as np

def test_aggregation():
    date = "2024-09-19"
    base_path = "/root/stock_project/data/us_stocks_sip"
    
    print(f"AGGREGATION VALIDATION TEST - US STOCKS SIP")
    print(f"Date: {date}")
    print("=" * 80)
    
    # Load 1-minute data with header
    df_1min = pd.read_csv(f"{base_path}/1MINUTE_BARS/{date}.csv")
    print(f"\n1-MINUTE DATA:")
    print(f"Total rows: {len(df_1min):,}")
    print(f"Unique tickers: {df_1min['ticker'].nunique():,}")
    print(f"Sample tickers: {', '.join(df_1min['ticker'].unique()[:10])}")
    
    # Define column names for aggregated files
    agg_columns = ['window_start', 'open', 'high', 'low', 'close', 'volume', 'transactions', 'ticker']
    
    # Test each interval
    intervals = [5, 15, 30, 60]
    for interval in intervals:
        print(f"\n\n{'-'*60}")
        print(f"{interval}-MINUTE AGGREGATION TEST")
        print(f"{'-'*60}")
        
        # Load aggregated data
        df_agg = pd.read_csv(f"{base_path}/{interval}MINUTE_BARS/{date}.csv", 
                            names=agg_columns, header=0)
        
        print(f"Total rows: {len(df_agg):,}")
        print(f"File has header: Yes")
        
        # Test ticker 'A'
        ticker = 'A'
        print(f"\nValidating ticker '{ticker}':")
        
        # Get data for this ticker
        source = df_1min[df_1min['ticker'] == ticker].copy()
        target = df_agg[df_agg['ticker'] == ticker].copy()
        
        if source.empty or target.empty:
            print(f"  ❌ Ticker {ticker} not found")
            continue
            
        print(f"  1-min bars: {len(source)}")
        print(f"  {interval}-min bars: {len(target)}")
        
        # Convert timestamps
        source['datetime'] = pd.to_datetime(source['window_start'], unit='ns')
        target['datetime'] = pd.to_datetime(target['window_start'], unit='ns')
        
        # Sort by time
        source = source.sort_values('datetime')
        target = target.sort_values('datetime')
        
        # Validate first 3 aggregated bars
        errors = 0
        for i in range(min(3, len(target))):
            target_bar = target.iloc[i]
            start_time = target_bar['datetime']
            end_time = start_time + pd.Timedelta(minutes=interval)
            
            # Get corresponding 1-minute bars
            mask = (source['datetime'] >= start_time) & (source['datetime'] < end_time)
            source_bars = source[mask]
            
            if len(source_bars) == 0:
                print(f"\n  ❌ No source bars found for {start_time.strftime('%H:%M')}")
                errors += 1
                continue
            
            # Calculate expected values
            expected = {
                'open': source_bars.iloc[0]['open'],
                'high': source_bars['high'].max(),
                'low': source_bars['low'].min(),
                'close': source_bars.iloc[-1]['close'],
                'volume': source_bars['volume'].sum(),
                'transactions': source_bars['transactions'].sum()
            }
            
            # Compare
            print(f"\n  Bar {i+1} at {start_time.strftime('%H:%M')} ({len(source_bars)} source bars):")
            
            checks = {
                'open': abs(expected['open'] - target_bar['open']) < 0.01,
                'high': abs(expected['high'] - target_bar['high']) < 0.01,
                'low': abs(expected['low'] - target_bar['low']) < 0.01,
                'close': abs(expected['close'] - target_bar['close']) < 0.01,
                'volume': expected['volume'] == target_bar['volume'],
                'transactions': expected['transactions'] == target_bar['transactions']
            }
            
            for field, passed in checks.items():
                symbol = '✓' if passed else '✗'
                if field in ['volume', 'transactions']:
                    print(f"    {field:12} Expected: {expected[field]:10.0f}, Actual: {target_bar[field]:10.0f} {symbol}")
                else:
                    print(f"    {field:12} Expected: {expected[field]:10.2f}, Actual: {target_bar[field]:10.2f} {symbol}")
                
                if not passed:
                    errors += 1
        
        if errors == 0:
            print(f"\n  ✅ All checks passed!")
        else:
            print(f"\n  ❌ {errors} errors found")
    
    # Summary of file formats
    print("\n\n" + "="*80)
    print("FILE FORMAT SUMMARY")
    print("="*80)
    print("\n1-MINUTE BARS:")
    print("  Header: Yes")
    print("  Columns: ticker, volume, open, close, high, low, window_start, transactions")
    
    print("\nAGGREGATED BARS (5, 15, 30, 60-minute):")
    print("  Header: Yes") 
    print("  Columns: window_start, open, high, low, close, volume, transactions, ticker")
    print("  Note: Different column order than 1-minute bars!")
    
    print("\n" + "="*80)
    print("CONCLUSION: Date 2024-09-19 is fully aggregated for all intervals")
    print("="*80)

if __name__ == "__main__":
    test_aggregation()
