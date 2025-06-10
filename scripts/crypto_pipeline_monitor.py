#!/usr/bin/env python3
"""
Monitoring and validation dashboard for crypto data pipeline
"""

import pandas as pd
import json
import os
from pathlib import Path
from datetime import datetime
import sys

BASE_DIR = Path("/root/stock_project/data/global_crypto")

def check_pipeline_status():
    """Check overall pipeline status and recent runs."""
    print("=" * 80)
    print("CRYPTO DATA PIPELINE STATUS")
    print("=" * 80)
    
    # Check pipeline metadata
    metadata_file = BASE_DIR / "pipeline_metadata.json"
    if metadata_file.exists():
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        
        print(f"Last run: {metadata.get('start_time', 'Unknown')}")
        print(f"Files processed: {metadata.get('files_processed', 0)}")
        print(f"Errors: {len(metadata.get('errors', []))}")
        
        if metadata.get('errors'):
            print("\nRecent errors:")
            for error in metadata['errors'][-5:]:
                print(f"  - {error['date']}: {error.get('error', 'Unknown error')}")
    else:
        print("No pipeline metadata found. Pipeline may not have run yet.")
    
    print("\n" + "-" * 80)

def check_data_coverage():
    """Check data coverage across different intervals."""
    print("\nDATA COVERAGE")
    print("-" * 80)
    
    intervals = ['1MINUTE_BARS', '5MINUTE_BARS', '15MINUTE_BARS', '30MINUTE_BARS', '60MINUTE_BARS']
    
    coverage = {}
    for interval in intervals:
        interval_dir = BASE_DIR / interval
        if interval_dir.exists():
            files = list(interval_dir.glob("*.csv"))
            coverage[interval] = len(files)
        else:
            coverage[interval] = 0
    
    # Display coverage
    print(f"{'Interval':<20} {'Files':<10} {'Status':<20}")
    print("-" * 50)
    
    for interval, count in coverage.items():
        status = "OK" if count > 0 else "Missing"
        if interval != '1MINUTE_BARS' and count < coverage['1MINUTE_BARS']:
            status = f"Incomplete ({count}/{coverage['1MINUTE_BARS']})"
        
        print(f"{interval:<20} {count:<10} {status:<20}")
    
    return coverage

def validate_sample_aggregations(sample_date: str = None):
    """Validate aggregations for a sample date."""
    print("\n" + "-" * 80)
    print("AGGREGATION VALIDATION")
    print("-" * 80)
    
    # Get a sample date if not provided
    if sample_date is None:
        files = list((BASE_DIR / "1MINUTE_BARS").glob("*.csv"))
        if not files:
            print("No 1-minute data found for validation")
            return
        sample_date = sorted([f.stem for f in files])[-1]  # Most recent
    
    print(f"Validating aggregations for: {sample_date}")
    
    # Read 1-minute data
    try:
        df_1min = pd.read_csv(BASE_DIR / "1MINUTE_BARS" / f"{sample_date}.csv")
        print(f"1-minute bars: {len(df_1min)} rows, {df_1min['ticker'].nunique()} tickers")
        
        # Sample a ticker for detailed validation
        sample_ticker = df_1min['ticker'].value_counts().index[0]  # Most common ticker
        df_1min_ticker = df_1min[df_1min['ticker'] == sample_ticker]
        
        print(f"\nDetailed validation for ticker: {sample_ticker}")
        print(f"1-minute data points: {len(df_1min_ticker)}")
        
        # Check each aggregation
        for interval in [5, 15, 30, 60]:
            interval_file = BASE_DIR / f"{interval}MINUTE_BARS" / f"{sample_date}.csv"
            
            if not interval_file.exists():
                print(f"\n{interval}-minute: FILE NOT FOUND")
                continue
            
            df_agg = pd.read_csv(interval_file)
            df_agg_ticker = df_agg[df_agg['ticker'] == sample_ticker]
            
            # Validate
            expected_bars = len(df_1min_ticker) // interval
            actual_bars = len(df_agg_ticker)
            
            # Volume check
            vol_1min = df_1min_ticker['volume'].sum()
            vol_agg = df_agg_ticker['volume'].sum()
            vol_match = abs(vol_1min - vol_agg) < 0.01
            
            # Transaction check
            trans_1min = df_1min_ticker['transactions'].sum()
            trans_agg = df_agg_ticker['transactions'].sum()
            trans_match = trans_1min == trans_agg
            
            print(f"\n{interval}-minute validation:")
            print(f"  Expected bars: ~{expected_bars}, Actual: {actual_bars}")
            print(f"  Volume match: {'✓' if vol_match else '✗'} (1min: {vol_1min:.2f}, agg: {vol_agg:.2f})")
            print(f"  Transaction match: {'✓' if trans_match else '✗'} (1min: {trans_1min}, agg: {trans_agg})")
            
            # Sample first bar details
            if len(df_agg_ticker) > 0:
                first_bar = df_agg_ticker.iloc[0]
                print(f"  First bar: O={first_bar['open']:.4f} H={first_bar['high']:.4f} "
                      f"L={first_bar['low']:.4f} C={first_bar['close']:.4f}")
    
    except Exception as e:
        print(f"Error during validation: {str(e)}")

def check_recent_processing():
    """Check recently processed files and their metadata."""
    print("\n" + "-" * 80)
    print("RECENT PROCESSING")
    print("-" * 80)
    
    metadata_dir = BASE_DIR / "metadata"
    if not metadata_dir.exists():
        print("No metadata directory found")
        return
    
    # Get recent metadata files
    metadata_files = sorted(metadata_dir.glob("*_metadata.json"), 
                          key=lambda x: x.stat().st_mtime, reverse=True)[:5]
    
    if not metadata_files:
        print("No metadata files found")
        return
    
    print(f"{'Date':<15} {'Checksum':<10} {'Rows':<10} {'Tickers':<10} {'Issues'}")
    print("-" * 60)
    
    for mf in metadata_files:
        with open(mf, 'r') as f:
            metadata = json.load(f)
        
        date = metadata.get('date', 'Unknown')
        checksum = metadata.get('checksum', '')[:8] + '...'
        rows = metadata.get('row_count', 0)
        tickers = metadata.get('ticker_count', 0)
        issues = len(metadata.get('raw_validation', {}).get('issues', []))
        
        print(f"{date:<15} {checksum:<10} {rows:<10} {tickers:<10} {issues}")

def main():
    """Main monitoring function."""
    if len(sys.argv) > 1 and sys.argv[1] == '--validate':
        # Detailed validation mode
        sample_date = sys.argv[2] if len(sys.argv) > 2 else None
        validate_sample_aggregations(sample_date)
    else:
        # Standard monitoring
        check_pipeline_status()
        check_data_coverage()
        check_recent_processing()
        
        print("\n" + "=" * 80)
        print("Run with --validate [date] for detailed aggregation validation")
        print("Example: python3 crypto_pipeline_monitor.py --validate 2025-06-01")

if __name__ == "__main__":
    main()
