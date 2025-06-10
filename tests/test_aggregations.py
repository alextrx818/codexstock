#!/usr/bin/env python3
"""
Comprehensive aggregation tests for all crypto data
Tests schema validation and aggregation correctness
"""

import os
import glob
import pandas as pd
import numpy as np
import pytest
from pathlib import Path

# -- adjust this to be project-root relative --
ROOT = Path(__file__).parent.parent

BASE_DIRS = [
    ROOT / "data" / "global_crypto"  / "1MINUTE_BARS",
    ROOT / "data" / "us_stocks_sip"  / "1MINUTE_BARS",
    ROOT / "data" / "us_indices"     / "1MINUTE_BARS",
]

def find_csv_files():
    files = []
    for base in BASE_DIRS:
        if base.exists():
            files.extend(base.glob("*.csv"))
    return sorted(files)

def test_find_all_csvs():
    """Smoke test to verify we can find CSVs in all three directories"""
    files = find_csv_files()
    # Print for debugging — pytest will show this on failure
    print(f"\nFound {len(files)} CSV files total")
    print("\nFound CSVs by directory:")
    
    crypto_files = [f for f in files if "global_crypto" in str(f)]
    stocks_files = [f for f in files if "us_stocks_sip" in str(f)]
    indices_files = [f for f in files if "us_indices" in str(f)]
    
    print(f"  global_crypto: {len(crypto_files)} files")
    print(f"  us_stocks_sip: {len(stocks_files)} files")
    print(f"  us_indices: {len(indices_files)} files")
    
    # Show sample files from each directory
    if crypto_files:
        print(f"  Sample crypto file: {crypto_files[0].name}")
    if stocks_files:
        print(f"  Sample stocks file: {stocks_files[0].name}")
    if indices_files:
        print(f"  Sample indices file: {indices_files[0].name}")
    
    # expect at least one per directory
    assert len(crypto_files) > 0, "no crypto CSVs found"
    assert len(stocks_files) > 0, "no US-stocks CSVs found"
    assert len(indices_files) > 0, "no US-indices CSVs found"

# -- Aggregation function adapted to our data structure --
def aggregate_bars(df: pd.DataFrame, minutes: int, ticker: str = None) -> pd.DataFrame:
    """
    Aggregate 1-minute bars to specified interval.
    Handles our specific column structure with window_start as nanosecond timestamps.
    """
    df = df.copy()
    
    # Convert window_start from nanoseconds to datetime
    df['timestamp'] = pd.to_datetime(df['window_start'], unit='ns')
    df.set_index('timestamp', inplace=True)
    
    # Resample
    agg = df.resample(f'{minutes}min', label='left', closed='left').agg({
        'volume': 'sum',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'transactions': 'sum'
    })
    
    # Drop empty bars
    agg = agg.dropna(subset=['open', 'close'])
    
    # Add back nanosecond timestamps
    agg['window_start'] = agg.index.astype(np.int64)
    
    return agg

# --- rest of your tests unchanged ---
# Note: Based on actual CSV structure, columns are: ticker, volume, open, close, high, low, window_start, transactions
REQUIRED_COLS = {'ticker', 'volume', 'open', 'close', 'high', 'low', 'window_start', 'transactions'}
INTERVALS = [5, 15, 30, 60]

# Limit files for testing (remove limit to test all files)
def get_test_files(limit=10):
    """Get a subset of files for testing"""
    all_files = find_csv_files()
    if limit and limit < len(all_files):
        # Take some files from each directory
        files = []
        for base_dir in BASE_DIRS:
            dir_files = [f for f in all_files if str(base_dir) in str(f)]
            files.extend(dir_files[:min(limit//3, len(dir_files))])
        return files[:limit]
    return all_files

@pytest.mark.parametrize("filepath", get_test_files())
def test_csv_schema(filepath):
    """Each CSV must have the required columns and valid data."""
    df = pd.read_csv(filepath)
    
    # 1) Required columns present
    missing_cols = REQUIRED_COLS - set(df.columns)
    assert not missing_cols, f"Missing columns {missing_cols} in {filepath.name}"
    
    # 2) window_start should be parseable as nanosecond timestamps
    try:
        ts = pd.to_datetime(df['window_start'], unit='ns')
    except Exception as e:
        pytest.fail(f"Cannot parse window_start as nanosecond timestamps in {filepath.name}: {e}")
    
    # 3) No null timestamps
    assert not ts.isnull().any(), f"Null timestamps in {filepath.name}"
    
    # 4) Timestamps should be unique per ticker (not globally)
    # Multi-ticker files will have duplicate timestamps across tickers
    for ticker in df['ticker'].unique():
        ticker_df = df[df['ticker'] == ticker]
        ticker_ts = pd.to_datetime(ticker_df['window_start'], unit='ns')
        if not ticker_ts.is_unique:
            # Find duplicates for better error message
            duplicates = ticker_df[ticker_ts.duplicated(keep=False)]
            pytest.fail(f"Duplicate timestamps for ticker {ticker} in {filepath.name}: {len(duplicates)} duplicates found")

@pytest.mark.parametrize("interval", INTERVALS)
@pytest.mark.parametrize("filepath", get_test_files(limit=3))  # Test fewer files for aggregation
def test_aggregation_correctness(filepath, interval):
    """Test aggregation produces correct results."""
    df = pd.read_csv(filepath)
    
    # Run aggregation
    df_agg = aggregate_bars(df, interval)
    
    # 1) Check all required columns are present
    expected_cols = {'volume', 'open', 'high', 'low', 'close', 'transactions', 'window_start'}
    assert set(df_agg.columns) == expected_cols, f"Unexpected columns in aggregated data"
    
    # 2) Check sums match
    orig_sum_vol = df['volume'].sum()
    agg_sum_vol = df_agg['volume'].sum()
    assert abs(orig_sum_vol - agg_sum_vol) < 0.01, f"Volume sum mismatch: {orig_sum_vol} vs {agg_sum_vol}"
    
    orig_sum_tx = df['transactions'].sum()
    agg_sum_tx = df_agg['transactions'].sum()
    assert abs(orig_sum_tx - agg_sum_tx) < 0.01, f"Transaction sum mismatch: {orig_sum_tx} vs {agg_sum_tx}"
    
    # 3) Check OHLC relationships
    assert (df_agg['high'] >= df_agg['low']).all(), "High < Low found"
    assert (df_agg['high'] >= df_agg['open']).all(), "High < Open found"
    assert (df_agg['high'] >= df_agg['close']).all(), "High < Close found"
    assert (df_agg['low'] <= df_agg['open']).all(), "Low > Open found"
    assert (df_agg['low'] <= df_agg['close']).all(), "Low > Close found"
    
    # 4) Check frequency
    if len(df_agg) > 1:
        # Convert window_start back to datetime for frequency check
        df_agg['timestamp'] = pd.to_datetime(df_agg['window_start'], unit='ns')
        df_agg.set_index('timestamp', inplace=True)
        
        # Calculate time differences
        time_diffs = df_agg.index.to_series().diff().dropna()
        expected_diff = pd.Timedelta(minutes=interval)
        
        # Allow for some gaps (e.g., market closed)
        # Check that most differences are the expected interval
        correct_intervals = (time_diffs == expected_diff).sum()
        total_intervals = len(time_diffs)
        
        if total_intervals > 0:
            accuracy = correct_intervals / total_intervals
            assert accuracy > 0.8, f"Only {accuracy:.1%} of intervals are {interval} minutes"

# Additional tests can be added here
@pytest.mark.skip(reason="Not implemented yet")
def test_aggregation_pipeline_output():
    """Test that aggregation outputs match expected file format."""
    pass

@pytest.mark.skip(reason="Not implemented yet")
def test_cross_interval_consistency():
    """Test that aggregating 1min->5min->15min gives same result as 1min->15min."""
    pass
