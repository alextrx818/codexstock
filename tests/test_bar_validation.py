#!/usr/bin/env python3
"""
Pytest-based validation tests for 1-minute CSV files and aggregations
Tests schema, OHLC relationships, and aggregation correctness
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path

# Project root
ROOT = Path(__file__).parent.parent

# Test parameters
DATASETS_AND_DATES = [
    ("global_crypto", "2023-06-09"),
    ("us_stocks_sip", "2023-06-09"),
    ("us_indices", "2023-06-09"),
]

AGGREGATION_INTERVALS = [5, 15, 30, 60]


@pytest.mark.parametrize("dataset,date", DATASETS_AND_DATES)
def test_schema_and_ohlc(dataset, date):
    """Test CSV schema and OHLC relationships for each dataset"""
    path = ROOT / "data" / dataset / "1MINUTE_BARS" / f"{date}.csv"
    
    # Check file exists
    assert path.exists(), f"File {path} does not exist"
    
    # Read CSV
    df = pd.read_csv(path)
    assert len(df) > 0, f"Empty dataframe for {dataset}/{date}"
    
    # Check required columns based on dataset
    if dataset == "us_indices":
        # US indices don't have volume/transactions
        required_cols = {'ticker', 'open', 'high', 'low', 'close', 'window_start'}
    else:
        required_cols = {'ticker', 'volume', 'open', 'high', 'low', 'close', 'window_start', 'transactions'}
    
    missing_cols = required_cols - set(df.columns)
    assert not missing_cols, f"Missing columns {missing_cols} in {dataset}/{date}"
    
    # Test timestamp parsing
    ts = pd.to_datetime(df['window_start'], unit='ns')
    assert not ts.isnull().any(), f"Null timestamps in {dataset}/{date}"
    
    # Pick a ticker and test timestamp uniqueness
    ticker = df['ticker'].iloc[0]
    df_ticker = df[df['ticker'] == ticker]
    ts_ticker = pd.to_datetime(df_ticker['window_start'], unit='ns')
    assert ts_ticker.is_unique, f"Duplicate timestamps for ticker {ticker} in {dataset}/{date}"
    
    # OHLC relationship tests
    assert (df['high'] >= df['low']).all(), f"High < Low violations in {dataset}/{date}"
    assert (df['high'] >= df['open']).all(), f"High < Open violations in {dataset}/{date}"
    assert (df['high'] >= df['close']).all(), f"High < Close violations in {dataset}/{date}"
    assert (df['low'] <= df['open']).all(), f"Low > Open violations in {dataset}/{date}"
    assert (df['low'] <= df['close']).all(), f"Low > Close violations in {dataset}/{date}"
    
    # Additional data quality checks
    assert (df[['open', 'high', 'low', 'close']] >= 0).all().all(), f"Negative prices in {dataset}/{date}"
    
    if 'volume' in df.columns:
        assert (df['volume'] >= 0).all(), f"Negative volume in {dataset}/{date}"
    
    if 'transactions' in df.columns:
        assert (df['transactions'] >= 0).all(), f"Negative transactions in {dataset}/{date}"


def aggregate_bars(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    """Aggregate 1-minute bars to specified interval"""
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['window_start'], unit='ns')
    df = df.sort_values('timestamp')
    df.set_index('timestamp', inplace=True)
    
    # Build aggregation dict based on available columns
    agg_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last'
    }
    
    if 'volume' in df.columns:
        agg_dict['volume'] = 'sum'
    if 'transactions' in df.columns:
        agg_dict['transactions'] = 'sum'
    
    # Resample and aggregate
    agg = df.resample(f'{minutes}min', label='left', closed='left').agg(agg_dict)
    
    # Drop empty bars
    agg = agg.dropna(subset=['open'])
    
    # Add back window_start as nanoseconds
    agg['window_start'] = agg.index.astype(np.int64)
    
    return agg


@pytest.mark.parametrize("dataset,date", DATASETS_AND_DATES)
@pytest.mark.parametrize("interval", AGGREGATION_INTERVALS)
def test_aggregation_correctness(dataset, date, interval):
    """Test aggregation correctness for different intervals"""
    path = ROOT / "data" / dataset / "1MINUTE_BARS" / f"{date}.csv"
    
    # Read CSV
    df = pd.read_csv(path)
    
    # Pick the most active ticker (by row count)
    ticker = df['ticker'].value_counts().index[0]
    df_ticker = df[df['ticker'] == ticker].copy()
    
    # Skip if not enough data
    if len(df_ticker) < interval:
        pytest.skip(f"Not enough data for {interval}-minute aggregation")
    
    # Aggregate
    agg = aggregate_bars(df_ticker, interval)
    
    # Basic checks
    assert len(agg) > 0, f"Empty aggregation result for {dataset}/{date}/{ticker}/{interval}min"
    assert len(agg) <= len(df_ticker) / interval + 1, f"Too many aggregated bars"
    
    # Verify aggregation correctness by checking a few bars
    for i, (ts, row) in enumerate(agg.head(3).iterrows()):
        # Get corresponding 1-minute bars
        window_end = ts + pd.Timedelta(minutes=interval)
        window_data = df_ticker[
            (pd.to_datetime(df_ticker['window_start'], unit='ns') >= ts) &
            (pd.to_datetime(df_ticker['window_start'], unit='ns') < window_end)
        ]
        
        if len(window_data) == 0:
            continue
            
        # Verify OHLC
        assert row['open'] == window_data.iloc[0]['open'], \
            f"Open mismatch at {ts} for {dataset}/{ticker}/{interval}min"
        assert row['close'] == window_data.iloc[-1]['close'], \
            f"Close mismatch at {ts} for {dataset}/{ticker}/{interval}min"
        assert row['high'] == window_data['high'].max(), \
            f"High mismatch at {ts} for {dataset}/{ticker}/{interval}min"
        assert row['low'] == window_data['low'].min(), \
            f"Low mismatch at {ts} for {dataset}/{ticker}/{interval}min"
        
        # Verify volume/transactions if present
        if 'volume' in window_data.columns and 'volume' in row:
            expected_vol = window_data['volume'].sum()
            assert abs(row['volume'] - expected_vol) < 0.01, \
                f"Volume mismatch at {ts} for {dataset}/{ticker}/{interval}min"
        
        if 'transactions' in window_data.columns and 'transactions' in row:
            expected_trans = window_data['transactions'].sum()
            assert abs(row['transactions'] - expected_trans) < 0.01, \
                f"Transactions mismatch at {ts} for {dataset}/{ticker}/{interval}min"
    
    # Verify aggregated OHLC relationships
    assert (agg['high'] >= agg['low']).all(), f"High < Low in aggregated data"
    assert (agg['high'] >= agg['open']).all(), f"High < Open in aggregated data"
    assert (agg['high'] >= agg['close']).all(), f"High < Close in aggregated data"
    assert (agg['low'] <= agg['open']).all(), f"Low > Open in aggregated data"
    assert (agg['low'] <= agg['close']).all(), f"Low > Close in aggregated data"


@pytest.mark.parametrize("dataset", ["global_crypto", "us_stocks_sip", "us_indices"])
def test_file_discovery(dataset):
    """Test that we can find CSV files in each dataset"""
    path = ROOT / "data" / dataset / "1MINUTE_BARS"
    csv_files = list(path.glob("*.csv"))
    
    assert len(csv_files) > 0, f"No CSV files found in {dataset}"
    assert len(csv_files) >= 100, f"Expected at least 100 CSV files in {dataset}, found {len(csv_files)}"
    
    # Check file naming convention
    for f in csv_files[:5]:  # Check first 5
        assert f.stem.count('-') == 2, f"Invalid filename format: {f.name}"
        # Should be YYYY-MM-DD format
        parts = f.stem.split('-')
        assert len(parts[0]) == 4, f"Invalid year in {f.name}"
        assert len(parts[1]) == 2, f"Invalid month in {f.name}"
        assert len(parts[2]) == 2, f"Invalid day in {f.name}"


if __name__ == "__main__":
    # Run with pytest
    pytest.main([__file__, "-v"])
