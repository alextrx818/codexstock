#!/usr/bin/env python3
"""
Extended validation tests across multiple dates
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import random

# Project root
ROOT = Path(__file__).parent.parent

def get_sample_dates(dataset, n=5):
    """Get n random dates from a dataset"""
    path = ROOT / "data" / dataset / "1MINUTE_BARS"
    csv_files = sorted(path.glob("*.csv"))
    
    if len(csv_files) <= n:
        return [f.stem for f in csv_files]
    
    # Sample evenly across the date range
    indices = np.linspace(0, len(csv_files)-1, n, dtype=int)
    return [csv_files[i].stem for i in indices]


# Generate test parameters dynamically
def generate_test_params():
    """Generate dataset/date pairs for testing"""
    params = []
    for dataset in ["global_crypto", "us_stocks_sip", "us_indices"]:
        dates = get_sample_dates(dataset, n=3)  # Test 3 dates per dataset
        for date in dates:
            params.append((dataset, date))
    return params


@pytest.mark.parametrize("dataset,date", generate_test_params())
def test_extended_schema_validation(dataset, date):
    """Extended schema validation across multiple dates"""
    path = ROOT / "data" / dataset / "1MINUTE_BARS" / f"{date}.csv"
    
    if not path.exists():
        pytest.skip(f"File {path} does not exist")
    
    df = pd.read_csv(path)
    
    # Basic checks
    assert len(df) > 0, f"Empty file: {dataset}/{date}"
    
    # Check timestamp range
    ts = pd.to_datetime(df['window_start'], unit='ns')
    date_parsed = pd.to_datetime(date)
    
    # All timestamps should be on or near the date (some markets may span midnight)
    # Allow for timezone differences - data might span into next day
    min_date = ts.min().date()
    max_date = ts.max().date()
    
    assert min_date <= date_parsed.date() <= max_date, \
        f"Date {date} not within data range [{min_date}, {max_date}]"
    
    # Most timestamps should be on the target date
    ts_dates = ts.dt.date
    date_counts = ts_dates.value_counts()
    primary_date = date_counts.index[0]
    assert primary_date == date_parsed.date(), \
        f"Primary date {primary_date} doesn't match expected {date}"
    
    # Check for reasonable number of unique tickers
    n_tickers = df['ticker'].nunique()
    assert n_tickers > 0, f"No tickers in {dataset}/{date}"
    
    if dataset == "us_stocks_sip":
        assert n_tickers > 1000, f"Expected >1000 tickers for US stocks, got {n_tickers}"
    elif dataset == "us_indices":
        assert n_tickers > 100, f"Expected >100 tickers for US indices, got {n_tickers}"
    elif dataset == "global_crypto":
        assert n_tickers > 50, f"Expected >50 tickers for crypto, got {n_tickers}"


@pytest.mark.parametrize("interval", [5, 15, 30, 60])
def test_cross_interval_consistency(interval):
    """Test that aggregations are consistent across intervals"""
    # Use a single file for this test
    path = ROOT / "data" / "global_crypto" / "1MINUTE_BARS" / "2023-06-09.csv"
    df = pd.read_csv(path)
    
    # Pick a liquid ticker
    ticker = "X:BTCUSD"  # Bitcoin usually has good liquidity
    df_ticker = df[df['ticker'] == ticker].copy()
    
    if len(df_ticker) < 60:
        pytest.skip(f"Not enough data for {ticker}")
    
    # Convert timestamps
    df_ticker['timestamp'] = pd.to_datetime(df_ticker['window_start'], unit='ns')
    df_ticker = df_ticker.sort_values('timestamp')
    df_ticker.set_index('timestamp', inplace=True)
    
    # Get a 60-minute window
    start_time = df_ticker.index[0].replace(minute=0, second=0)
    end_time = start_time + pd.Timedelta(hours=1)
    hour_data = df_ticker[start_time:end_time].iloc[:-1]
    
    if len(hour_data) < 60:
        pytest.skip("Not enough data in hour window")
    
    # Calculate expected values for the hour
    expected_open = hour_data['open'].iloc[0]
    expected_close = hour_data['close'].iloc[-1]
    expected_high = hour_data['high'].max()
    expected_low = hour_data['low'].min()
    expected_volume = hour_data['volume'].sum()
    
    # Now aggregate to the interval and sum up
    agg = hour_data.resample(f'{interval}min', label='left', closed='left').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    
    # The aggregated bars should preserve the hour's OHLCV
    if len(agg) > 0:
        assert agg['open'].iloc[0] == expected_open, f"Open mismatch for {interval}min aggregation"
        assert agg['close'].iloc[-1] == expected_close, f"Close mismatch for {interval}min aggregation"
        assert agg['high'].max() == expected_high, f"High mismatch for {interval}min aggregation"
        assert agg['low'].min() == expected_low, f"Low mismatch for {interval}min aggregation"
        assert abs(agg['volume'].sum() - expected_volume) < 0.01, f"Volume mismatch for {interval}min aggregation"


def test_aggregation_edge_cases():
    """Test aggregation handles edge cases properly"""
    # Create synthetic test data
    timestamps = pd.date_range('2023-01-01 09:30', '2023-01-01 10:30', freq='1min')
    n_timestamps = len(timestamps)
    
    # Case 1: Missing minutes
    sparse_timestamps = timestamps[::2]  # Every other minute
    n_sparse = len(sparse_timestamps)
    
    df = pd.DataFrame({
        'timestamp': sparse_timestamps,
        'open': range(0, n_sparse),
        'high': range(1, n_sparse + 1),
        'low': range(0, n_sparse),
        'close': range(1, n_sparse + 1),
        'volume': [100] * n_sparse,
        'window_start': sparse_timestamps.astype(np.int64)
    })
    df.set_index('timestamp', inplace=True)
    
    # Aggregate to 5 minutes
    agg = df.resample('5min', label='left', closed='left').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    
    # Should handle gaps correctly
    assert len(agg) > 0
    assert (agg['volume'] <= 300).all()  # Max 3 minutes per 5-min bar due to gaps
    
    # Case 2: All same price (flat market)
    df_flat = pd.DataFrame({
        'timestamp': timestamps[:10],
        'open': [100.0] * 10,
        'high': [100.0] * 10,
        'low': [100.0] * 10,
        'close': [100.0] * 10,
        'volume': [50] * 10,
        'window_start': timestamps[:10].astype(np.int64)
    })
    df_flat.set_index('timestamp', inplace=True)
    
    agg_flat = df_flat.resample('5min', label='left', closed='left').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    
    # All prices should still be 100
    assert (agg_flat[['open', 'high', 'low', 'close']] == 100.0).all().all()
    assert (agg_flat['volume'] == 250).all()  # 5 bars * 50 volume


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-x"])  # Stop on first failure
