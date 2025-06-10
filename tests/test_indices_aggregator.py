#!/usr/bin/env python3
"""
Test suite for US Indices aggregation (no volume/transactions)
"""
import pandas as pd
import numpy as np
import sys
from pathlib import Path

# Add parent directory to path to import our aggregation functions
sys.path.append(str(Path(__file__).parent.parent))

# Import the vectorized aggregation function from our parallel script
from scripts.aggregate_us_indices_parallel import aggregate_all_tickers

def test_indices_aggregate_columns_and_dtypes():
    """Test that aggregation produces correct columns and dtypes for indices"""
    # Hand-crafted 1-min sample for ticker "IDX"
    data = {
        'ticker': ['IDX','IDX','IDX'],
        'window_start': [
            int(pd.Timestamp("2023-06-01 09:30").value),
            int(pd.Timestamp("2023-06-01 09:31").value),
            int(pd.Timestamp("2023-06-01 09:32").value),
        ],
        'open':   [100,101,102],
        'high':   [101,102,103],
        'low':    [ 99,100,101],
        'close':  [100,101,102],
    }
    df = pd.DataFrame(data)
    agg = aggregate_all_tickers(df, minutes=5)

    # Should have exactly these columns (no volume/transactions)
    expected = ['ticker','window_start','open','high','low','close']
    assert list(agg.columns) == expected

    # window_start remains integer
    assert np.issubdtype(agg['window_start'].dtype, np.integer)
    
    print("✓ Column and dtype test passed")

def test_indices_known_window():
    """Test aggregation with known values"""
    base_ns = int(pd.Timestamp("2023-06-01 09:30").value)
    # 3 one-minute bars
    data = [
        {'ticker':'IDX','window_start':base_ns               ,'open':1,'high':3,'low':1,'close':2},
        {'ticker':'IDX','window_start':base_ns+60*1_000_000_000,'open':2,'high':4,'low':2,'close':3},
        {'ticker':'IDX','window_start':base_ns+2*60*1_000_000_000,'open':3,'high':5,'low':3,'close':4},
    ]
    df = pd.DataFrame(data)
    agg = aggregate_all_tickers(df, minutes=5).set_index('ticker').iloc[0]

    # Manual expectations
    assert agg['open']  == 1
    assert agg['high']  == 5
    assert agg['low']   == 1
    assert agg['close'] == 4
    
    print("✓ Known value test passed")

def test_indices_real_file_sample():
    """Test on a real indices file sample"""
    # Check if file exists
    test_file = Path("/root/stock_project/data/us_indices/1MINUTE_BARS/2023-06-09.csv")
    if not test_file.exists():
        print(f"⚠ Skipping real file test - {test_file} not found")
        return
    
    # Read first 1000 rows for quick test
    src = pd.read_csv(test_file, nrows=1000)
    
    # Test 5-minute aggregation
    agg5 = aggregate_all_tickers(src, 5)
    
    # Basic sanity checks
    assert len(agg5) > 0, "Aggregation produced no results"
    assert len(agg5) < len(src), "Aggregation should reduce row count"
    
    # Check that all required columns exist
    required_cols = ['ticker','window_start','open','high','low','close']
    for col in required_cols:
        assert col in agg5.columns, f"Missing column: {col}"
    
    # Verify no volume/transactions columns
    assert 'volume' not in agg5.columns
    assert 'transactions' not in agg5.columns
    
    # Check a specific ticker's aggregation
    ticker_sample = agg5['ticker'].iloc[0]
    ticker_src = src[src['ticker'] == ticker_sample]
    ticker_agg = agg5[agg5['ticker'] == ticker_sample]
    
    if len(ticker_src) > 0 and len(ticker_agg) > 0:
        # First aggregated bar should have:
        # - open from first source bar
        # - close from last bar in window
        # - high as max of all highs
        # - low as min of all lows
        first_agg = ticker_agg.iloc[0]
        window_start = first_agg['window_start']
        window_end = window_start + 5 * 60 * 1_000_000_000  # 5 minutes in nanoseconds
        
        window_data = ticker_src[
            (ticker_src['window_start'] >= window_start) & 
            (ticker_src['window_start'] < window_end)
        ]
        
        if len(window_data) > 0:
            assert first_agg['open'] == window_data.iloc[0]['open']
            assert first_agg['high'] == window_data['high'].max()
            assert first_agg['low'] == window_data['low'].min()
            assert first_agg['close'] == window_data.iloc[-1]['close']
    
    print(f"✓ Real file test passed - aggregated {len(src)} rows to {len(agg5)} rows")

def test_multiple_tickers_aggregation():
    """Test aggregation with multiple tickers"""
    base_ns = int(pd.Timestamp("2023-06-01 09:30").value)
    
    # Create data for two tickers
    data = []
    for ticker in ['IDX1', 'IDX2']:
        for i in range(10):  # 10 minutes of data
            data.append({
                'ticker': ticker,
                'window_start': base_ns + i * 60 * 1_000_000_000,
                'open': 100 + i,
                'high': 101 + i,
                'low': 99 + i,
                'close': 100.5 + i
            })
    
    df = pd.DataFrame(data)
    
    # Aggregate to 5-minute bars
    agg = aggregate_all_tickers(df, 5)
    
    # Should have 2 tickers * 2 five-minute windows = 4 rows
    assert len(agg) == 4
    
    # Check each ticker has 2 aggregated bars
    for ticker in ['IDX1', 'IDX2']:
        ticker_agg = agg[agg['ticker'] == ticker]
        assert len(ticker_agg) == 2
        
        # First 5-min bar should aggregate minutes 0-4
        first_bar = ticker_agg.iloc[0]
        assert first_bar['open'] == 100  # from minute 0
        assert first_bar['close'] == 104.5  # from minute 4
        assert first_bar['high'] == 105  # max from minutes 0-4
        assert first_bar['low'] == 99  # min from minutes 0-4
    
    print("✓ Multiple ticker test passed")

if __name__ == "__main__":
    print("Running US Indices aggregation tests...\n")
    
    try:
        test_indices_aggregate_columns_and_dtypes()
        test_indices_known_window()
        test_multiple_tickers_aggregation()
        test_indices_real_file_sample()
        
        print("\n✅ All tests passed!")
        
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        raise
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        raise
