#!/usr/bin/env python3
"""Debug the aggregation function"""
import pandas as pd
import numpy as np

def aggregate_all_tickers(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    """
    Vectorized aggregation for all tickers at once - MUCH faster than looping
    """
    # convert timestamp once
    df = df.copy()
    df['ts'] = pd.to_datetime(df['window_start'], unit='ns')
    df = df.set_index(['ticker','ts']).sort_index()

    # one-step groupby-resample
    agg = (
        df
        .groupby(level='ticker', group_keys=False)
        .resample(f"{minutes}min", level='ts', label='left', closed='left')
        .agg({
            'open' : 'first',
            'high' : 'max',
            'low'  : 'min',
            'close': 'last'
            # US indices don't have volume/transactions
        })
        .dropna(subset=['open'])
    )
    
    print(f"After groupby-resample, agg type: {type(agg)}")
    print(f"agg.index: {agg.index}")
    print(f"agg.columns: {agg.columns}")
    
    # flatten back to columns - reset_index brings ticker and ts back as columns
    agg = agg.reset_index()
    
    print(f"\nAfter reset_index, agg.columns: {agg.columns}")
    print(f"First few rows:\n{agg.head()}")
    
    agg = agg.rename(columns={'ts':'window_start'})
    agg['window_start'] = agg['window_start'].astype(np.int64)
    
    print(f"\nFinal columns: {agg.columns}")
    
    return agg[['ticker','window_start','open','high','low','close']]

# Test with sample data
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
print("Input DataFrame:")
print(df)
print("\nRunning aggregation...")
result = aggregate_all_tickers(df, minutes=5)
print("\nFinal result:")
print(result)
