#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

# Sample one file from each dataset
datasets = {
    'crypto': 'data/global_crypto/1MINUTE_BARS/2023-06-09.csv',
    'us_stocks': 'data/us_stocks_sip/1MINUTE_BARS/2023-06-09.csv', 
    'us_indices': 'data/us_indices/1MINUTE_BARS/2023-06-09.csv'
}

for name, filepath in datasets.items():
    print(f"\n{'='*60}")
    print(f"Dataset: {name}")
    print(f"File: {filepath}")
    print('='*60)
    
    df = pd.read_csv(filepath)
    
    print(f"\nShape: {df.shape}")
    print(f"Unique tickers: {df['ticker'].nunique()}")
    
    print("\nData types:")
    print(df.dtypes)
    
    print("\nSample data (first 3 rows):")
    print(df.head(3))
    
    print("\nNumeric column ranges:")
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_cols:
        if col != 'window_start':  # Skip timestamp
            print(f"  {col}: min={df[col].min():.4f}, max={df[col].max():.4f}")
