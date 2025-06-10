#!/usr/bin/env python3
"""
Detailed technical analysis of why schema differences exist
"""
import pandas as pd
from pathlib import Path

print("DETAILED TECHNICAL ANALYSIS OF SCHEMA DIFFERENCES")
print("="*70)

# 1. Let's examine the actual CSV headers
print("\n1. RAW CSV HEADERS (first line of each file):")
print("-"*70)

datasets = {
    'crypto': 'data/global_crypto/1MINUTE_BARS/2023-06-09.csv',
    'us_stocks': 'data/us_stocks_sip/1MINUTE_BARS/2023-06-09.csv',
    'us_indices': 'data/us_indices/1MINUTE_BARS/2023-06-09.csv'
}

for name, filepath in datasets.items():
    with open(filepath, 'r') as f:
        header = f.readline().strip()
    print(f"{name:12} : {header}")

# 2. Why US Indices lack volume/transactions - Market Structure
print("\n\n2. MARKET STRUCTURE EXPLANATION:")
print("-"*70)

# Load sample data to demonstrate
indices_df = pd.read_csv(datasets['us_indices'], nrows=100)
stocks_df = pd.read_csv(datasets['us_stocks'], nrows=100)

print("US STOCKS (tradeable securities):")
print("  - Each row represents actual trades that occurred")
print("  - Volume = number of shares traded in that minute")
print("  - Transactions = number of individual trades")
print(f"\n  Example: {stocks_df.iloc[0]['ticker']}")
print(f"    Volume: {stocks_df.iloc[0]['volume']:,} shares traded")
print(f"    Transactions: {stocks_df.iloc[0]['transactions']} trades executed")
print(f"    OHLC: ${stocks_df.iloc[0]['open']:.2f} / ${stocks_df.iloc[0]['high']:.2f} / ${stocks_df.iloc[0]['low']:.2f} / ${stocks_df.iloc[0]['close']:.2f}")

print("\n\nUS INDICES (calculated values):")
print("  - Each row represents a calculated index value")
print("  - No volume because indices aren't directly traded")
print("  - No transactions because it's a mathematical calculation")
print(f"\n  Example: {indices_df.iloc[0]['ticker']}")
print(f"    Index calculation: {indices_df.iloc[0]['close']:.4f}")
print("    Based on weighted average of constituent stocks")

# 3. Technical implementation in pandas
print("\n\n3. PANDAS TECHNICAL IMPLEMENTATION:")
print("-"*70)

print("\nWhen pandas reads the CSV headers:")
print("```python")
print("# For stocks/crypto - 8 columns detected")
print("df = pd.read_csv('us_stocks.csv')")
print("df.columns.tolist()")
print(f"# Returns: {stocks_df.columns.tolist()}")
print("\n# For indices - 6 columns detected")
print("df = pd.read_csv('us_indices.csv')")
print("df.columns.tolist()")
print(f"# Returns: {indices_df.columns.tolist()}")
print("```")

# 4. Why this matters for aggregation
print("\n\n4. AGGREGATION IMPLICATIONS:")
print("-"*70)

print("\nThe schema discovery found these differences because:")
print("\n a) CSV Parser Detection:")
print("    - pd.read_csv() automatically detects column count from header row")
print("    - Indices files literally have 2 fewer columns in the header")

print("\n b) Data Type Detection:")
stocks_dtypes = stocks_df.dtypes
indices_dtypes = indices_df.dtypes

print("\n    US Stocks dtypes:")
for col, dtype in stocks_dtypes.items():
    print(f"      {col:15} : {dtype}")

print("\n    US Indices dtypes:")
for col, dtype in indices_dtypes.items():
    print(f"      {col:15} : {dtype}")

print("\n c) Aggregation Code Must Handle:")
print("    - Dynamic column detection: 'if \"volume\" in df.columns:'")
print("    - Conditional aggregation rules based on available columns")
print("    - No volume summation for indices (would cause KeyError)")

# 5. Memory layout comparison
print("\n\n5. MEMORY LAYOUT COMPARISON:")
print("-"*70)

stocks_memory = stocks_df.memory_usage(deep=True).sum()
indices_memory = indices_df.memory_usage(deep=True).sum()

print(f"\nMemory per 100 rows:")
print(f"  US Stocks (8 cols): {stocks_memory:,} bytes")
print(f"  US Indices (6 cols): {indices_memory:,} bytes")
print(f"  Difference: {stocks_memory - indices_memory:,} bytes ({(stocks_memory - indices_memory) / stocks_memory * 100:.1f}% less)")

# 6. Show actual missing column behavior
print("\n\n6. MISSING COLUMN BEHAVIOR:")
print("-"*70)

print("\nAttempting to access missing columns:")
try:
    print("indices_df['volume'].sum()  # This will fail")
    result = indices_df['volume'].sum()
except KeyError as e:
    print(f"KeyError: {e}")
    print("This is why our code checks 'if \"volume\" in df.columns' first!")

# 7. File structure verification
print("\n\n7. FILE STRUCTURE VERIFICATION:")
print("-"*70)

for name, filepath in datasets.items():
    df_check = pd.read_csv(filepath, nrows=0)  # Just headers
    print(f"\n{name}:")
    print(f"  Column count: {len(df_check.columns)}")
    print(f"  Columns: {', '.join(df_check.columns)}")
    
    # Check if it's a proper CSV
    with open(filepath, 'rb') as f:
        first_bytes = f.read(100)
    print(f"  First 100 bytes: {first_bytes[:50]}...")
    print(f"  Delimiter confirmed: {',' in str(first_bytes)}")
