import pandas as pd

# Read both files
minute_file = "/root/stock_project/data/us_stocks_sip/1MINUTE_BARS/2023-06-26.csv"
fifteen_file = "/root/stock_project/data/us_stocks_sip/15MINUTE_BARS/2023-06-26.csv"

# Read 1-minute data
df_1min = pd.read_csv(minute_file)
# Read 15-minute data with no header
df_15min = pd.read_csv(fifteen_file, header=None)

# Print column structure
print("1-minute file columns:", df_1min.columns.tolist())
print("\n15-minute file shape:", df_15min.shape)
print("\nFirst few rows of 15-minute file:")
print(df_15min.head())

# Filter for ticker A in 1-minute data
ticker_1min = df_1min[df_1min['ticker'] == 'A'].copy()

# Convert timestamps to datetime for grouping
ticker_1min['datetime'] = pd.to_datetime(ticker_1min['window_start'], unit='ns')
ticker_1min['15min_group'] = ticker_1min['datetime'].dt.floor('15min')

# Calculate what 15-minute aggregation should be
print("\n\nVerifying first 15-minute period:")
first_group = ticker_1min.groupby('15min_group').first().iloc[0]
print(f"Time period: {first_group.name}")

# Get all 1-minute bars in first 15-minute period
first_15min_bars = ticker_1min[ticker_1min['15min_group'] == first_group.name]
print(f"\nFound {len(first_15min_bars)} 1-minute bars in first 15-minute period")

# Calculate aggregated values
expected_open = first_15min_bars.iloc[0]['open']
expected_close = first_15min_bars.iloc[-1]['close']
expected_high = first_15min_bars['high'].max()
expected_low = first_15min_bars['low'].min()
expected_volume = first_15min_bars['volume'].sum()
expected_transactions = first_15min_bars['transactions'].sum()

print(f"\nExpected 15-minute bar values:")
print(f"  Open: {expected_open}")
print(f"  Close: {expected_close}")
print(f"  High: {expected_high}")
print(f"  Low: {expected_low}")
print(f"  Volume: {expected_volume}")
print(f"  Transactions: {expected_transactions}")

# Compare with actual 15-minute bar (assuming it's in row 2)
if len(df_15min) > 2:
    actual_row = df_15min.iloc[2]
    print(f"\nActual 15-minute bar values (row 2):")
    print(f"  Row data: {actual_row.values}")
    
    # The columns seem to be: open, close, high, low, volume, transactions, timestamp, ticker, timestamp2
    print(f"\n  Open: {actual_row[0]} (Match: {actual_row[0] == expected_open})")
    print(f"  Close: {actual_row[1]} (Match: {actual_row[1] == expected_close})")
    print(f"  High: {actual_row[2]} (Match: {actual_row[2] == expected_high})")
    print(f"  Low: {actual_row[3]} (Match: {actual_row[3] == expected_low})")
    print(f"  Volume: {actual_row[4]} (Match: {int(actual_row[4]) == expected_volume})")
    print(f"  Transactions: {actual_row[5]} (Match: {int(actual_row[5]) == expected_transactions})")
