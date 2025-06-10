import os
import pandas as pd
from termcolor import colored

# Define the intervals for aggregation
INTERVALS = {'15MINUTE': 15, '30MINUTE': 30, '60MINUTE': 60}

# Function to aggregate 1-minute bars to specified intervals
def aggregate_bars(input_file, output_dirs):
    # Read the 1-minute data
    df = pd.read_csv(input_file)

    # Check for necessary columns
    required_columns = ['volume', 'transactions']
    if not all(column in df.columns for column in required_columns):
        print(colored(f'Skipping {input_file}: Missing columns', 'red'))
        return
    
    # Ensure the timestamp is in datetime format
    df['window_start'] = pd.to_datetime(df['window_start'], unit='ns')
    df.set_index('window_start', inplace=True)

    for label, minutes in INTERVALS.items():
        # Resample the data
        resampled = df.resample(f'{minutes}min').agg({
            'ticker': 'first',
            'volume': 'sum',
            'open': 'first',
            'close': 'last',
            'high': 'max',
            'low': 'min',
            'transactions': 'sum'
        }).dropna()

        # Write to CSV
        output_file = os.path.join(output_dirs[label], os.path.basename(input_file))
        resampled.to_csv(output_file)
        print(colored(f'Successfully aggregated to {label} and saved to {output_file}', 'green'))

# Example usage
if __name__ == "__main__":
    # Define the input and output directories
    input_dir = '/root/stock_project/data/us_indices/1MINUTE_BARS/'
    output_dirs = {
        '15MINUTE': '/root/stock_project/data/us_indices/15MINUTE_BARS/',
        '30MINUTE': '/root/stock_project/data/us_indices/30MINUTE_BARS/',
        '60MINUTE': '/root/stock_project/data/us_indices/60MINUTE_BARS/'
    }
    
    # Process each file
    for filename in os.listdir(input_dir):
        if filename.endswith('.csv'):
            input_file = os.path.join(input_dir, filename)
            try:
                aggregate_bars(input_file, output_dirs)
            except Exception as e:
                print(colored(f'Failed to aggregate {filename}: {str(e)}', 'red'))
