#!/usr/bin/env python3
"""
GENERATE AND VALIDATE AGGREGATES
Processes 1-minute bars into 5,15,30,60-minute bars with validation
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
import logging
import pytz
from multiprocessing import Pool, cpu_count

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='aggregation.log',
    filemode='w'
)
logger = logging.getLogger(__name__)

# Configuration
BASE_PATH = "/root/stock_project/data"
DATASETS = ["global_crypto", "us_stocks_sip", "us_indices"]
INTERVALS = [5, 15, 30, 60]

# Timezone configuration
TIMEZONES = {
    "global_crypto": "UTC",
    "us_stocks_sip": "America/New_York",
    "us_indices": "America/New_York"
}

# Trading hours configuration
TRADING_HOURS = {
    "us_stocks_sip": ("09:30", "16:00"),
    "us_indices": ("09:30", "16:00"),
    "global_crypto": ("00:00", "23:59")  # 24/7
}

# Tolerance configuration
ABSOLUTE_TOL = 1e-8
RELATIVE_TOL = 0.01  # 1% tolerance for volume

# Use all 32 CPUs
NUM_CORES = 32

def load_data(file_path, dataset):
    """Load data with automatic timestamp column detection"""
    df = pd.read_csv(file_path)
    
    # Detect timestamp column
    candidates = [col for col in df.columns 
                if 'time' in col.lower() or 'date' in col.lower() or 'window' in col.lower()]
    
    if not candidates:
        raise KeyError(f"No timestamp-like column found in {file_path}")
    
    time_col = candidates[0]
    
    # Convert to timezone-aware datetime
    tz = TIMEZONES[dataset]
    df['timestamp'] = pd.to_datetime(df[time_col], utc=True)
    df['timestamp'] = df['timestamp'].dt.tz_convert(tz)
    
    # Drop original time column if different
    if time_col != 'timestamp':
        df.drop(columns=[time_col], inplace=True)
    
    return df

def is_trading_hour(timestamp, dataset):
    """Check if timestamp is within trading hours"""
    start, end = TRADING_HOURS[dataset]
    time_val = timestamp.time()
    
    # Handle DST transitions
    if dataset in ['us_stocks_sip', 'us_indices']:
        # Allow partial bars at session edges
        return time_val >= pd.to_datetime(start).time() and \
               time_val <= pd.to_datetime(end).time()
    return True  # 24/7 for crypto

def aggregate_bars(df, interval, dataset):
    """Aggregate 1-minute bars to target interval"""
    # Set timestamp as index
    df = df.set_index('timestamp')
    
    # Resample to target interval
    agg_df = df.groupby('ticker').resample(f"{interval}min").agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    })
    
    # Reset index and filter partial bars
    agg_df = agg_df.reset_index()
    
    # Filter partial bars for stocks
    if dataset in ['us_stocks_sip', 'us_indices']:
        # Only keep complete bars within trading hours
        agg_df = agg_df[agg_df['timestamp'].apply(
            lambda x: is_trading_hour(x, dataset)
        )]
    
    return agg_df

def validate_aggregation(one_min_df, agg_df, interval, dataset):
    """Validate aggregated bars against 1-minute data"""
    # Select random tickers for validation
    tickers = np.random.choice(one_min_df['ticker'].unique(), 
                              min(5, len(one_min_df['ticker'].unique())), 
                              replace=False)
    
    for ticker in tickers:
        # Filter data for selected ticker
        ticker_1min = one_min_df[one_min_df['ticker'] == ticker]
        ticker_agg = agg_df[agg_df['ticker'] == ticker]
        
        for _, agg_row in ticker_agg.iterrows():
            # Get corresponding 1-min bars
            start = agg_row['timestamp'] - pd.Timedelta(minutes=interval-1)
            end = agg_row['timestamp']
            
            # Handle DST transitions (59/61 minute intervals)
            actual_interval = (end - start).total_seconds() / 60
            if abs(actual_interval - interval) > 1:
                continue  # Skip DST transition bars
            
            mask = (ticker_1min['timestamp'] >= start) & \
                   (ticker_1min['timestamp'] <= end)
            
            one_min_bars = ticker_1min[mask]
            
            if len(one_min_bars) == 0:
                continue
            
            # Validate OHLC
            if not np.isclose(agg_row['open'], one_min_bars.iloc[0]['open'], 
                             atol=ABSOLUTE_TOL):
                logger.warning(f"Open mismatch: {agg_row['open']} vs {one_min_bars.iloc[0]['open']}")
            
            if not np.isclose(agg_row['high'], one_min_bars['high'].max(), 
                             atol=ABSOLUTE_TOL):
                logger.warning(f"High mismatch: {agg_row['high']} vs {one_min_bars['high'].max()}")
            
            if not np.isclose(agg_row['low'], one_min_bars['low'].min(), 
                             atol=ABSOLUTE_TOL):
                logger.warning(f"Low mismatch: {agg_row['low']} vs {one_min_bars['low'].min()}")
            
            if not np.isclose(agg_row['close'], one_min_bars.iloc[-1]['close'], 
                             atol=ABSOLUTE_TOL):
                logger.warning(f"Close mismatch: {agg_row['close']} vs {one_min_bars.iloc[-1]['close']}")
            
            # Validate volume with relative tolerance
            expected_volume = one_min_bars['volume'].sum()
            if not np.isclose(agg_row['volume'], expected_volume, 
                             rtol=RELATIVE_TOL, atol=ABSOLUTE_TOL):
                logger.warning(f"Volume mismatch: {agg_row['volume']} vs {expected_volume}")

def process_file(file, dataset):
    """Process a single file with all intervals"""
    logger.info(f"Processing file: {file.name}")
    
    # Load 1-minute data
    one_min_df = load_data(file, dataset)
    
    for interval in INTERVALS:
        # Create output directory
        output_path = Path(BASE_PATH) / dataset / f"{interval}MINUTE_BARS"
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Generate aggregated bars
        agg_df = aggregate_bars(one_min_df, interval, dataset)
        
        # Save aggregated data
        output_file = output_path / file.name
        agg_df.to_csv(output_file, index=False)
        
        # Validate aggregation
        validate_aggregation(one_min_df, agg_df, interval, dataset)

def process_dataset(dataset):
    """Process all files for a dataset in parallel"""
    logger.info(f"Processing dataset: {dataset}")
    
    input_path = Path(BASE_PATH) / dataset / "1MINUTE_BARS"
    files = list(input_path.glob("*.csv"))
    
    # Create multiprocessing pool
    with Pool(processes=NUM_CORES) as pool:
        args = [(file, dataset) for file in files]
        pool.starmap(process_file, args)

if __name__ == "__main__":
    for dataset in DATASETS:
        process_dataset(dataset)
    logger.info("Aggregation complete!")
