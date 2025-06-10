#!/usr/bin/env python3
"""
Parallel aggregation script for US indices using multiple CPU cores
"""
import pandas as pd
import numpy as np
from pathlib import Path
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import sys

# Configuration
DATASET = "data/us_indices"
INPUT_SUBDIR = "1MINUTE_BARS"
OUTPUT_SUBDIR = "{n}MINUTE_BARS"
INTERVALS = [5, 15, 30, 60]
NUM_WORKERS = 16  # Use 16 cores, leaving plenty for other processes

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def aggregate_bars(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    """Aggregate a multi-ticker 1-min DataFrame into N-min bars."""
    df = df.copy()
    
    # Convert timestamp to datetime
    df['ts'] = pd.to_datetime(df['window_start'], unit='ns')
    df = df.set_index('ts')
    
    # Aggregation dict for indices (no volume/transactions)
    agg_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last'
    }
    
    # Group-resample per ticker
    aggs = []
    for ticker, grp in df.groupby('ticker', sort=False):
        tmp = grp.resample(f"{minutes}min", label='left', closed='left').agg(agg_dict).dropna(subset=['open'])
        tmp['ticker'] = ticker
        aggs.append(tmp)
    
    if not aggs:
        return pd.DataFrame(columns=['window_start', 'ticker'] + list(agg_dict.keys()))
    
    result = pd.concat(aggs).reset_index().rename(columns={'ts': 'window_start'})
    # Convert back to nanoseconds
    result['window_start'] = result['window_start'].astype(np.int64)
    return result

def process_single_file(args):
    """Process a single CSV file for all intervals"""
    csv_path, out_dirs = args
    file_name = csv_path.name
    
    try:
        # Read the 1-minute data once
        df1 = pd.read_csv(csv_path)
        
        results = []
        for interval in INTERVALS:
            # Aggregate to N-minute bars
            dfn = aggregate_bars(df1, interval)
            
            # Save to appropriate directory
            target = out_dirs[interval] / file_name
            dfn.to_csv(target, index=False)
            
            results.append(f"{interval}-min: {len(dfn)} rows")
        
        return f"✓ {file_name}: " + ", ".join(results)
    
    except Exception as e:
        return f"✗ {file_name}: Error - {str(e)}"

def main():
    project_root = Path(__file__).parent.parent
    in_dir = project_root / DATASET / INPUT_SUBDIR
    
    if not in_dir.exists():
        logging.error(f"Input directory not found: {in_dir}")
        sys.exit(1)
    
    # Create output directories
    out_dirs = {}
    for n in INTERVALS:
        out_dir = project_root / DATASET / OUTPUT_SUBDIR.format(n=n)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_dirs[n] = out_dir
    
    # Get all CSV files
    csv_files = sorted(in_dir.glob("*.csv"))
    total_files = len(csv_files)
    
    logging.info(f"Starting parallel aggregation for US indices")
    logging.info(f"Found {total_files} files to process")
    logging.info(f"Using {NUM_WORKERS} worker processes")
    
    # Prepare arguments for parallel processing
    file_args = [(csv_file, out_dirs) for csv_file in csv_files]
    
    # Process files in parallel
    completed = 0
    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        # Submit all tasks
        future_to_file = {executor.submit(process_single_file, args): args[0] 
                          for args in file_args}
        
        # Process completed tasks
        for future in as_completed(future_to_file):
            completed += 1
            result = future.result()
            logging.info(f"[{completed}/{total_files}] {result}")
    
    logging.info(f"\nAggregation complete! Processed {total_files} files.")

if __name__ == "__main__":
    main()
