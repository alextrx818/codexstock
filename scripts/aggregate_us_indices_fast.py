#!/usr/bin/env python3
"""
Optimized aggregation for US indices - handles massive files efficiently
"""
import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime

# CONFIG
ROOT = Path(__file__).parent.parent / "data" / "us_indices"
IN_DIR = ROOT / "1MINUTE_BARS"
INTERVALS = [5, 15, 30, 60]

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

def aggregate_optimized(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    """
    Optimized aggregation using vectorized operations
    """
    # Convert timestamp once
    df['ts'] = pd.to_datetime(df['window_start'], unit='ns')
    
    # Create aggregation window (floor to nearest N minutes)
    df['agg_window'] = df['ts'].dt.floor(f'{minutes}min')
    
    # Use optimized groupby with observed=True to avoid creating empty groups
    agg = df.groupby(['ticker', 'agg_window'], observed=True).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last'
    }).reset_index()
    
    # Convert back to nanoseconds
    agg['window_start'] = agg['agg_window'].astype(np.int64)
    
    # Return in correct column order
    return agg[['ticker', 'window_start', 'open', 'high', 'low', 'close']]

def process_file_chunked(csv_path: Path, out_dirs: dict):
    """
    Process large file in chunks to manage memory
    """
    # Read file info
    file_size_mb = csv_path.stat().st_size / 1024 / 1024
    logging.info(f"\nProcessing {csv_path.name} ({file_size_mb:.1f} MB)")
    
    # For very large files, we could use chunks, but for now let's load it all
    # since we have enough memory
    start = datetime.now()
    df = pd.read_csv(csv_path)
    load_time = (datetime.now() - start).total_seconds()
    logging.info(f"  Loaded {len(df):,} rows in {load_time:.1f}s")
    
    # Process each interval
    for minutes in INTERVALS:
        try:
            start = datetime.now()
            agg_df = aggregate_optimized(df, minutes)
            agg_time = (datetime.now() - start).total_seconds()
            
            # Write output
            out_path = out_dirs[minutes] / csv_path.name
            agg_df.to_csv(out_path, index=False)
            
            logging.info(f"  ✓ {minutes:2d}-min: {len(agg_df):,} rows in {agg_time:.1f}s → {out_path.name}")
            
        except Exception as e:
            logging.error(f"  ✗ {minutes}-min failed: {e}")

def main():
    if not IN_DIR.exists():
        logging.error(f"Input directory not found: {IN_DIR}")
        return
    
    # Create output directories
    out_dirs = {n: ROOT / f"{n}MINUTE_BARS" for n in INTERVALS}
    for p in out_dirs.values():
        p.mkdir(parents=True, exist_ok=True)
    
    # Get all CSV files
    csv_files = sorted(IN_DIR.glob("*.csv"))
    logging.info(f"Found {len(csv_files)} files to process")
    
    # Process each file
    total_start = datetime.now()
    for i, csv_path in enumerate(csv_files):
        logging.info(f"\n[{i+1}/{len(csv_files)}] {csv_path.name}")
        process_file_chunked(csv_path, out_dirs)
    
    total_time = (datetime.now() - total_start).total_seconds()
    logging.info(f"\n✓ Completed all files in {total_time/60:.1f} minutes")

if __name__ == "__main__":
    main()
