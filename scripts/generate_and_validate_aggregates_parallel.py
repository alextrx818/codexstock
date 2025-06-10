#!/usr/bin/env python3
import pandas as pd
import numpy as np
from pathlib import Path
import random
import logging
from multiprocessing import Pool, cpu_count
import os
import sys

# --- CONFIGURATION ---
DATASETS = [
    "data/global_crypto",
    "data/us_stocks_sip",
    "data/us_indices",
]
INPUT_SUBDIR  = "1MINUTE_BARS"
OUTPUT_SUBDIR = "{n}MINUTE_BARS"   # will format with n=5,15,30,60
INTERVALS     = [5, 15, 30, 60]
SAMPLE_CHECKS = 5  # how many random bars to validate per interval

# Setup a simple logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(processName)s - %(message)s")

def aggregate_bars(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    """Aggregate a multi-ticker 1-min DataFrame into N-min bars."""
    # prepare index
    df = df.copy()
    df['ts'] = pd.to_datetime(df['window_start'], unit='ns')
    df = df.set_index('ts')
    
    # Build aggregation dict based on available columns
    agg_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last'
    }
    
    # Only add volume and transactions if they exist in the dataframe
    if 'volume' in df.columns:
        agg_dict['volume'] = 'sum'
    if 'transactions' in df.columns:
        agg_dict['transactions'] = 'sum'
    
    # group‐resample per ticker
    aggs = []
    for ticker, grp in df.groupby('ticker', sort=False):
        tmp = grp.resample(f"{minutes}min", label='left', closed='left').agg(agg_dict).dropna(subset=['open'])
        tmp['ticker'] = ticker
        aggs.append(tmp)
    
    if not aggs:
        # Return empty dataframe with correct columns if no data
        return pd.DataFrame(columns=['window_start', 'ticker'] + list(agg_dict.keys()))
    
    result = pd.concat(aggs).reset_index().rename(columns={'ts':'window_start'})
    # back to nanoseconds int
    result['window_start'] = result['window_start'].astype(np.int64)
    return result

def validate_sample(orig: pd.DataFrame, agg: pd.DataFrame, minutes: int):
    """Pick SAMPLE_CHECKS random bars from `agg` and verify sums against `orig`."""
    if len(agg) == 0:
        return
        
    # convert timestamp back to datetime for slicing
    agg_dt = pd.to_datetime(agg['window_start'], unit='ns')
    agg = agg.assign(ts=agg_dt)
    orig_dt = pd.to_datetime(orig['window_start'], unit='ns')
    orig = orig.assign(ts=orig_dt)
    
    # pick SAMPLE_CHECKS random rows
    rows = agg.sample(min(SAMPLE_CHECKS, len(agg)), random_state=0)
    for _, row in rows.iterrows():
        t0 = row['ts']
        t1 = t0 + pd.Timedelta(minutes=minutes)
        mask = (orig['ticker'] == row['ticker']) & (orig['ts'] >= t0) & (orig['ts'] < t1)
        window = orig.loc[mask]
        
        if len(window) == 0:
            continue
            
        # quick checks
        assert window['open'].iloc[0] == row['open'],   f"Open mismatch at {row['ticker']}@{t0}"
        assert window['close'].iloc[-1] == row['close'],f"Close mismatch at {row['ticker']}@{t0}"
        assert window['high'].max() == row['high'],     f"High mismatch at {row['ticker']}@{t0}"
        assert window['low'].min() == row['low'],       f"Low mismatch at {row['ticker']}@{t0}"
        
        # Only check volume if it exists
        if 'volume' in window.columns and 'volume' in row:
            vol_sum = window['volume'].sum()
            assert abs(vol_sum - row['volume']) < 1e-6, f"Volume mismatch at {row['ticker']}@{t0}"

def process_single_file(args):
    """Process a single CSV file - this will be called by each worker process"""
    csv_path, ds_name, out_dirs = args
    
    try:
        logging.info(f"Processing {ds_name} / {csv_path.name}")
        df1 = pd.read_csv(csv_path)
        
        results = []
        for n in INTERVALS:
            try:
                dfn = aggregate_bars(df1, n)
                target = out_dirs[n] / csv_path.name
                
                # Check if file already exists (for resuming)
                if target.exists():
                    logging.info(f"   ⏭️  {n}-min already exists, skipping")
                    results.append((n, "skipped"))
                    continue
                
                dfn.to_csv(target, index=False)
                # spot‐check validation
                validate_sample(df1, dfn, n)
                logging.info(f"   ✔ {n}-min ({len(dfn)} rows)")
                results.append((n, "success"))
            except AssertionError as e:
                logging.error(f"   ✖ Validation failed for {n}-min: {e}")
                results.append((n, f"validation_error: {e}"))
            except Exception as e:
                logging.error(f"   ✖ Error in {n}-min: {e}")
                results.append((n, f"error: {e}"))
        
        return csv_path.name, results
    except Exception as e:
        logging.error(f"Failed to process {csv_path.name}: {e}")
        return csv_path.name, [(n, f"file_error: {e}") for n in INTERVALS]

def main():
    project_root = Path(__file__).parent.parent
    
    # Get number of CPUs
    num_cpus = cpu_count()
    logging.info(f"Using {num_cpus} CPU cores for parallel processing")
    
    for ds in DATASETS:
        # Skip already completed datasets
        if ds == "data/global_crypto" or ds == "data/us_indices":
            logging.info(f"\nSkipping {ds} - already 100% complete")
            continue
            
        in_dir = project_root / ds / INPUT_SUBDIR
        if not in_dir.exists():
            logging.warning(f"Skipping missing folder {in_dir}")
            continue

        # create output folders
        out_dirs = {
            n: project_root / ds / OUTPUT_SUBDIR.format(n=n)
            for n in INTERVALS
        }
        for p in out_dirs.values():
            p.mkdir(parents=True, exist_ok=True)

        # Get list of files to process
        csv_files = sorted(in_dir.glob("*.csv"))
        
        # Filter to only process files from 2024-09-20 onwards
        start_file = "2024-09-20.csv"
        csv_files = [f for f in csv_files if f.name >= start_file]
        
        if not csv_files:
            logging.info(f"\nNo files to process in {ds}")
            continue
            
        logging.info(f"\nProcessing {len(csv_files)} files in {ds} starting from {start_file}")
        logging.info(f"Files range: {csv_files[0].name} to {csv_files[-1].name}")
        
        # Prepare arguments for parallel processing
        process_args = [(csv, ds, out_dirs) for csv in csv_files]
        
        # Process files in parallel
        with Pool(processes=num_cpus) as pool:
            results = pool.map(process_single_file, process_args)
        
        # Summary
        logging.info(f"\n{'='*60}")
        logging.info(f"SUMMARY for {ds}:")
        logging.info(f"{'='*60}")
        
        success_count = {n: 0 for n in INTERVALS}
        error_count = {n: 0 for n in INTERVALS}
        skip_count = {n: 0 for n in INTERVALS}
        
        for filename, interval_results in results:
            for interval, status in interval_results:
                if status == "success":
                    success_count[interval] += 1
                elif status == "skipped":
                    skip_count[interval] += 1
                else:
                    error_count[interval] += 1
        
        for n in INTERVALS:
            total = len(csv_files)
            logging.info(f"{n}-minute bars: {success_count[n]} success, {skip_count[n]} skipped, {error_count[n]} errors (out of {total} files)")
        
        logging.info(f"{'='*60}\n")

if __name__ == "__main__":
    # Add a check to see if we're resuming
    if len(sys.argv) > 1 and sys.argv[1] == "--check-status":
        project_root = Path(__file__).parent.parent
        ds = "data/us_stocks_sip"
        for n in INTERVALS:
            out_dir = project_root / ds / f"{n}MINUTE_BARS"
            if out_dir.exists():
                count = len(list(out_dir.glob("*.csv")))
                print(f"{n}-minute bars: {count} files")
    else:
        main()
