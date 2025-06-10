#!/usr/bin/env python3
import pandas as pd
import numpy as np
from pathlib import Path
import random
import logging
from multiprocessing import Pool, cpu_count
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
import sys
import gc

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
CHUNK_SIZE = 100000  # for chunked reading of large files
NUM_WORKERS = min(32, cpu_count())  # Limit workers to available CPUs

# Setup a simple logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(processName)s - %(message)s")

def aggregate_bars_optimized(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    """Optimized aggregation of multi-ticker 1-min DataFrame into N-min bars."""
    # Early termination if empty
    if df.empty:
        return pd.DataFrame()
    
    # No copy needed - we're working with fresh data
    # Convert timestamp inline during set_index
    df['ts'] = pd.to_datetime(df['window_start'], unit='ns')
    df = df.set_index('ts')
    
    # Build aggregation dict based on available columns
    agg_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last'
    }
    
    # Only add volume and transactions if they exist
    if 'volume' in df.columns:
        agg_dict['volume'] = 'sum'
    if 'transactions' in df.columns:
        agg_dict['transactions'] = 'sum'
    
    # group‐resample per ticker
    aggs = []
    for ticker, grp in df.groupby('ticker', sort=False):
        tmp = grp.resample(f"{minutes}min", label='left', closed='left').agg(agg_dict).dropna(subset=['open'])
        if not tmp.empty:  # Only append non-empty results
            tmp['ticker'] = ticker
            aggs.append(tmp)
    
    # Early termination for empty results
    if not aggs:
        return pd.DataFrame()
    
    result = pd.concat(aggs).reset_index().rename(columns={'ts':'window_start'})
    # Convert timestamp back to int64 only during save
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

def process_single_interval(csv_path, interval, out_dir):
    """Process a single interval for a CSV file"""
    target = out_dir / csv_path.name
    
    # Smart caching - skip if file already exists
    if target.exists():
        return interval, "skipped", 0
    
    try:
        # Single optimized read with pyarrow engine if available
        try:
            df = pd.read_csv(csv_path, engine='pyarrow')
        except:
            df = pd.read_csv(csv_path)
        
        # Aggregate
        df_agg = aggregate_bars_optimized(df, interval)
        
        # Early termination for empty results
        if df_agg.empty:
            return interval, "empty", 0
        
        # Save without unnecessary conversions
        df_agg.to_csv(target, index=False)
        
        # Validation
        validate_sample(df, df_agg, interval)
        
        rows = len(df_agg)
        
        # Explicit memory cleanup
        del df
        del df_agg
        gc.collect()
        
        return interval, "success", rows
        
    except AssertionError as e:
        return interval, f"validation_error: {e}", 0
    except Exception as e:
        return interval, f"error: {e}", 0

def process_file_batch(file_batch, ds_name, out_dirs):
    """Process a batch of files - for file-level parallelism"""
    results = []
    
    for csv_path in file_batch:
        logging.info(f"Processing {ds_name} / {csv_path.name}")
        file_results = []
        
        for interval in INTERVALS:
            interval_result = process_single_interval(csv_path, interval, out_dirs[interval])
            status_type, status_msg, rows = interval_result
            
            if status_msg == "success":
                logging.info(f"   ✔ {interval}-min ({rows} rows)")
            elif status_msg == "skipped":
                logging.info(f"   ⏭️  {interval}-min already exists")
            elif status_msg == "empty":
                logging.info(f"   ⚠️  {interval}-min empty result")
            else:
                logging.error(f"   ✖ {interval}-min: {status_msg}")
            
            file_results.append(interval_result)
        
        results.append((csv_path.name, file_results))
    
    return results

def main():
    project_root = Path(__file__).parent.parent
    
    logging.info(f"Using {NUM_WORKERS} workers for parallel processing")
    
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
        
        # Resume from last completed file
        # Check which files are fully aggregated
        last_completed = None
        for f in csv_files:
            if all((out_dirs[i] / f.name).exists() for i in INTERVALS):
                last_completed = f.name
            else:
                break
        
        if last_completed:
            logging.info(f"Last fully completed file: {last_completed}")
            # Start from the next file
            csv_files = [f for f in csv_files if f.name > last_completed]
        
        if not csv_files:
            logging.info(f"\nNo files to process in {ds} - all completed!")
            continue
            
        logging.info(f"\nProcessing {len(csv_files)} remaining files in {ds}")
        logging.info(f"Starting from: {csv_files[0].name}")
        
        # Split files into batches for parallel processing
        batch_size = max(1, len(csv_files) // NUM_WORKERS)
        file_batches = []
        
        for i in range(0, len(csv_files), batch_size):
            batch = csv_files[i:i + batch_size]
            if batch:
                file_batches.append(batch)
        
        # Process batches in parallel
        all_results = []
        with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
            futures = []
            for batch in file_batches:
                future = executor.submit(process_file_batch, batch, ds, out_dirs)
                futures.append(future)
            
            for future in as_completed(futures):
                try:
                    batch_results = future.result()
                    all_results.extend(batch_results)
                except Exception as e:
                    logging.error(f"Batch processing failed: {e}")
        
        # Summary
        logging.info(f"\n{'='*60}")
        logging.info(f"SUMMARY for {ds}:")
        logging.info(f"{'='*60}")
        
        success_count = {n: 0 for n in INTERVALS}
        error_count = {n: 0 for n in INTERVALS}
        skip_count = {n: 0 for n in INTERVALS}
        empty_count = {n: 0 for n in INTERVALS}
        
        for filename, interval_results in all_results:
            for interval, status, rows in interval_results:
                if status == "success":
                    success_count[interval] += 1
                elif status == "skipped":
                    skip_count[interval] += 1
                elif status == "empty":
                    empty_count[interval] += 1
                else:
                    error_count[interval] += 1
        
        for n in INTERVALS:
            total = len(csv_files)
            logging.info(f"{n}-minute bars: {success_count[n]} new, {skip_count[n]} skipped, "
                        f"{empty_count[n]} empty, {error_count[n]} errors (out of {total} files)")
        
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
