#!/usr/bin/env python3
import pandas as pd
import numpy as np
import random
from pathlib import Path
import logging
from concurrent.futures import ProcessPoolExecutor
import os

# ─── CONFIG ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent / "data" / "us_indices"
IN_DIR = ROOT / "1MINUTE_BARS"
INTERVALS = [5, 15, 30, 60]
SAMPLE_SIZE = 10

# ─── VECTORIZED AGGREGATION ────────────────────────────────────────────────────
def aggregate_all_tickers(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    """
    Vectorized aggregation for all tickers at once - MUCH faster than looping
    """
    # 1) Copy & timestamp
    df = df.copy()
    df['ts'] = pd.to_datetime(df['window_start'], unit='ns')
    
    # 2) MultiIndex for groupby+resample
    df = df.set_index(['ticker','ts']).sort_index()

    # one-step groupby-resample (group_keys=True to preserve ticker in index)
    agg = (
        df
        .groupby(level='ticker', group_keys=True)
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
    
    # 3) reset both index levels back into columns
    agg = agg.reset_index()  # brings back 'ticker' and 'ts'
    agg = agg.rename(columns={'ts':'window_start'})
    
    # 4) convert window_start back to nanosecond int
    agg['window_start'] = agg['window_start'].astype(np.int64)
    
    # 5) final column order
    return agg[['ticker','window_start','open','high','low','close']]

# ─── VALIDATION FUNCTION ───────────────────────────────────────────────────────
def validate_spot_checks(orig: pd.DataFrame, agg: pd.DataFrame, minutes: int, file_name: str):
    """
    Sample random rows and verify aggregation correctness
    """
    # prepare timestamps
    orig = orig.copy()
    orig['ts'] = pd.to_datetime(orig['window_start'], unit='ns')

    # pick random rows
    rows = orig.sample(min(SAMPLE_SIZE, len(orig)), random_state=42)
    errors = []

    for _, row in rows.iterrows():
        t0 = row['ts']
        floored = t0.floor(f'{minutes}min')
        
        # filter original data in window
        mask = (orig['ticker'] == row['ticker']) & \
               (orig['ts'] >= floored) & (orig['ts'] < floored + pd.Timedelta(minutes=minutes))
        window = orig.loc[mask]

        # find corresponding aggregated row
        match = agg[
            (agg['ticker'] == row['ticker']) &
            (agg['window_start'] == int(floored.value))
        ]
        
        if match.empty or len(window)==0:
            continue

        m = match.iloc[0]
        # compute expected values
        exp_open  = window['open'].iloc[0]
        exp_close = window['close'].iloc[-1]
        exp_high  = window['high'].max()
        exp_low   = window['low'].min()

        checks = [
            ("open",  m['open'],  exp_open),
            ("close", m['close'], exp_close),
            ("high",  m['high'],  exp_high),
            ("low",   m['low'],   exp_low),
        ]
        
        for name, got, exp in checks:
            if got != exp:
                errors.append(f"{minutes}min {name} mismatch for {row['ticker']}@{floored}: got {got}, exp {exp}")
    
    return errors

# ─── PER-FILE PROCESSOR ────────────────────────────────────────────────────────
def process_file(csv_path: Path):
    """
    Process a single CSV file - this runs in parallel
    """
    # Set up logging for this process
    logging.basicConfig(level=logging.INFO, format=f"[PID {os.getpid()}] %(message)s")
    
    try:
        logging.info(f"Processing {csv_path.name}")
        df1 = pd.read_csv(csv_path)
        
        # verify required columns
        if 'ticker' not in df1.columns or 'window_start' not in df1.columns:
            logging.error(f"Missing required columns in {csv_path.name}")
            return
        
        # ensure output dirs exist (each process needs to check)
        out_dirs = {n: ROOT / f"{n}MINUTE_BARS" for n in INTERVALS}
        for p in out_dirs.values():
            p.mkdir(parents=True, exist_ok=True)
        
        # process each interval
        for minutes in INTERVALS:
            try:
                # FAST vectorized aggregation
                agg_df = aggregate_all_tickers(df1, minutes)
                
                # write output
                out_csv = out_dirs[minutes] / csv_path.name
                agg_df.to_csv(out_csv, index=False)
                
                # validate
                errors = validate_spot_checks(df1, agg_df, minutes, csv_path.name)
                
                if errors:
                    for err in errors:
                        logging.error(f"  ✖ {err}")
                else:
                    logging.info(f"  ✔ {minutes}min → {out_csv.name} ({len(agg_df)} rows)")
                        
            except Exception as e:
                logging.error(f"  ✖ Error at {minutes}min for {csv_path.name}: {e}")
                
        logging.info(f"✓ Completed {csv_path.name}")
        
    except Exception as e:
        logging.error(f"Failed to process {csv_path.name}: {e}")

# ─── MAIN WITH PARALLEL EXECUTION ─────────────────────────────────────────────
def main():
    if not IN_DIR.exists():
        print(f"Input folder not found: {IN_DIR}")
        return
    
    # get all CSV files
    files = sorted(IN_DIR.glob("*.csv"))
    print(f"Found {len(files)} CSV files to process")
    print(f"Using parallel processing with {os.cpu_count()} CPUs")
    
    # process files in parallel
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        # this will process multiple files simultaneously
        list(executor.map(process_file, files))
    
    print("\n✓ All files processed!")

if __name__ == "__main__":
    main()
