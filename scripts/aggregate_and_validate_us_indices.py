#!/usr/bin/env python3
import pandas as pd
import numpy as np
import random
from pathlib import Path
import logging

# ─── CONFIG ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent / "data" / "us_indices"
IN_DIR  = ROOT / "1MINUTE_BARS"
INTERVALS = [5, 15, 30, 60]
SAMPLE_SIZE = 10  # how many random rows to validate per file/interval

# ─── LOGGER SETUP ──────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(message)s")

# ─── AGGREGATION FUNCTION ──────────────────────────────────────────────────────
def aggregate_per_ticker(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    """
    Given a multi-ticker 1-min DataFrame with 'window_start' in ns,
    resample each ticker into N-min bars and return a concatenated DataFrame.
    """
    df = df.copy()
    # convert to datetime index
    df['ts'] = pd.to_datetime(df['window_start'], unit='ns')
    df.set_index('ts', inplace=True)

    out = []
    for ticker, grp in df.groupby('ticker', sort=False):
        agg = grp.resample(f"{minutes}min", label='left', closed='left').agg({
            'open' : 'first',
            'high' : 'max',
            'low'  : 'min',
            'close': 'last'
        })
        # indices data has no volume/transactions
        agg['ticker'] = ticker
        out.append(agg)

    result = pd.concat(out).reset_index().rename(columns={'ts':'window_start'})
    # back to int ns, and reorder columns
    result['window_start'] = result['window_start'].astype(np.int64)
    return result[['ticker','window_start','open','high','low','close']]

# ─── VALIDATION FUNCTION ───────────────────────────────────────────────────────
def validate_spot_checks(orig: pd.DataFrame, agg: pd.DataFrame, minutes: int):
    """
    Sample SAMPLE_SIZE random rows from orig, locate their interval in agg,
    and verify OHLC match the underlying window.
    """
    # prepare timestamps
    orig = orig.copy()
    orig['ts'] = pd.to_datetime(orig['window_start'], unit='ns')

    # pick SAMPLE_SIZE random indices
    rows = orig.sample(min(SAMPLE_SIZE, len(orig)), random_state=42)

    for _, row in rows.iterrows():
        t0 = row['ts']
        # compute window start aligned to 'left' closed
        # floor minute: t0 rounded down to nearest multiple of minutes
        floored = (t0.floor(f'{minutes}min'))
        # filter original data in [floored, floored + minutes)
        mask = (orig['ticker'] == row['ticker']) & \
               (orig['ts'] >= floored) & (orig['ts'] < floored + pd.Timedelta(minutes=minutes))
        window = orig.loc[mask]

        # find the corresponding aggregated row
        match = agg[
            (agg['ticker'] == row['ticker']) &
            (agg['window_start'] == int(floored.value))
        ]
        if match.empty or len(window)==0:
            logging.error(f"  ✖ No data for {row['ticker']} @ {floored} in {minutes}min agg")
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
                logging.error(f"  ✖ {minutes}min {name} mismatch for {row['ticker']}@{floored}: got {got}, exp {exp}")

# ─── MAIN PROCESS ──────────────────────────────────────────────────────────────
def main():
    if not IN_DIR.exists():
        logging.error(f"Input folder not found: {IN_DIR}")
        return

    # ensure output folders
    OUT_DIRS = {n: ROOT / f"{n}MINUTE_BARS" for n in INTERVALS}
    for p in OUT_DIRS.values():
        p.mkdir(parents=True, exist_ok=True)

    # process each 1-min CSV
    csv_files = sorted(IN_DIR.glob("*.csv"))
    logging.info(f"Found {len(csv_files)} CSV files to process")
    
    for i, csv in enumerate(csv_files):
        logging.info(f"\n=== [{i+1}/{len(csv_files)}] Processing {csv.name} ===")
        df1 = pd.read_csv(csv)

        # ensure ticker & window_start exist
        if 'ticker' not in df1.columns or 'window_start' not in df1.columns:
            logging.error(f"Missing required columns in {csv.name}, skipping.")
            continue

        # for each interval: aggregate, write, and validate
        for n in INTERVALS:
            try:
                dfn = aggregate_per_ticker(df1, n)
                out_csv = OUT_DIRS[n] / csv.name
                dfn.to_csv(out_csv, index=False)
                logging.info(f"  ✔ {n}min → {out_csv.name} ({len(dfn)} rows)")
                # spot-check validation
                validate_spot_checks(df1, dfn, n)
            except Exception as e:
                logging.error(f"  ✖ Error at {n}min: {e}")

if __name__ == "__main__":
    main()
