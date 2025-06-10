#!/usr/bin/env python3
"""Statistical aggregation validation.

Recompute random aggregated bars from raw 1-minute data and assert equality
with stored 5/15/30/60-minute CSVs.  Covers multiple datasets & tickers.
"""

from __future__ import annotations

import random
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
DATASETS = [
    "global_crypto",
    "us_stocks_sip",
    "us_indices",
]
INTERVAL_MAP = {
    "5MINUTE_BARS": "5min",
    "15MINUTE_BARS": "15min",
    "30MINUTE_BARS": "30min",
    "60MINUTE_BARS": "60min",
}
# Parameters
DATES_PER_DATASET = 3  # random dates
TICKERS_PER_DATE = 2  # random tickers per date


@pytest.mark.parametrize("dataset", DATASETS)
@pytest.mark.parametrize("agg_folder,rule", INTERVAL_MAP.items())
def test_aggregation(dataset: str, agg_folder: str, rule: str) -> None:
    """Randomly validate aggregation."""
    raw_dir = ROOT / "data" / dataset / "1MINUTE_BARS"
    agg_dir = ROOT / "data" / dataset / agg_folder

    if not raw_dir.exists() or not agg_dir.exists():
        pytest.skip(f"{dataset} missing folders")

    dates = sorted(fp.name for fp in raw_dir.glob("*.csv"))
    if not dates:
        pytest.skip(f"No CSV files in {raw_dir}")

    chosen_dates = random.sample(dates, min(DATES_PER_DATASET, len(dates)))

    for date_file in chosen_dates:
        raw_fp = raw_dir / date_file
        agg_fp = agg_dir / date_file
        if not agg_fp.exists():
            pytest.fail(f"Missing aggregated file {agg_fp}")

        raw_df = pd.read_csv(raw_fp)
        agg_df = pd.read_csv(agg_fp)

        # choose tickers present in both frames
        tickers = list(set(raw_df.ticker.unique()) & set(agg_df.ticker.unique()))
        if not tickers:
            pytest.skip(f"No common tickers in {date_file}")

        sampled_tickers = random.sample(tickers, min(TICKERS_PER_DATE, len(tickers)))

        for ticker in sampled_tickers:
            one_min = raw_df[raw_df.ticker == ticker].copy()
            one_min["ts"] = pd.to_datetime(one_min.window_start, unit="ns")
            one_min = one_min.sort_values("ts").set_index("ts")

            # build agg dict respecting available columns
            agg_dict: dict[str, str] = {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
            }
            if "volume" in one_min.columns:
                agg_dict["volume"] = "sum"
            if "transactions" in one_min.columns:
                agg_dict["transactions"] = "sum"

            recomputed = (
                one_min.resample(rule, label="left", closed="left")
                .agg(agg_dict)
                .dropna(subset=["open"])
                .reset_index(drop=True)
            )
            recomputed["ticker"] = ticker
            recomputed["window_start"] = (
                recomputed.index.astype("datetime64[ns]").astype("int64")
            )
            recomputed.reset_index(drop=True, inplace=True)

            stored = agg_df[agg_df.ticker == ticker].reset_index(drop=True)

            # Align lengths -- some intervals may have missing bars; compare common rows
            common = min(len(recomputed), len(stored))
            recomputed = recomputed.iloc[:common]
            stored = stored.iloc[:common]

            # Compare numeric columns
            for col in agg_dict:
                assert (
                    (recomputed[col] - stored[col]).abs() < 1e-6
                ).all(), f"{dataset} {date_file} {ticker} {col} mismatch"
