#!/usr/bin/env python3
"""Schema validation tests using pandera (optional).

We randomly sample a handful of CSVs from every dataset and bar interval and
validate that each DataFrame conforms to a canonical schema (required columns
datatypes, etc.).  If *pandera* is not installed the entire module is skipped.
"""

from __future__ import annotations

import random
from pathlib import Path

import pandas as pd
import pytest

# Skip the whole test suite if pandera is not available.
pd.options.mode.chained_assignment = None  # quieten pandas warning
pa = pytest.importorskip("pandera")  # noqa: E305 This call returns the module
import pandera as pa
from pandera import Column, Check, DataFrameSchema  # noqa: E402  # pylint: disable=wrong-import-position

ROOT = Path(__file__).resolve().parent.parent

DATASETS = [
    "global_crypto",
    "us_stocks_sip",
    "us_indices",
]
INTERVALS = [
    "1MINUTE_BARS",
    "5MINUTE_BARS",
    "15MINUTE_BARS",
    "30MINUTE_BARS",
    "60MINUTE_BARS",
]
# How many files to sample per (dataset, interval)
SAMPLE_SIZE = 5

# ---------------------------------------------------------------------------
# Canonical schema (column presence & dtypes only). We keep this intentionally
# permissive: *volume* and *transactions* are optional because certain asset
# classes (e.g. indices) do not include them.
# ---------------------------------------------------------------------------
BASE_COLUMNS = {
    "ticker": Column(str),
    "open": Column(float),
    "high": Column(float),
    "low": Column(float),
    "close": Column(float),
    "window_start": Column(int),
}
OPTIONAL_COLUMNS = {
    "volume": Column(float, required=False, nullable=True),
    "transactions": Column(float, required=False, nullable=True),
}

SCHEMA = DataFrameSchema({**BASE_COLUMNS, **OPTIONAL_COLUMNS}, coerce=True)


@pytest.mark.parametrize("dataset", DATASETS)
@pytest.mark.parametrize("interval", INTERVALS)
def test_schema(dataset: str, interval: str) -> None:
    """Randomly sample a few files and validate schema compliance."""
    data_dir = ROOT / "data" / dataset / interval

    if not data_dir.exists():
        pytest.skip(f"{data_dir} missing")

    all_files = sorted(data_dir.glob("*.csv"))
    if not all_files:
        pytest.skip(f"No CSV files in {data_dir}")

    chosen = random.sample(all_files, min(SAMPLE_SIZE, len(all_files)))

    for fp in chosen:
        df = pd.read_csv(fp, nrows=20_000)  # partial read keeps test speedy
        SCHEMA.validate(df)  # raises if invalid
