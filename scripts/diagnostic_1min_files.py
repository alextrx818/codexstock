#!/usr/bin/env python3
"""
📁 DIAGNOSTIC SCRIPT FOR 1-MINUTE AGGREGATION FILES
Checks formatting, timestamp consistency, trading session logic, and duplication
"""

import os
import pandas as pd
from datetime import datetime, time, timezone
import pytz
from pathlib import Path
import json
import numpy as np
import sys
import logging
import traceback
from multiprocessing import Pool, cpu_count

# Set up verbose logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

BASE_PATH = "/root/stock_project/data"
CATEGORIES = {
    "us_stocks_sip": "stocks",
    "global_crypto": "currency",  # crypto trades 24/7 like forex
    "us_indices": "indices"
}
FILES_PER_CATEGORY = 20

# Expected columns for each category
EXPECTED_COLUMNS = {
    "us_stocks_sip": ['ticker', 'volume', 'open', 'close', 'high', 'low', 'window_start', 'transactions'],
    "global_crypto": ['ticker', 'volume', 'open', 'close', 'high', 'low', 'window_start', 'transactions'],
    "us_indices": ['ticker', 'open', 'close', 'high', 'low', 'window_start']
}

def is_valid_timestamp(ts):
    """Check if timestamp is valid nanosecond Unix timestamp"""
    try:
        if isinstance(ts, (int, float)):
            # Convert nanoseconds to datetime
            pd.to_datetime(ts, unit='ns')
            return True
        return False
    except:
        return False

def within_market_hours(ts_nano, category, ticker=None):
    """Check if timestamp is within market hours for the asset class"""
    try:
        # Convert nanoseconds to datetime
        dt = pd.to_datetime(ts_nano, unit='ns')
        
        # Convert to Eastern Time for US markets
        et = pytz.timezone('US/Eastern')
        dt_et = dt.tz_localize('UTC').tz_convert(et)
        
        # Get time component
        t = dt_et.time()
        day_of_week = dt_et.weekday()  # 0=Monday, 6=Sunday
        
        if category == "stocks":
            # US stocks: 9:30 AM - 4:00 PM ET, Monday-Friday
            if day_of_week >= 5:  # Weekend
                return False
            return time(9, 30) <= t <= time(16, 0)
        elif category == "indices":
            # US indices: Similar to stocks but some may have extended hours
            if day_of_week >= 5:  # Weekend
                return False
            # Allow slightly extended hours for indices
            return time(4, 0) <= t <= time(20, 0)
        else:
            # Crypto/Currency: 24/7
            return True
    except Exception as e:
        logger.warning(f"Error checking market hours: {e}")
        return True

def diagnose_file(filepath, dataset, category):
    """Diagnose issues in a single 1-minute data file"""
    filename = os.path.basename(filepath)
    logger.info(f"  Diagnosing file: {filename}")
    
    try:
        # Read the file
        logger.debug(f"    Reading CSV file...")
        df = pd.read_csv(filepath)
        logger.debug(f"    Loaded {len(df)} rows, {len(df.columns)} columns")
        
        issues = []
        
        # 1. Check columns
        logger.debug(f"    Checking columns...")
        expected_cols = EXPECTED_COLUMNS[dataset]
        actual_cols = df.columns.tolist()
        
        if set(actual_cols) != set(expected_cols):
            missing = set(expected_cols) - set(actual_cols)
            extra = set(actual_cols) - set(expected_cols)
            if missing:
                issues.append(f"Missing columns: {missing}")
            if extra:
                issues.append(f"Extra columns: {extra}")
        
        # 2. Check timestamp format and validity
        logger.debug(f"    Checking timestamps...")
        if 'window_start' in df.columns:
            invalid_ts = df[~df['window_start'].apply(is_valid_timestamp)]
            if len(invalid_ts) > 0:
                issues.append(f"Invalid timestamp format: {len(invalid_ts)} rows")
            
            # Convert to datetime for analysis
            df['timestamp'] = pd.to_datetime(df['window_start'], unit='ns')
        else:
            issues.append("No window_start column found")
            return {
                "file": filename,
                "dataset": dataset,
                "category": category,
                "issues": issues,
                "rows": len(df),
                "tickers": 0,
                "file_size_mb": round(os.path.getsize(filepath) / (1024 ** 2), 2)
            }
        
        # 3. Check for duplicates (per ticker)
        logger.debug(f"    Checking for duplicates...")
        if 'ticker' in df.columns:
            dup_count = 0
            unique_tickers = df['ticker'].unique()
            logger.debug(f"    Found {len(unique_tickers)} unique tickers")
            
            for ticker in unique_tickers[:10]:  # Check first 10 tickers
                ticker_df = df[df['ticker'] == ticker]
                dups = ticker_df.duplicated(subset=['window_start'])
                if dups.any():
                    dup_count += dups.sum()
            if dup_count > 0:
                issues.append(f"Duplicate timestamps found: {dup_count} rows")
        
        # 4. Check time gaps (sample a few tickers)
        logger.debug(f"    Checking time gaps...")
        if 'ticker' in df.columns:
            sample_tickers = df['ticker'].unique()[:5]  # Check first 5 tickers
            gap_issues = []
            
            for ticker in sample_tickers:
                ticker_df = df[df['ticker'] == ticker].sort_values('timestamp')
                if len(ticker_df) > 1:
                    # Calculate time differences
                    ticker_df['delta'] = ticker_df['timestamp'].diff()
                    gaps = ticker_df['delta'].dt.total_seconds().dropna()
                    
                    # Find unexpected gaps (not 60 seconds)
                    unexpected = gaps[(gaps != 60) & (gaps < 3600)]  # Ignore large gaps (market close)
                    if len(unexpected) > 0:
                        gap_issues.append(f"{ticker}: {len(unexpected)} unexpected gaps")
            
            if gap_issues:
                issues.append(f"Time gap issues: {'; '.join(gap_issues[:3])}")
        
        # 5. Check market hours (only for stocks and indices)
        logger.debug(f"    Checking market hours...")
        if category in ["stocks", "indices"]:
            out_of_hours_count = 0
            sample_tickers = df['ticker'].unique()[:5] if 'ticker' in df.columns else []
            
            for ticker in sample_tickers:
                ticker_df = df[df['ticker'] == ticker]
                out_of_hours = ticker_df[~ticker_df['window_start'].apply(
                    lambda x: within_market_hours(x, category, ticker)
                )]
                out_of_hours_count += len(out_of_hours)
            
            if out_of_hours_count > 0:
                # Calculate percentage
                total_checked = len(df[df['ticker'].isin(sample_tickers)])
                pct = (out_of_hours_count / total_checked * 100) if total_checked > 0 else 0
                issues.append(f"Out-of-market-hours data: {out_of_hours_count} rows ({pct:.1f}% of sampled)")
        
        # 6. Check for data quality issues
        logger.debug(f"    Checking data quality...")
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            if col in df.columns:
                # Check for negative values
                neg_count = (df[col] < 0).sum()
                if neg_count > 0:
                    issues.append(f"Negative {col} values: {neg_count} rows")
                
                # Check for NaN values
                nan_count = df[col].isna().sum()
                if nan_count > 0:
                    issues.append(f"NaN {col} values: {nan_count} rows")
        
        # 7. Check OHLC relationships
        logger.debug(f"    Checking OHLC relationships...")
        if all(col in df.columns for col in ['open', 'high', 'low', 'close']):
            invalid_hl = (df['high'] < df['low']).sum()
            if invalid_hl > 0:
                issues.append(f"High < Low: {invalid_hl} rows")
            
            invalid_high = ((df['high'] < df['open']) | (df['high'] < df['close'])).sum()
            if invalid_high > 0:
                issues.append(f"High < Open/Close: {invalid_high} rows")
            
            invalid_low = ((df['low'] > df['open']) | (df['low'] > df['close'])).sum()
            if invalid_low > 0:
                issues.append(f"Low > Open/Close: {invalid_low} rows")
        
        # 8. Check timestamp ordering
        if 'timestamp' in df.columns:
            if not df['timestamp'].is_monotonic_increasing:
                issues.append("Timestamps not in chronological order")
        
        # Get file stats
        num_tickers = len(df['ticker'].unique()) if 'ticker' in df.columns else 0
        
        logger.info(f"    ✓ Completed: {filename} - {len(issues)} issues found")
        
        return {
            "file": filename,
            "dataset": dataset,
            "category": category,
            "issues": issues if issues else ["OK"],
            "rows": len(df),
            "tickers": num_tickers,
            "file_size_mb": round(os.path.getsize(filepath) / (1024 ** 2), 2)
        }
        
    except Exception as e:
        logger.error(f"    ✗ Error processing {filename}: {str(e)}")
        logger.debug(traceback.format_exc())
        return {
            "file": os.path.basename(filepath),
            "dataset": dataset,
            "category": category,
            "issues": [f"Error reading file: {str(e)}"],
            "rows": 0,
            "tickers": 0,
            "file_size_mb": round(os.path.getsize(filepath) / (1024 ** 2), 2)
        }

def run_diagnostics():
    """Run diagnostics on sample files from each category"""
    report = []
    
    print("="*80)
    print("1-MINUTE DATA DIAGNOSTICS")
    print("="*80)
    logger.info("Starting diagnostics...")
    
    for dataset, category in CATEGORIES.items():
        folder = Path(BASE_PATH) / dataset / "1MINUTE_BARS"
        
        logger.info(f"\nProcessing dataset: {dataset}")
        
        if not folder.exists():
            logger.warning(f"[{dataset}] Folder not found: {folder}")
            continue
            
        # Get all CSV files
        all_files = sorted(folder.glob("*.csv"))
        
        if not all_files:
            logger.warning(f"[{dataset}] No CSV files found")
            continue
            
        # Sample files evenly across the date range
        num_files = min(FILES_PER_CATEGORY, len(all_files))
        if num_files < len(all_files):
            # Sample evenly distributed files
            indices = np.linspace(0, len(all_files)-1, num_files, dtype=int)
            sample_files = [all_files[i] for i in indices]
        else:
            sample_files = all_files
        
        print(f"\n[{dataset.upper()}] - {category}")
        print(f"Total files: {len(all_files)}")
        print(f"Sampling: {len(sample_files)} files")
        print(f"Date range: {all_files[0].stem} to {all_files[-1].stem}")
        
        logger.info(f"Selected {len(sample_files)} files for analysis")
        
        # Create multiprocessing pool
        with Pool(processes=32) as pool:
            args = [(str(filepath), dataset, category) for filepath in sample_files]
            results = pool.starmap(diagnose_file, args)
            
            for result in results:
                report.append(result)
                logger.info(f"  {result['file']}: {len(result['issues'])} issues")
        
        logger.info(f"Completed processing {dataset}")
    
    logger.info("Diagnostics complete!")
    return pd.DataFrame(report)

def generate_summary(df_report):
    """Generate a summary of issues found"""
    print("\n" + "="*80)
    print("SUMMARY OF ISSUES")
    print("="*80)
    
    # Group by dataset
    for dataset in df_report['dataset'].unique():
        dataset_df = df_report[df_report['dataset'] == dataset]
        
        print(f"\n[{dataset.upper()}]")
        
        # Count files with issues
        ok_files = dataset_df[dataset_df['issues'].apply(lambda x: x == ["OK"])].shape[0]
        total_files = dataset_df.shape[0]
        
        print(f"Files analyzed: {total_files}")
        print(f"Files OK: {ok_files}")
        print(f"Files with issues: {total_files - ok_files}")
        
        if total_files > ok_files:
            # Aggregate issue types
            all_issues = []
            for issues_list in dataset_df['issues']:
                if issues_list != ["OK"]:
                    all_issues.extend(issues_list)
            
            # Count issue types
            issue_counts = {}
            for issue in all_issues:
                # Extract issue type
                issue_type = issue.split(":")[0]
                issue_counts[issue_type] = issue_counts.get(issue_type, 0) + 1
            
            print("\nMost common issues:")
            for issue_type, count in sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"  - {issue_type}: {count} occurrences")

if __name__ == "__main__":
    # Run diagnostics
    df_report = run_diagnostics()
    
    # Save detailed report
    output_file = "/root/stock_project/diagnostics_1min_report.csv"
    df_report.to_csv(output_file, index=False)
    print(f"\nDetailed report saved to: {output_file}")
    
    # Generate summary
    generate_summary(df_report)
    
    # Show sample of files with issues
    print("\n" + "="*80)
    print("SAMPLE FILES WITH ISSUES")
    print("="*80)
    
    files_with_issues = df_report[df_report['issues'].apply(lambda x: x != ["OK"])]
    if not files_with_issues.empty:
        for _, row in files_with_issues.head(10).iterrows():
            print(f"\nFile: {row['file']} ({row['dataset']})")
            print(f"Rows: {row['rows']:,}, Tickers: {row['tickers']}")
            print(f"File size: {row['file_size_mb']:.2f} MB")
            print("Issues:")
            for issue in row['issues']:
                print(f"  - {issue}")
    else:
        print("No issues found in sampled files!")
    
    # Save detailed JSON report for further analysis
    json_output = "/root/stock_project/diagnostics_1min_detailed.json"
    with open(json_output, 'w') as f:
        json.dump(df_report.to_dict('records'), f, indent=2)
    print(f"\nDetailed JSON report saved to: {json_output}")
