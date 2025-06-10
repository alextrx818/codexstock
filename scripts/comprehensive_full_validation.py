#!/usr/bin/env python3
"""
Comprehensive Full Validation Script
Tests every single aggregated file across all instruments and intervals
Validates format consistency and aggregation accuracy
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime
import json
from typing import Dict, List, Tuple, Optional
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('validation_results.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Configuration
DATASETS = {
    "data/global_crypto": {
        "expected_columns": ["window_start", "open", "high", "low", "close", "volume", "transactions", "ticker"],
        "has_volume": True,
        "has_transactions": True
    },
    "data/us_stocks_sip": {
        "expected_columns": ["window_start", "open", "high", "low", "close", "volume", "transactions", "ticker"],
        "has_volume": True,
        "has_transactions": True
    },
    "data/us_indices": {
        "expected_columns": ["window_start", "open", "high", "low", "close", "ticker"],
        "has_volume": False,
        "has_transactions": False
    }
}

INTERVALS = [5, 15, 30, 60]

class FormatValidator:
    """Validates file format consistency within each instrument"""
    
    def __init__(self, dataset: str, config: dict):
        self.dataset = dataset
        self.config = config
        self.format_errors = []
        
    def validate_file_format(self, file_path: Path) -> Tuple[bool, List[str]]:
        """Validate individual file format"""
        errors = []
        
        try:
            # Read first few rows to check format
            df = pd.read_csv(file_path, nrows=10)
            
            # Check columns
            actual_columns = list(df.columns)
            expected_columns = self.config["expected_columns"]
            
            if actual_columns != expected_columns:
                errors.append(f"Column mismatch: expected {expected_columns}, got {actual_columns}")
            
            # Check data types
            if 'window_start' in df.columns:
                if df['window_start'].dtype != np.int64:
                    errors.append(f"window_start should be int64, got {df['window_start'].dtype}")
            
            # Check for required numeric columns
            numeric_cols = ['open', 'high', 'low', 'close']
            if self.config["has_volume"]:
                numeric_cols.append('volume')
            
            for col in numeric_cols:
                if col in df.columns and not pd.api.types.is_numeric_dtype(df[col]):
                    errors.append(f"{col} should be numeric, got {df[col].dtype}")
            
            # Check ticker column
            if 'ticker' in df.columns and df['ticker'].dtype != 'object':
                errors.append(f"ticker should be string/object, got {df['ticker'].dtype}")
                
        except Exception as e:
            errors.append(f"Failed to read file: {str(e)}")
            
        return len(errors) == 0, errors
    
    def validate_dataset_format(self, base_path: Path) -> Dict:
        """Validate format consistency across all files in dataset"""
        results = {
            "1MINUTE_BARS": {"total": 0, "passed": 0, "errors": []},
            "5MINUTE_BARS": {"total": 0, "passed": 0, "errors": []},
            "15MINUTE_BARS": {"total": 0, "passed": 0, "errors": []},
            "30MINUTE_BARS": {"total": 0, "passed": 0, "errors": []},
            "60MINUTE_BARS": {"total": 0, "passed": 0, "errors": []}
        }
        
        for interval_dir in ["1MINUTE_BARS"] + [f"{i}MINUTE_BARS" for i in INTERVALS]:
            dir_path = base_path / self.dataset / interval_dir
            if not dir_path.exists():
                continue
                
            csv_files = sorted(dir_path.glob("*.csv"))
            results[interval_dir]["total"] = len(csv_files)
            
            for csv_file in csv_files:
                passed, errors = self.validate_file_format(csv_file)
                if passed:
                    results[interval_dir]["passed"] += 1
                else:
                    results[interval_dir]["errors"].append({
                        "file": csv_file.name,
                        "errors": errors
                    })
        
        return results

class AggregationValidator:
    """Validates aggregation accuracy by comparing with 1-minute data"""
    
    def __init__(self, dataset: str, config: dict):
        self.dataset = dataset
        self.config = config
        
    def validate_single_file(self, date_str: str, base_path: Path) -> Dict:
        """Validate all intervals for a single date"""
        results = {
            "date": date_str,
            "intervals": {}
        }
        
        # Load 1-minute data
        one_min_path = base_path / self.dataset / "1MINUTE_BARS" / f"{date_str}.csv"
        if not one_min_path.exists():
            results["error"] = "1-minute file not found"
            return results
            
        try:
            df_1min = pd.read_csv(one_min_path)
            df_1min['ts'] = pd.to_datetime(df_1min['window_start'], unit='ns')
            
            # Validate each interval
            for interval in INTERVALS:
                interval_path = base_path / self.dataset / f"{interval}MINUTE_BARS" / f"{date_str}.csv"
                
                if not interval_path.exists():
                    results["intervals"][interval] = {"status": "missing"}
                    continue
                
                try:
                    df_agg = pd.read_csv(interval_path)
                    validation_result = self.validate_aggregation(df_1min, df_agg, interval)
                    results["intervals"][interval] = validation_result
                except Exception as e:
                    results["intervals"][interval] = {"status": "error", "message": str(e)}
                    
        except Exception as e:
            results["error"] = f"Failed to process: {str(e)}"
            
        return results
    
    def validate_aggregation(self, df_1min: pd.DataFrame, df_agg: pd.DataFrame, interval: int) -> Dict:
        """Validate aggregation accuracy for specific interval"""
        result = {
            "status": "passed",
            "total_bars": len(df_agg),
            "errors": [],
            "warnings": []
        }
        
        # Convert aggregated timestamps
        df_agg['ts'] = pd.to_datetime(df_agg['window_start'], unit='ns')
        
        # Sample validation - check random bars
        sample_size = min(100, len(df_agg))
        sample_indices = np.random.choice(len(df_agg), sample_size, replace=False)
        
        errors_found = 0
        for idx in sample_indices:
            row = df_agg.iloc[idx]
            ticker = row['ticker']
            start_time = row['ts']
            end_time = start_time + pd.Timedelta(minutes=interval)
            
            # Get corresponding 1-minute bars
            mask = (df_1min['ticker'] == ticker) & \
                   (df_1min['ts'] >= start_time) & \
                   (df_1min['ts'] < end_time)
            window = df_1min[mask]
            
            if len(window) == 0:
                result["warnings"].append(f"No 1-min data for {ticker} at {start_time}")
                continue
            
            # Validate OHLC
            expected_open = window.iloc[0]['open']
            expected_close = window.iloc[-1]['close']
            expected_high = window['high'].max()
            expected_low = window['low'].min()
            
            tolerance = 1e-6
            
            if abs(row['open'] - expected_open) > tolerance:
                errors_found += 1
                if errors_found <= 10:  # Limit error reporting
                    result["errors"].append(
                        f"{ticker}@{start_time}: Open mismatch {row['open']} vs {expected_open}"
                    )
            
            if abs(row['close'] - expected_close) > tolerance:
                errors_found += 1
                if errors_found <= 10:
                    result["errors"].append(
                        f"{ticker}@{start_time}: Close mismatch {row['close']} vs {expected_close}"
                    )
            
            if abs(row['high'] - expected_high) > tolerance:
                errors_found += 1
                if errors_found <= 10:
                    result["errors"].append(
                        f"{ticker}@{start_time}: High mismatch {row['high']} vs {expected_high}"
                    )
            
            if abs(row['low'] - expected_low) > tolerance:
                errors_found += 1
                if errors_found <= 10:
                    result["errors"].append(
                        f"{ticker}@{start_time}: Low mismatch {row['low']} vs {expected_low}"
                    )
            
            # Validate volume if present
            if self.config["has_volume"] and 'volume' in window.columns:
                expected_volume = window['volume'].sum()
                if abs(row['volume'] - expected_volume) > tolerance:
                    errors_found += 1
                    if errors_found <= 10:
                        result["errors"].append(
                            f"{ticker}@{start_time}: Volume mismatch {row['volume']} vs {expected_volume}"
                        )
            
            # Validate transactions if present
            if self.config["has_transactions"] and 'transactions' in window.columns:
                expected_transactions = window['transactions'].sum()
                if abs(row['transactions'] - expected_transactions) > tolerance:
                    errors_found += 1
                    if errors_found <= 10:
                        result["errors"].append(
                            f"{ticker}@{start_time}: Transactions mismatch {row['transactions']} vs {expected_transactions}"
                        )
        
        if errors_found > 0:
            result["status"] = "failed"
            result["total_errors"] = errors_found
            
        return result

def validate_file_batch(args):
    """Process a batch of files for parallel validation"""
    dataset, config, base_path, file_batch = args
    validator = AggregationValidator(dataset, config)
    results = []
    
    for date_str in file_batch:
        result = validator.validate_single_file(date_str, base_path)
        results.append(result)
        
    return results

def main():
    """Main validation function"""
    project_root = Path(__file__).parent.parent
    
    print("\n" + "="*80)
    print("COMPREHENSIVE FULL VALIDATION")
    print("="*80)
    print(f"Started at: {datetime.now()}")
    print("="*80 + "\n")
    
    # Overall results
    overall_results = {}
    
    # Phase 1: Format Validation
    print("\nPHASE 1: FORMAT VALIDATION")
    print("-"*60)
    
    for dataset, config in DATASETS.items():
        print(f"\nValidating format for {dataset}...")
        format_validator = FormatValidator(dataset, config)
        format_results = format_validator.validate_dataset_format(project_root)
        overall_results[dataset] = {"format": format_results}
        
        # Print format results
        for interval, stats in format_results.items():
            if stats["total"] > 0:
                pass_rate = (stats["passed"] / stats["total"]) * 100
                print(f"  {interval}: {stats['passed']}/{stats['total']} passed ({pass_rate:.1f}%)")
                if stats["errors"]:
                    print(f"    ⚠️  {len(stats['errors'])} files with format errors")
    
    # Phase 2: Aggregation Validation
    print("\n\nPHASE 2: AGGREGATION VALIDATION")
    print("-"*60)
    
    for dataset, config in DATASETS.items():
        print(f"\nValidating aggregation for {dataset}...")
        
        # Get list of dates to validate
        one_min_dir = project_root / dataset / "1MINUTE_BARS"
        if not one_min_dir.exists():
            print(f"  ⚠️  No 1-minute data found")
            continue
            
        date_files = sorted([f.stem for f in one_min_dir.glob("*.csv")])
        total_files = len(date_files)
        print(f"  Total dates to validate: {total_files}")
        
        # Prepare for parallel processing
        num_workers = min(32, multiprocessing.cpu_count())
        batch_size = max(1, total_files // num_workers)
        
        file_batches = []
        for i in range(0, total_files, batch_size):
            batch = date_files[i:i + batch_size]
            if batch:
                file_batches.append((dataset, config, project_root, batch))
        
        # Process in parallel
        all_results = []
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            futures = []
            for batch_args in file_batches:
                future = executor.submit(validate_file_batch, batch_args)
                futures.append(future)
            
            # Collect results
            for future in as_completed(futures):
                try:
                    batch_results = future.result()
                    all_results.extend(batch_results)
                except Exception as e:
                    logging.error(f"Batch processing failed: {e}")
        
        # Analyze results
        interval_stats = {interval: {"total": 0, "passed": 0, "failed": 0, "missing": 0} 
                         for interval in INTERVALS}
        
        for file_result in all_results:
            if "error" in file_result:
                continue
                
            for interval, interval_result in file_result.get("intervals", {}).items():
                interval_stats[interval]["total"] += 1
                
                if interval_result.get("status") == "passed":
                    interval_stats[interval]["passed"] += 1
                elif interval_result.get("status") == "failed":
                    interval_stats[interval]["failed"] += 1
                elif interval_result.get("status") == "missing":
                    interval_stats[interval]["missing"] += 1
        
        # Store and print results
        overall_results[dataset]["aggregation"] = {
            "total_dates": total_files,
            "interval_stats": interval_stats,
            "detailed_results": all_results
        }
        
        print(f"\n  Aggregation Results for {dataset}:")
        for interval in INTERVALS:
            stats = interval_stats[interval]
            if stats["total"] > 0:
                pass_rate = (stats["passed"] / stats["total"]) * 100
                print(f"    {interval}-minute: {stats['passed']}/{stats['total']} passed ({pass_rate:.1f}%)")
                if stats["failed"] > 0:
                    print(f"      ❌ {stats['failed']} failed validations")
                if stats["missing"] > 0:
                    print(f"      ⚠️  {stats['missing']} missing files")
    
    # Phase 3: Summary Report
    print("\n\nPHASE 3: SUMMARY REPORT")
    print("="*80)
    
    # Save detailed results to JSON
    with open('validation_results.json', 'w') as f:
        json.dump(overall_results, f, indent=2, default=str)
    
    # Print summary
    all_passed = True
    for dataset in DATASETS:
        if dataset not in overall_results:
            continue
            
        print(f"\n{dataset}:")
        
        # Format summary
        format_results = overall_results[dataset].get("format", {})
        format_passed = all(
            stats["passed"] == stats["total"] 
            for stats in format_results.values() 
            if stats["total"] > 0
        )
        print(f"  Format Validation: {'✅ PASSED' if format_passed else '❌ FAILED'}")
        
        # Aggregation summary
        agg_results = overall_results[dataset].get("aggregation", {})
        if "interval_stats" in agg_results:
            agg_passed = all(
                stats["passed"] == stats["total"] 
                for stats in agg_results["interval_stats"].values()
            )
            print(f"  Aggregation Validation: {'✅ PASSED' if agg_passed else '❌ FAILED'}")
            all_passed = all_passed and format_passed and agg_passed
        else:
            all_passed = False
    
    print("\n" + "="*80)
    print(f"OVERALL RESULT: {'✅ ALL TESTS PASSED' if all_passed else '❌ SOME TESTS FAILED'}")
    print(f"Detailed results saved to: validation_results.json")
    print(f"Log file: validation_results.log")
    print(f"Completed at: {datetime.now()}")
    print("="*80 + "\n")
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())
