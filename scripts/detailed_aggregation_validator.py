#!/usr/bin/env python3
"""
Detailed aggregation validator that checks all aspects of the aggregated data
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import json
from typing import Dict, List, Tuple
from pathlib import Path

class AggregateValidator:
    def __init__(self, base_path: str = "/root/stock_project/data"):
        self.base_path = base_path
        self.issues = []
        self.stats = {}
        
    def validate_dataset_date(self, dataset: str, date: str):
        """Validate all timeframes for a specific dataset and date"""
        print(f"\n{'='*60}")
        print(f"Validating {dataset} for {date}")
        print(f"{'='*60}")
        
        # Load all timeframes
        data = {}
        timeframes = ['1', '5', '15', '30', '60']
        
        for tf in timeframes:
            try:
                data[tf] = self.load_data(dataset, date, tf)
                print(f"✓ Loaded {tf}-minute data: {len(data[tf])} rows, {len(data[tf]['ticker'].unique()) if 'ticker' in data[tf].columns else 0} tickers")
            except Exception as e:
                print(f"✗ Failed to load {tf}-minute data: {e}")
                self.issues.append(f"{dataset}-{date}-{tf}min: Load failed - {e}")
                
        # Run all validation tests
        if '1' in data:
            self.validate_data_format(data, dataset, date)
            self.validate_timestamps(data, dataset, date)
            self.validate_aggregation_math(data, dataset, date)
            self.validate_data_continuity(data, dataset, date)
            self.validate_price_sanity(data, dataset, date)
            self.validate_volume_consistency(data, dataset, date)
            
        return self.generate_report(dataset, date)
    
    def load_data(self, dataset: str, date: str, timeframe: str) -> pd.DataFrame:
        """Load data with proper parsing for our format"""
        # Build path based on our structure
        if timeframe == '1':
            file_path = f"{self.base_path}/{dataset}/1MINUTE_BARS/{date}.csv"
        else:
            file_path = f"{self.base_path}/{dataset}/{timeframe}MINUTE_BARS/{date}.csv"
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Read CSV
        df = pd.read_csv(file_path)
        
        # Convert window_start to datetime
        if 'window_start' in df.columns:
            df['timestamp'] = pd.to_datetime(df['window_start'], unit='ns')
        
        # Ensure numeric columns
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'transactions']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
        # Sort by timestamp and ticker
        if 'timestamp' in df.columns and 'ticker' in df.columns:
            df = df.sort_values(['ticker', 'timestamp']).reset_index(drop=True)
        
        return df
    
    def validate_data_format(self, data: Dict, dataset: str, date: str):
        """Check data types and formats"""
        print("\n1. Validating Data Format...")
        
        for tf, df in data.items():
            # Check required columns based on dataset
            if dataset == 'us_indices':
                required_cols = ['window_start', 'open', 'high', 'low', 'close', 'ticker']
            else:
                required_cols = ['window_start', 'open', 'high', 'low', 'close', 'volume', 'transactions', 'ticker']
            
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                self.issues.append(f"{tf}min: Missing columns {missing_cols}")
            
            # Check for numeric types
            for col in ['open', 'high', 'low', 'close', 'volume', 'transactions']:
                if col in df.columns:
                    if not pd.api.types.is_numeric_dtype(df[col]):
                        self.issues.append(f"{tf}min: {col} is not numeric")
                    
                    # Check for NaN values
                    nan_count = df[col].isna().sum()
                    if nan_count > 0:
                        self.issues.append(f"{tf}min: {col} has {nan_count} NaN values")
                    
                    # Check for negative prices
                    if col in ['open', 'high', 'low', 'close']:
                        neg_count = (df[col] < 0).sum()
                        if neg_count > 0:
                            self.issues.append(f"{tf}min: {col} has {neg_count} negative values")
        
        print("✓ Format validation complete")
    
    def validate_timestamps(self, data: Dict, dataset: str, date: str):
        """Validate timestamp alignment and gaps"""
        print("\n2. Validating Timestamps...")
        
        for tf, df in data.items():
            if 'timestamp' not in df.columns or len(df) == 0:
                continue
            
            # Check per ticker
            if 'ticker' in df.columns:
                for ticker in df['ticker'].unique()[:5]:  # Check first 5 tickers
                    ticker_df = df[df['ticker'] == ticker].sort_values('timestamp')
                    
                    if len(ticker_df) > 1:
                        intervals = ticker_df['timestamp'].diff().dropna()
                        expected_interval = pd.Timedelta(minutes=int(tf))
                        
                        # Find irregular intervals (allowing for market gaps)
                        irregular = intervals[(intervals != expected_interval) & (intervals < pd.Timedelta(hours=24))]
                        if len(irregular) > 0:
                            # For crypto, this might be normal due to sparse trading
                            if dataset != 'global_crypto' or len(irregular) > len(ticker_df) * 0.5:
                                self.issues.append(f"{tf}min: {ticker} has {len(irregular)} irregular intervals")
            
            # Check alignment for aggregated data
            if tf != '1' and dataset != 'global_crypto':  # Skip alignment check for 24/7 crypto
                if 'timestamp' in df.columns:
                    misaligned = df[df['timestamp'].dt.minute % int(tf) != 0]
                    if len(misaligned) > 0:
                        # Only report if it's a significant portion
                        if len(misaligned) > len(df) * 0.1:
                            self.issues.append(f"{tf}min: {len(misaligned)} misaligned timestamps ({len(misaligned)/len(df)*100:.1f}%)")
        
        print("✓ Timestamp validation complete")
    
    def validate_aggregation_math(self, data: Dict, dataset: str, date: str):
        """Verify OHLC aggregation calculations"""
        print("\n3. Validating Aggregation Math...")
        
        if '1' not in data:
            print("  ⚠ Cannot validate without 1-minute data")
            return
            
        base_df = data['1']
        
        # Sample a few tickers for validation
        if 'ticker' in base_df.columns:
            sample_tickers = base_df['ticker'].unique()[:3]  # Check first 3 tickers
        else:
            print("  ⚠ No ticker column found")
            return
        
        for tf in ['5', '15', '30', '60']:
            if tf not in data:
                continue
                
            print(f"\n  Checking {tf}-minute aggregation...")
            agg_df = data[tf]
            
            errors = 0
            checks = 0
            
            for ticker in sample_tickers:
                ticker_base = base_df[base_df['ticker'] == ticker].sort_values('timestamp')
                ticker_agg = agg_df[agg_df['ticker'] == ticker].sort_values('timestamp')
                
                # Check first few aggregated candles
                for idx, row in ticker_agg.head(5).iterrows():
                    checks += 1
                    
                    # Find corresponding 1-minute candles
                    start_time = row['timestamp']
                    end_time = start_time + pd.Timedelta(minutes=int(tf))
                    
                    mask = (ticker_base['timestamp'] >= start_time) & (ticker_base['timestamp'] < end_time)
                    base_subset = ticker_base[mask]
                    
                    if len(base_subset) == 0:
                        continue
                    
                    # Verify OHLC
                    expected = {
                        'open': base_subset.iloc[0]['open'],
                        'high': base_subset['high'].max(),
                        'low': base_subset['low'].min(),
                        'close': base_subset.iloc[-1]['close']
                    }
                    
                    if 'volume' in base_subset.columns and 'volume' in row:
                        expected['volume'] = base_subset['volume'].sum()
                    
                    # Check each value
                    for field in expected:
                        if field in row and not pd.isna(row[field]) and not pd.isna(expected[field]):
                            # Use relative tolerance for prices
                            if field == 'volume':
                                if abs(row[field] - expected[field]) > 0.01:
                                    errors += 1
                                    if errors <= 3:  # Only show first 3 errors
                                        self.issues.append(
                                            f"{tf}min: {ticker} {field} mismatch at {start_time}: "
                                            f"got {row[field]}, expected {expected[field]}"
                                        )
                            else:
                                rel_diff = abs(row[field] - expected[field]) / (expected[field] + 1e-10)
                                if rel_diff > 0.0001:
                                    errors += 1
                                    if errors <= 3:
                                        self.issues.append(
                                            f"{tf}min: {ticker} {field} mismatch at {start_time}: "
                                            f"got {row[field]}, expected {expected[field]}"
                                        )
            
            if errors > 0:
                print(f"  ✗ Found {errors} aggregation errors in {checks} checks")
            else:
                print(f"  ✓ Aggregation math correct ({checks} checks)")
    
    def validate_data_continuity(self, data: Dict, dataset: str, date: str):
        """Check for data continuity issues"""
        print("\n4. Validating Data Continuity...")
        
        for tf, df in data.items():
            if len(df) < 2 or 'ticker' not in df.columns:
                continue
            
            # Sample a few tickers
            sample_tickers = df['ticker'].unique()[:3]
            
            for ticker in sample_tickers:
                ticker_df = df[df['ticker'] == ticker].sort_values('timestamp')
                
                if len(ticker_df) < 2:
                    continue
                
                # Check for price jumps
                for col in ['close']:
                    if col not in ticker_df.columns:
                        continue
                    
                    # Calculate percentage changes
                    pct_change = ticker_df[col].pct_change().abs()
                    
                    # Flag jumps > 20% (higher threshold for crypto)
                    threshold = 0.5 if dataset == 'global_crypto' else 0.2
                    large_jumps = pct_change[pct_change > threshold]
                    
                    if len(large_jumps) > 0:
                        max_jump = pct_change.max()
                        if not pd.isna(max_jump):
                            self.issues.append(
                                f"{tf}min: {ticker} has {len(large_jumps)} large price jumps (>{threshold*100:.0f}%), "
                                f"max: {max_jump:.2%}"
                            )
        
        print("✓ Continuity validation complete")
    
    def validate_price_sanity(self, data: Dict, dataset: str, date: str):
        """Validate OHLC relationships"""
        print("\n5. Validating Price Sanity...")
        
        for tf, df in data.items():
            # Check High >= Low
            invalid_hl = df[df['high'] < df['low']]
            if len(invalid_hl) > 0:
                self.issues.append(f"{tf}min: {len(invalid_hl)} candles with High < Low")
            
            # Check High >= Open, Close
            invalid_high = df[(df['high'] < df['open']) | (df['high'] < df['close'])]
            if len(invalid_high) > 0:
                self.issues.append(f"{tf}min: {len(invalid_high)} candles with High < Open or Close")
            
            # Check Low <= Open, Close
            invalid_low = df[(df['low'] > df['open']) | (df['low'] > df['close'])]
            if len(invalid_low) > 0:
                self.issues.append(f"{tf}min: {len(invalid_low)} candles with Low > Open or Close")
            
            # Check for zero or negative volume (except indices)
            if 'volume' in df.columns and dataset != 'us_indices':
                zero_vol = (df['volume'] <= 0).sum()
                if zero_vol > 0:
                    # For crypto, some zero volume is normal
                    if dataset != 'global_crypto' or zero_vol > len(df) * 0.5:
                        self.issues.append(f"{tf}min: {zero_vol} candles with zero/negative volume")
        
        print("✓ Price sanity validation complete")
    
    def validate_volume_consistency(self, data: Dict, dataset: str, date: str):
        """Check volume aggregation consistency"""
        print("\n6. Validating Volume Consistency...")
        
        if dataset == 'us_indices':
            print("  ⚠ Skipping volume validation for indices")
            return
        
        if '1' not in data or 'volume' not in data['1'].columns:
            print("  ⚠ Cannot validate volume without 1-minute data")
            return
        
        # Check per ticker for a sample
        base_df = data['1']
        sample_tickers = base_df['ticker'].unique()[:3]
        
        for ticker in sample_tickers:
            ticker_1min_vol = base_df[base_df['ticker'] == ticker]['volume'].sum()
            
            for tf in ['5', '15', '30', '60']:
                if tf not in data or 'volume' not in data[tf].columns:
                    continue
                
                ticker_agg_vol = data[tf][data[tf]['ticker'] == ticker]['volume'].sum()
                
                # Volumes should match (within small tolerance)
                volume_diff = abs(ticker_agg_vol - ticker_1min_vol)
                rel_diff = volume_diff / (ticker_1min_vol + 1e-10)
                
                if rel_diff > 0.001:  # 0.1% tolerance
                    self.issues.append(
                        f"{tf}min: {ticker} volume mismatch: {ticker_agg_vol:.2f} vs "
                        f"1-min: {ticker_1min_vol:.2f} (diff: {rel_diff*100:.2f}%)"
                    )
        
        print("✓ Volume consistency validation complete")
    
    def generate_report(self, dataset: str, date: str) -> Dict:
        """Generate validation report"""
        print(f"\n{'='*60}")
        print("VALIDATION REPORT")
        print(f"{'='*60}")
        
        if len(self.issues) == 0:
            print("✅ ALL VALIDATIONS PASSED!")
        else:
            print(f"❌ FOUND {len(self.issues)} ISSUES:\n")
            for i, issue in enumerate(self.issues, 1):
                print(f"{i}. {issue}")
        
        return {
            'dataset': dataset,
            'date': date,
            'issues': self.issues,
            'passed': len(self.issues) == 0
        }

# Example usage
if __name__ == "__main__":
    # Test specific datasets and dates
    test_cases = [
        ('global_crypto', '2024-01-15'),
        ('us_stocks_sip', '2025-06-02'),
        ('us_indices', '2025-06-02'),
    ]
    
    validator = AggregateValidator()
    
    all_results = []
    for dataset, date in test_cases:
        # Clear issues for each test
        validator.issues = []
        result = validator.validate_dataset_date(dataset, date)
        all_results.append(result)
    
    # Summary
    print(f"\n{'='*60}")
    print("OVERALL SUMMARY")
    print(f"{'='*60}")
    passed = sum(1 for r in all_results if r['passed'])
    print(f"Passed: {passed}/{len(all_results)}")
    
    # Save detailed report
    with open('/root/stock_project/detailed_validation_report.json', 'w') as f:
        json.dump(all_results, f, indent=2, default=str)
    
    print(f"\nDetailed report saved to: /root/stock_project/detailed_validation_report.json")
