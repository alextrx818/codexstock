#!/usr/bin/env python3
"""
Production-ready crypto data aggregation pipeline v2
Meets all specified requirements for reliable, validated aggregation
"""

import pandas as pd
import numpy as np
import os
import sys
import json
import hashlib
import pytz
from datetime import datetime, timedelta
from pathlib import Path
import logging
from typing import Dict, List, Optional, Tuple
import concurrent.futures
from functools import partial

# Configuration
CONFIG = {
    'base_dir': '/root/stock_project/data/global_crypto',
    'timezone': 'UTC',  # Crypto trades 24/7, using UTC
    'session_hours': None,  # No session hours for crypto
    'intervals': [5, 15, 30, 60],
    'max_workers': 4,  # For parallel processing
    'chunk_size': 100,  # Process tickers in chunks
}

# Setup structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/root/stock_project/logs/crypto_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CryptoDataPipeline:
    def __init__(self, config: Dict):
        self.config = config
        self.base_dir = Path(config['base_dir'])
        self.timezone = pytz.timezone(config['timezone'])
        
        # Create directories
        self.raw_dir = self.base_dir / "1MINUTE_BARS"
        self.metadata_dir = self.base_dir / "metadata"
        self.validation_dir = self.base_dir / "validation"
        
        for interval in config['intervals']:
            (self.base_dir / f"{interval}MINUTE_BARS").mkdir(exist_ok=True)
        
        self.metadata_dir.mkdir(exist_ok=True)
        self.validation_dir.mkdir(exist_ok=True)
        
        # Initialize metadata
        self.processing_metadata = {
            'start_time': datetime.now().isoformat(),
            'files_processed': 0,
            'errors': [],
            'validations': {}
        }
    
    def calculate_file_checksum(self, filepath: Path) -> str:
        """Calculate SHA256 checksum of a file."""
        sha256_hash = hashlib.sha256()
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    def validate_raw_data(self, df: pd.DataFrame, date_str: str) -> Dict:
        """Validate raw 1-minute data."""
        validation_results = {
            'date': date_str,
            'total_rows': len(df),
            'unique_tickers': df['ticker'].nunique(),
            'issues': []
        }
        
        # Check for duplicate timestamps per ticker
        duplicates = df.groupby(['ticker', 'window_start']).size()
        dup_count = (duplicates > 1).sum()
        if dup_count > 0:
            validation_results['issues'].append(f"Found {dup_count} duplicate timestamp-ticker pairs")
        
        # Check for negative volumes
        neg_volumes = (df['volume'] < 0).sum()
        if neg_volumes > 0:
            validation_results['issues'].append(f"Found {neg_volumes} negative volumes")
        
        # Check price sanity (low <= high, all prices > 0)
        price_issues = ((df['low'] > df['high']) | 
                       (df['open'] <= 0) | 
                       (df['close'] <= 0) | 
                       (df['high'] <= 0) | 
                       (df['low'] <= 0)).sum()
        if price_issues > 0:
            validation_results['issues'].append(f"Found {price_issues} price anomalies")
        
        # Check timestamp ordering
        df_sorted = df.sort_values(['ticker', 'window_start'])
        if not df.equals(df_sorted):
            validation_results['issues'].append("Data not properly sorted by ticker and timestamp")
        
        validation_results['passed'] = len(validation_results['issues']) == 0
        return validation_results
    
    def aggregate_bars(self, df: pd.DataFrame, minutes: int, ticker: str) -> pd.DataFrame:
        """
        Aggregate 1-minute bars to specified interval for a single ticker.
        Uses pandas resample with proper boundary handling.
        """
        # Filter for specific ticker
        ticker_data = df[df['ticker'] == ticker].copy()
        
        if len(ticker_data) == 0:
            return pd.DataFrame()
        
        # Convert nanosecond timestamps to datetime
        ticker_data['datetime'] = pd.to_datetime(ticker_data['window_start'], unit='ns', utc=True)
        ticker_data = ticker_data.set_index('datetime').sort_index()
        
        # Resample using pandas - crypto trades 24/7 so no session filtering
        agg = ticker_data.resample(f'{minutes}min', label='left', closed='left').agg({
            'volume': 'sum',
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'transactions': 'sum'
        }).dropna(subset=['open', 'close'])
        
        # Convert back to nanosecond timestamps
        agg['window_start'] = agg.index.astype(np.int64)
        agg['ticker'] = ticker
        agg = agg.reset_index(drop=True)
        
        # Reorder columns to match original format
        return agg[['ticker', 'volume', 'open', 'close', 'high', 'low', 'window_start', 'transactions']]
    
    def validate_aggregation(self, df_1min: pd.DataFrame, df_agg: pd.DataFrame, 
                           interval: int, ticker: str) -> Dict:
        """Validate aggregated data against 1-minute source."""
        validation = {
            'ticker': ticker,
            'interval': interval,
            'checks': {}
        }
        
        # Filter both dataframes for the ticker
        df_1min_ticker = df_1min[df_1min['ticker'] == ticker]
        df_agg_ticker = df_agg[df_agg['ticker'] == ticker]
        
        if len(df_1min_ticker) == 0 or len(df_agg_ticker) == 0:
            validation['checks']['data_exists'] = False
            return validation
        
        # Volume reconciliation
        vol_1min = df_1min_ticker['volume'].sum()
        vol_agg = df_agg_ticker['volume'].sum()
        vol_diff = abs(vol_1min - vol_agg)
        validation['checks']['volume_match'] = vol_diff < 0.01  # Floating point tolerance
        
        # Transaction count reconciliation
        trans_1min = df_1min_ticker['transactions'].sum()
        trans_agg = df_agg_ticker['transactions'].sum()
        validation['checks']['transaction_match'] = trans_1min == trans_agg
        
        # Price sanity checks
        validation['checks']['price_sanity'] = (
            (df_agg_ticker['low'] <= df_agg_ticker['high']).all() and
            (df_agg_ticker['low'] <= df_agg_ticker['open']).all() and
            (df_agg_ticker['low'] <= df_agg_ticker['close']).all() and
            (df_agg_ticker['high'] >= df_agg_ticker['open']).all() and
            (df_agg_ticker['high'] >= df_agg_ticker['close']).all()
        )
        
        validation['passed'] = all(validation['checks'].values())
        return validation
    
    def process_ticker_chunk(self, tickers: List[str], df_1min: pd.DataFrame, 
                           date_str: str, interval: int) -> Tuple[pd.DataFrame, List[Dict]]:
        """Process a chunk of tickers for a specific interval."""
        aggregated_data = []
        validations = []
        
        for ticker in tickers:
            try:
                # Aggregate data
                df_agg = self.aggregate_bars(df_1min, interval, ticker)
                if len(df_agg) > 0:
                    aggregated_data.append(df_agg)
                    
                    # Validate aggregation
                    validation = self.validate_aggregation(df_1min, df_agg, interval, ticker)
                    validations.append(validation)
            
            except Exception as e:
                logger.error(f"Error processing {ticker} for {interval}min: {str(e)}")
                self.processing_metadata['errors'].append({
                    'date': date_str,
                    'ticker': ticker,
                    'interval': interval,
                    'error': str(e)
                })
        
        # Combine all ticker data
        if aggregated_data:
            return pd.concat(aggregated_data, ignore_index=True), validations
        return pd.DataFrame(), validations
    
    def process_date(self, date_str: str) -> bool:
        """Process all aggregations for a single date."""
        logger.info(f"Processing {date_str}")
        
        try:
            # Read 1-minute data
            input_file = self.raw_dir / f"{date_str}.csv"
            if not input_file.exists():
                logger.error(f"File not found: {input_file}")
                return False
            
            # Calculate checksum
            checksum = self.calculate_file_checksum(input_file)
            
            # Read and validate data
            df_1min = pd.read_csv(input_file)
            validation_results = self.validate_raw_data(df_1min, date_str)
            
            if not validation_results['passed']:
                logger.warning(f"Validation issues for {date_str}: {validation_results['issues']}")
            
            # Get unique tickers and split into chunks
            tickers = df_1min['ticker'].unique()
            ticker_chunks = [tickers[i:i + self.config['chunk_size']] 
                           for i in range(0, len(tickers), self.config['chunk_size'])]
            
            # Process each interval
            for interval in self.config['intervals']:
                logger.info(f"  Aggregating {interval}-minute bars for {len(tickers)} tickers")
                
                # Process ticker chunks in parallel
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.config['max_workers']) as executor:
                    process_func = partial(self.process_ticker_chunk, 
                                         df_1min=df_1min, date_str=date_str, interval=interval)
                    results = list(executor.map(process_func, ticker_chunks))
                
                # Combine results
                all_data = []
                all_validations = []
                for data, validations in results:
                    if len(data) > 0:
                        all_data.append(data)
                    all_validations.extend(validations)
                
                if all_data:
                    df_aggregated = pd.concat(all_data, ignore_index=True)
                    df_aggregated = df_aggregated.sort_values(['ticker', 'window_start'])
                    
                    # Write aggregated data
                    output_file = self.base_dir / f"{interval}MINUTE_BARS" / f"{date_str}.csv"
                    df_aggregated.to_csv(output_file, index=False)
                    
                    logger.info(f"    Created {len(df_aggregated)} {interval}-minute bars")
                    
                    # Store validation results
                    failed_validations = [v for v in all_validations if not v.get('passed', True)]
                    if failed_validations:
                        logger.warning(f"    {len(failed_validations)} validation failures")
            
            # Save metadata
            metadata = {
                'date': date_str,
                'checksum': checksum,
                'processing_time': datetime.now().isoformat(),
                'raw_validation': validation_results,
                'row_count': len(df_1min),
                'ticker_count': len(tickers)
            }
            
            metadata_file = self.metadata_dir / f"{date_str}_metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing {date_str}: {str(e)}")
            self.processing_metadata['errors'].append({
                'date': date_str,
                'error': str(e)
            })
            return False
    
    def run_pipeline(self, dates: Optional[List[str]] = None):
        """Run the complete pipeline for specified dates or all available dates."""
        if dates is None:
            # Process all available dates
            dates = sorted([f.stem for f in self.raw_dir.glob("*.csv")])
        
        logger.info(f"Starting pipeline for {len(dates)} dates")
        
        success_count = 0
        for date_str in dates:
            if self.process_date(date_str):
                success_count += 1
            
            # Save progress metadata
            self.processing_metadata['files_processed'] = success_count
            with open(self.base_dir / 'pipeline_metadata.json', 'w') as f:
                json.dump(self.processing_metadata, f, indent=2)
        
        # Final summary
        logger.info(f"Pipeline complete: {success_count}/{len(dates)} files processed successfully")
        if self.processing_metadata['errors']:
            logger.warning(f"Total errors: {len(self.processing_metadata['errors'])}")
        
        return success_count == len(dates)

def main():
    """Main entry point."""
    # Create logs directory
    os.makedirs('/root/stock_project/logs', exist_ok=True)
    
    # Initialize pipeline
    pipeline = CryptoDataPipeline(CONFIG)
    
    # Run for all dates or specific dates passed as arguments
    if len(sys.argv) > 1:
        dates = sys.argv[1:]
        pipeline.run_pipeline(dates)
    else:
        # Process recent dates first for testing
        all_dates = sorted([f.stem for f in pipeline.raw_dir.glob("*.csv")])
        recent_dates = all_dates[-10:]  # Last 10 dates
        
        logger.info(f"Processing {len(recent_dates)} most recent dates as a test")
        pipeline.run_pipeline(recent_dates)

if __name__ == "__main__":
    main()
