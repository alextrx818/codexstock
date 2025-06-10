#!/usr/bin/env python3
"""
Download Monitor with Progress Tracking for Polygon.io Data
"""

import os
import sys
import time
import subprocess
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Import the downloader
sys.path.append('/root/stock_project')
from polygon_downloader import PolygonDownloader, ASSET_CLASSES

class DownloadMonitor:
    def __init__(self):
        self.downloader = PolygonDownloader()
        self.total_files = 0
        self.completed_files = 0
        self.failed_files = 0
        self.start_time = None
        self.current_asset = ""
        self.lock = threading.Lock()
        
    def estimate_file_count(self, asset_class, data_type, days):
        """Estimate number of files to download"""
        # Crypto trades 7 days a week, stocks/indices only weekdays
        if asset_class == 'global_crypto':
            return days
        else:
            # Roughly 5 trading days per week
            return int(days * 5 / 7)
    
    def format_time(self, seconds):
        """Format seconds into human readable time"""
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            return f"{int(seconds/60)}m {int(seconds%60)}s"
        else:
            hours = int(seconds/3600)
            minutes = int((seconds%3600)/60)
            return f"{hours}h {minutes}m"
    
    def print_progress(self):
        """Print progress bar and stats"""
        with self.lock:
            if self.total_files == 0:
                return
                
            percent = (self.completed_files / self.total_files) * 100
            bar_length = 50
            filled_length = int(bar_length * self.completed_files // self.total_files)
            bar = '█' * filled_length + '-' * (bar_length - filled_length)
            
            # Calculate time stats
            elapsed_time = time.time() - self.start_time
            if self.completed_files > 0:
                avg_time_per_file = elapsed_time / self.completed_files
                remaining_files = self.total_files - self.completed_files
                eta = avg_time_per_file * remaining_files
            else:
                eta = 0
            
            # Clear line and print progress
            print(f'\r{" "*100}', end='')  # Clear line
            print(f'\r[{bar}] {percent:.1f}%', end='')
            print(f' | {self.completed_files}/{self.total_files} files', end='')
            print(f' | Elapsed: {self.format_time(elapsed_time)}', end='')
            print(f' | ETA: {self.format_time(eta)}', end='')
            print(f' | Current: {self.current_asset}', end='', flush=True)
    
    def download_with_progress(self, s3_key):
        """Download a single file and update progress"""
        try:
            result = self.downloader.download_file(s3_key, decompress=True)
            with self.lock:
                if result:
                    self.completed_files += 1
                else:
                    self.failed_files += 1
                    self.completed_files += 1
            self.print_progress()
            return result
        except Exception as e:
            with self.lock:
                self.failed_files += 1
                self.completed_files += 1
            self.print_progress()
            return None
    
    def download_asset_class(self, asset_class, data_type, days):
        """Download data for a specific asset class with progress tracking"""
        print(f"\n\n{'='*80}")
        print(f"Downloading {data_type} for {asset_class}")
        print(f"{'='*80}")
        
        self.current_asset = asset_class
        
        # Calculate date range
        end_date = datetime.now().date() - timedelta(days=1)
        start_date = end_date - timedelta(days=days-1)
        
        # Generate list of files to download
        files_to_download = []
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            year = current_date.strftime('%Y')
            month = current_date.strftime('%m')
            s3_key = f"{asset_class}/{data_type}/{year}/{month}/{date_str}.csv.gz"
            files_to_download.append(s3_key)
            current_date += timedelta(days=1)
        
        # Update total files count
        with self.lock:
            self.total_files += len(files_to_download)
        
        # Download files with thread pool
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(self.download_with_progress, f) for f in files_to_download]
            for future in as_completed(futures):
                pass  # Progress is updated in download_with_progress
        
        print(f"\n✓ Completed {asset_class}")
    
    def run(self, asset_classes, data_type='minute_aggs_v1', days=30):
        """Run the download monitor for specified asset classes"""
        self.start_time = time.time()
        
        print(f"Starting monitored download")
        print(f"Asset classes: {', '.join(asset_classes)}")
        print(f"Data type: {data_type}")
        print(f"Days to download: {days}")
        print(f"Estimated total files: ~{sum(self.estimate_file_count(ac, data_type, days) for ac in asset_classes)}")
        
        for asset_class in asset_classes:
            self.download_asset_class(asset_class, data_type, days)
        
        # Final summary
        total_time = time.time() - self.start_time
        print(f"\n\n{'='*80}")
        print(f"DOWNLOAD COMPLETE!")
        print(f"{'='*80}")
        print(f"Total files downloaded: {self.completed_files - self.failed_files}")
        print(f"Failed downloads: {self.failed_files}")
        print(f"Total time: {self.format_time(total_time)}")
        print(f"Average speed: {self.completed_files / total_time:.2f} files/second")
        print(f"\nFiles saved to: /root/stock_project/data/")
        print(f"{'='*80}")

def main():
    # Configuration
    ASSET_CLASSES_TO_DOWNLOAD = [
        'global_crypto',
        'us_indices',
        'us_stocks_sip'
    ]
    DATA_TYPE = 'minute_aggs_v1'
    DAYS_TO_DOWNLOAD = 730  # 2 years
    
    # Create and run monitor
    monitor = DownloadMonitor()
    monitor.run(ASSET_CLASSES_TO_DOWNLOAD, DATA_TYPE, DAYS_TO_DOWNLOAD)

if __name__ == '__main__':
    main()
