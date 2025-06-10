#!/usr/bin/env python3
"""
Download all 1-minute bars for Global Crypto, US Indices, and US Stocks
"""

import subprocess
import sys
from datetime import datetime, timedelta

# Asset classes to download
ASSET_CLASSES = [
    'global_crypto',
    'us_indices', 
    'us_stocks_sip'
]

def download_minute_bars(asset_class, days=30):
    """Download minute bars for a given asset class"""
    print(f"\n{'='*60}")
    print(f"Downloading 1-minute bars for {asset_class}")
    print(f"{'='*60}\n")
    
    cmd = [
        'python3',
        'polygon_downloader.py',
        '--asset-class', asset_class,
        '--data-type', 'minute_aggs_v1',
        '--recent-days', str(days)
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✓ Successfully downloaded {asset_class} minute bars")
            print(result.stdout)
        else:
            print(f"✗ Error downloading {asset_class} minute bars")
            print(result.stderr)
    except Exception as e:
        print(f"✗ Exception while downloading {asset_class}: {str(e)}")

def main():
    print("Starting download of 1-minute bars for all requested asset classes")
    print(f"Downloading last 30 days of data (modify 'days' variable to change)")
    
    # You can change the number of days to download
    days_to_download = 30
    
    for asset_class in ASSET_CLASSES:
        download_minute_bars(asset_class, days_to_download)
    
    print("\n" + "="*60)
    print("Download process completed!")
    print("Check /root/stock_project/data/ for your files")
    print("="*60)

if __name__ == '__main__':
    main()
