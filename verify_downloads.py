#!/usr/bin/env python3
"""
Verify that all trading days for the last 2 years have been downloaded
"""

import os
import glob
from datetime import datetime, timedelta
from collections import defaultdict

def get_directory_size(path):
    """Calculate total size of a directory in GB"""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            total_size += os.path.getsize(filepath)
    return total_size / (1024**3)  # Convert to GB

def get_us_trading_days(start_date, end_date):
    """Get all US trading days between two dates (excludes weekends and major holidays)"""
    # Major US market holidays
    holidays = {
        # 2023 holidays
        '2023-01-02',  # New Year's Day (observed)
        '2023-01-16',  # MLK Day
        '2023-02-20',  # Presidents Day
        '2023-04-07',  # Good Friday
        '2023-05-29',  # Memorial Day
        '2023-06-19',  # Juneteenth
        '2023-07-04',  # Independence Day
        '2023-09-04',  # Labor Day
        '2023-11-23',  # Thanksgiving
        '2023-12-25',  # Christmas
        
        # 2024 holidays
        '2024-01-01',  # New Year's Day
        '2024-01-15',  # MLK Day
        '2024-02-19',  # Presidents Day
        '2024-03-29',  # Good Friday
        '2024-05-27',  # Memorial Day
        '2024-06-19',  # Juneteenth
        '2024-07-04',  # Independence Day
        '2024-09-02',  # Labor Day
        '2024-11-28',  # Thanksgiving
        '2024-12-25',  # Christmas
        
        # 2025 holidays (partial)
        '2025-01-01',  # New Year's Day
        '2025-01-20',  # MLK Day
        '2025-02-17',  # Presidents Day
        '2025-04-18',  # Good Friday
        '2025-05-26',  # Memorial Day
    }
    
    trading_days = []
    current = start_date
    
    while current <= end_date:
        # Skip weekends (Saturday=5, Sunday=6)
        if current.weekday() < 5 and current.strftime('%Y-%m-%d') not in holidays:
            trading_days.append(current)
        current += timedelta(days=1)
    
    return trading_days

def verify_asset_class(asset_class, base_dir='/root/stock_project/data'):
    """Verify downloads for a specific asset class"""
    print(f"\n{'='*80}")
    print(f"Verifying {asset_class}")
    print(f"{'='*80}")
    
    # Get all downloaded files
    pattern = os.path.join(base_dir, asset_class, '1MINUTE_BARS', '*.csv')
    files = glob.glob(pattern)
    
    # Extract dates from filenames
    downloaded_dates = set()
    for file in files:
        filename = os.path.basename(file)
        date_str = filename.replace('.csv', '')
        try:
            date = datetime.strptime(date_str, '%Y-%m-%d').date()
            downloaded_dates.add(date)
        except:
            pass
    
    # Calculate date range (2 years back from yesterday)
    end_date = datetime.now().date() - timedelta(days=1)
    start_date = end_date - timedelta(days=730)
    
    # Get expected dates based on asset class
    if asset_class == 'global_crypto':
        # Crypto trades every day
        expected_dates = []
        current = start_date
        while current <= end_date:
            expected_dates.append(current)
            current += timedelta(days=1)
    else:
        # US markets only trade on weekdays (excluding holidays)
        expected_dates = get_us_trading_days(start_date, end_date)
    
    # Find missing dates
    expected_set = set(expected_dates)
    missing_dates = expected_set - downloaded_dates
    extra_dates = downloaded_dates - expected_set
    
    # Calculate statistics
    total_size_gb = get_directory_size(os.path.join(base_dir, asset_class))
    
    # Print results
    print(f"Date range: {start_date} to {end_date}")
    print(f"Expected files: {len(expected_dates)}")
    print(f"Downloaded files: {len(downloaded_dates)}")
    print(f"Missing files: {len(missing_dates)}")
    print(f"Total size: {total_size_gb:.2f} GB")
    
    if missing_dates:
        print(f"\nMissing dates ({len(missing_dates)}):")
        for date in sorted(missing_dates)[:10]:  # Show first 10
            print(f"  - {date}")
        if len(missing_dates) > 10:
            print(f"  ... and {len(missing_dates) - 10} more")
    
    # File size analysis
    if files:
        sizes = [os.path.getsize(f) / (1024**2) for f in files]  # MB
        avg_size = sum(sizes) / len(sizes)
        print(f"\nAverage file size: {avg_size:.2f} MB")
        print(f"Largest file: {max(sizes):.2f} MB")
        print(f"Smallest file: {min(sizes):.2f} MB")
    
    return {
        'asset_class': asset_class,
        'expected': len(expected_dates),
        'downloaded': len(downloaded_dates),
        'missing': len(missing_dates),
        'size_gb': total_size_gb,
        'missing_dates': sorted(missing_dates)
    }

def main():
    """Main verification function"""
    print("Polygon.io 2-Year Download Verification Report")
    print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    asset_classes = ['global_crypto', 'us_indices', 'us_stocks_sip']
    results = []
    total_size = 0
    
    for asset_class in asset_classes:
        result = verify_asset_class(asset_class)
        results.append(result)
        total_size += result['size_gb']
    
    # Summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    print(f"Total storage used: {total_size:.2f} GB")
    print(f"\nAsset Class Summary:")
    print(f"{'Asset Class':<20} {'Expected':<10} {'Downloaded':<12} {'Missing':<10} {'Size (GB)':<10}")
    print("-" * 70)
    
    for r in results:
        print(f"{r['asset_class']:<20} {r['expected']:<10} {r['downloaded']:<12} {r['missing']:<10} {r['size_gb']:<10.2f}")
    
    # Overall completion percentage
    total_expected = sum(r['expected'] for r in results)
    total_downloaded = sum(r['downloaded'] for r in results)
    completion_pct = (total_downloaded / total_expected) * 100 if total_expected > 0 else 0
    
    print(f"\nOverall completion: {completion_pct:.1f}% ({total_downloaded}/{total_expected} files)")
    
    # Check if download is still running
    if any(r['missing'] > 0 for r in results):
        print("\n⚠️  Some files are missing. The download may still be in progress.")
        print("Run this script again after the download completes for final verification.")

if __name__ == '__main__':
    main()
