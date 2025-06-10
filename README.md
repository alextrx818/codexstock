# Polygon.io Flat Files Downloader

This project provides tools to download historical market data from Polygon.io Flat Files and browse them through a Windows Explorer-like web interface.

## Components

### 1. Python Downloader (`polygon_downloader.py`)
A Python script that uses Boto3 to download data from Polygon.io's S3-compatible endpoint.

### 2. File Browser (Web UI on port 8080)
A web-based file manager with a Windows Explorer-like interface for browsing and managing downloaded files.

## Directory Structure
```
/root/stock_project/
├── polygon_downloader.py    # Main downloader script
├── data/                    # Downloaded data files (organized by asset class)
├── filebrowser/             # File browser configuration
└── README.md               # This file
```

## Quick Start

### Using the Python Downloader

1. **List available files:**
   ```bash
   python polygon_downloader.py --list --prefix us_stocks_sip/trades_v1/2024/
   ```

2. **Download recent data (last 7 days):**
   ```bash
   python polygon_downloader.py --asset-class us_stocks_sip --data-type trades_v1 --recent-days 7
   ```

3. **Download specific date range:**
   ```bash
   python polygon_downloader.py --asset-class us_stocks_sip --data-type trades_v1 --start-date 2024-03-01 --end-date 2024-03-07
   ```

4. **Download a specific file:**
   ```bash
   python polygon_downloader.py --specific-file us_stocks_sip/trades_v1/2024/03/2024-03-07.csv.gz
   ```

### Available Asset Classes and Data Types

- **us_stocks_sip**: trades_v1, quotes_v1, minute_aggs_v1, day_aggs_v1
- **us_options_opra**: trades_v1, quotes_v1, minute_aggs_v1, day_aggs_v1
- **us_indices**: minute_aggs_v1, day_aggs_v1
- **global_crypto**: trades_v1, quotes_v1, minute_aggs_v1, day_aggs_v1
- **global_forex**: quotes_v1, minute_aggs_v1, day_aggs_v1

### Options
- `--no-decompress`: Keep files compressed (.gz format)
- `--list`: List available files with a given prefix
- `--prefix`: Specify prefix for listing files

## File Browser Web UI

Access the file browser at: http://YOUR_SERVER_IP:8080

- **Default login:** admin/admin (change on first login!)
- **Features:**
  - Windows Explorer-like interface
  - Drag and drop files
  - Copy/paste functionality
  - Create folders
  - Upload/download files
  - Preview files

### Managing the File Browser Service

```bash
# Check status
systemctl status filebrowser

# Stop service
systemctl stop filebrowser

# Start service
systemctl start filebrowser

# View logs
journalctl -u filebrowser -f
```

## Data Format

Downloaded CSV files include headers. Example for minute aggregates:

```csv
ticker,volume,open,close,high,low,window_start,transactions
AAPL,4930,200.29,200.5,200.63,200.29,1744792500000000000,129
AAPL,1815,200.39,200.34,200.61,200.34,1744792560000000000,57
```

## Notes

- Data for each trading day is typically available by 11:00 AM ET the following day
- Files are automatically decompressed by default (use --no-decompress to keep them compressed)
- Downloaded files are organized in `/root/stock_project/data/` by asset class and data type
- The downloader supports parallel downloads for faster retrieval of multiple files
