#!/usr/bin/env python3
"""
Orchestrator for automated crypto data pipeline execution
Can be run via cron or other schedulers
"""

import os
import sys
import json
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
import logging
import smtplib
from email.mime.text import MIMEText

# Configuration
CONFIG = {
    'base_dir': '/root/stock_project/data/global_crypto',
    'scripts_dir': '/root/stock_project/scripts',
    'log_dir': '/root/stock_project/logs',
    'alert_email': None,  # Set to email address for alerts
    'max_retries': 3,
    'mode': 'daily',  # 'daily', 'backfill', or 'specific'
}

# Setup logging
os.makedirs(CONFIG['log_dir'], exist_ok=True)
log_file = Path(CONFIG['log_dir']) / f"orchestrator_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PipelineOrchestrator:
    def __init__(self, config):
        self.config = config
        self.base_dir = Path(config['base_dir'])
        self.state_file = self.base_dir / 'orchestrator_state.json'
        self.state = self.load_state()
    
    def load_state(self):
        """Load orchestrator state from file."""
        if self.state_file.exists():
            with open(self.state_file, 'r') as f:
                return json.load(f)
        return {
            'last_run': None,
            'last_successful_date': None,
            'failures': {},
            'runs': []
        }
    
    def save_state(self):
        """Save orchestrator state to file."""
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f, indent=2)
    
    def get_dates_to_process(self):
        """Determine which dates need processing."""
        dates_to_process = []
        
        if self.config['mode'] == 'daily':
            # Process yesterday's data (for daily runs)
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            if self.check_raw_data_exists(yesterday):
                dates_to_process = [yesterday]
            else:
                logger.warning(f"No raw data found for {yesterday}")
        
        elif self.config['mode'] == 'backfill':
            # Find all unprocessed dates
            raw_dates = set(f.stem for f in (self.base_dir / '1MINUTE_BARS').glob('*.csv'))
            
            # Check which dates have complete aggregations
            for date in sorted(raw_dates):
                if not self.check_aggregations_complete(date):
                    dates_to_process.append(date)
        
        elif self.config['mode'] == 'specific' and len(sys.argv) > 1:
            # Process specific dates from command line
            dates_to_process = sys.argv[1:]
        
        return dates_to_process
    
    def check_raw_data_exists(self, date_str):
        """Check if raw 1-minute data exists for a date."""
        raw_file = self.base_dir / '1MINUTE_BARS' / f'{date_str}.csv'
        return raw_file.exists()
    
    def check_aggregations_complete(self, date_str):
        """Check if all aggregations exist for a date."""
        for interval in [5, 15, 30, 60]:
            agg_file = self.base_dir / f'{interval}MINUTE_BARS' / f'{date_str}.csv'
            if not agg_file.exists():
                return False
        return True
    
    def run_pipeline(self, dates):
        """Run the aggregation pipeline for specified dates."""
        logger.info(f"Running pipeline for {len(dates)} dates")
        
        script_path = Path(self.config['scripts_dir']) / 'crypto_pipeline_v2.py'
        
        # Run pipeline with dates
        cmd = ['python3', str(script_path)] + dates
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logger.info("Pipeline completed successfully")
            logger.debug(f"Pipeline output: {result.stdout}")
            return True
        
        except subprocess.CalledProcessError as e:
            logger.error(f"Pipeline failed: {e}")
            logger.error(f"Error output: {e.stderr}")
            return False
    
    def run_validation(self, date_str):
        """Run validation for a specific date."""
        script_path = Path(self.config['scripts_dir']) / 'crypto_pipeline_monitor.py'
        cmd = ['python3', str(script_path), '--validate', date_str]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            # Check for validation failures in output
            if 'Volume match: ✗' in result.stdout or 'Transaction match: ✗' in result.stdout:
                logger.warning(f"Validation issues found for {date_str}")
                return False
            
            logger.info(f"Validation passed for {date_str}")
            return True
        
        except subprocess.CalledProcessError as e:
            logger.error(f"Validation script failed: {e}")
            return False
    
    def send_alert(self, subject, message):
        """Send email alert (if configured)."""
        if not self.config.get('alert_email'):
            logger.info(f"Alert: {subject} - {message}")
            return
        
        # Email implementation would go here
        # For now, just log the alert
        logger.warning(f"ALERT: {subject} - {message}")
    
    def run(self):
        """Main orchestration logic."""
        run_info = {
            'start_time': datetime.now().isoformat(),
            'mode': self.config['mode'],
            'dates_processed': [],
            'success': True,
            'errors': []
        }
        
        logger.info(f"Starting orchestrator in {self.config['mode']} mode")
        
        try:
            # Get dates to process
            dates = self.get_dates_to_process()
            
            if not dates:
                logger.info("No dates to process")
                run_info['success'] = True
                return
            
            logger.info(f"Found {len(dates)} dates to process: {dates[:5]}...")
            
            # Process in batches to avoid memory issues
            batch_size = 10
            for i in range(0, len(dates), batch_size):
                batch = dates[i:i + batch_size]
                
                # Run pipeline
                if self.run_pipeline(batch):
                    # Validate each processed date
                    for date in batch:
                        if self.run_validation(date):
                            run_info['dates_processed'].append(date)
                            self.state['last_successful_date'] = date
                            
                            # Clear any previous failures for this date
                            if date in self.state.get('failures', {}):
                                del self.state['failures'][date]
                        else:
                            error_msg = f"Validation failed for {date}"
                            run_info['errors'].append(error_msg)
                            self.state['failures'][date] = {
                                'error': error_msg,
                                'timestamp': datetime.now().isoformat()
                            }
                else:
                    error_msg = f"Pipeline failed for batch starting with {batch[0]}"
                    run_info['errors'].append(error_msg)
                    run_info['success'] = False
                    
                    # Record failures
                    for date in batch:
                        self.state['failures'][date] = {
                            'error': 'Pipeline execution failed',
                            'timestamp': datetime.now().isoformat()
                        }
            
            # Check for persistent failures
            if len(self.state.get('failures', {})) > 5:
                self.send_alert(
                    "Crypto Pipeline Alert",
                    f"Multiple persistent failures: {list(self.state['failures'].keys())[:10]}"
                )
        
        except Exception as e:
            logger.error(f"Orchestrator error: {str(e)}")
            run_info['success'] = False
            run_info['errors'].append(str(e))
            self.send_alert("Crypto Pipeline Critical Error", str(e))
        
        finally:
            # Update state
            run_info['end_time'] = datetime.now().isoformat()
            self.state['last_run'] = run_info['end_time']
            self.state['runs'].append(run_info)
            
            # Keep only last 100 runs
            if len(self.state['runs']) > 100:
                self.state['runs'] = self.state['runs'][-100:]
            
            self.save_state()
            
            # Final summary
            logger.info(f"Orchestrator complete. Processed {len(run_info['dates_processed'])} dates")
            if run_info['errors']:
                logger.warning(f"Errors encountered: {len(run_info['errors'])}")

def setup_cron():
    """Helper to setup cron job."""
    print("To setup daily automated runs, add this to your crontab:")
    print("0 1 * * * /usr/bin/python3 /root/stock_project/scripts/crypto_pipeline_orchestrator.py")
    print("\nOr for hourly checks:")
    print("0 * * * * /usr/bin/python3 /root/stock_project/scripts/crypto_pipeline_orchestrator.py")

def main():
    if len(sys.argv) > 1 and sys.argv[1] == '--setup-cron':
        setup_cron()
        return
    
    # Allow mode override from command line
    if len(sys.argv) > 1 and sys.argv[1] in ['daily', 'backfill', 'specific']:
        CONFIG['mode'] = sys.argv[1]
    
    orchestrator = PipelineOrchestrator(CONFIG)
    orchestrator.run()

if __name__ == "__main__":
    main()
