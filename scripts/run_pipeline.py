#!/usr/bin/env python3
"""
Pipeline orchestration script for ETL data processing.
This script coordinates the execution of raw_to_silver.py, silver_to_gold.py, 
and gold_to_snowflake.py, handling date-specific processing and data retention.
"""
import os
import sys
import json
import logging
import argparse
import traceback
import subprocess
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ===== CONFIG =====
# Base paths
BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__))).parent

# Handle relative vs absolute paths from .env
data_dir_env = os.getenv('DATA_DIR')
if data_dir_env:
    if data_dir_env.startswith('./') or not ('/' in data_dir_env or '\\' in data_dir_env):
        DATA_DIR = BASE_DIR / data_dir_env.lstrip('./')
    else:
        DATA_DIR = Path(data_dir_env)
else:
    DATA_DIR = BASE_DIR / "data"

# Define explicit paths relative to DATA_DIR
LANDING_DIR = DATA_DIR / os.getenv('LANDING_SUBDIR', 'landing')
RAW_DIR = DATA_DIR / os.getenv('RAW_SUBDIR', 'raw')
SILVER_DIR = DATA_DIR / os.getenv('SILVER_SUBDIR', 'silver')
GOLD_DIR = DATA_DIR / os.getenv('GOLD_SUBDIR', 'gold')
LOGS_DIR = DATA_DIR / os.getenv('LOGS_SUBDIR', 'logs')

# Convert paths to absolute for logging
LANDING_DIR = LANDING_DIR.resolve()
RAW_DIR = RAW_DIR.resolve()
SILVER_DIR = SILVER_DIR.resolve()
GOLD_DIR = GOLD_DIR.resolve()
LOGS_DIR = LOGS_DIR.resolve()

# Configuration parameters
EXPECTED_TABLES = os.getenv('EXPECTED_TABLES', "Device,EPG,Playback,User,Smartcard,VODCatalog,VODCatalogExtended").split(',')
MAX_DAYS_BACK = int(os.getenv('MAX_DAYS_BACK', '3'))
DELETE_SILVER_FILES_AFTER_LOAD = os.getenv('DELETE_SILVER_FILES_AFTER_LOAD', 'false').lower() == 'true'
GOLD_RETENTION_DAYS = int(os.getenv('GOLD_RETENTION_DAYS', '30'))
RAW_RETENTION_DAYS = int(os.getenv('RAW_RETENTION_DAYS', '0'))

# Ensure the logs directory exists
LOGS_DIR.mkdir(exist_ok=True, parents=True)

# Configure logging
log_file = LOGS_DIR / "run_pipeline.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def identify_pending_dates(max_days_back=MAX_DAYS_BACK):
    """
    Identifies dates that need processing by scanning the raw directory.
    
    Args:
        max_days_back: Maximum number of days to look back
        
    Returns:
        List of dates (YYYYMMDD format) that need processing
    """
    # Dates to check (today and previous days)
    today = datetime.now().date()
    dates_to_check = [(today - timedelta(days=i)).strftime('%Y%m%d') 
                      for i in range(max_days_back)]
    
    logger.info(f"Checking dates: {dates_to_check}")
    
    # Scan the raw directory for available dates
    available_dates = set()
    
    for date_folder in RAW_DIR.glob('*'):
        # Check if the folder name is a valid date in YYYYMMDD format
        if date_folder.is_dir() and date_folder.name.isdigit() and len(date_folder.name) == 8:
            date_str = date_folder.name
            # Only consider dates within our max_days_back window
            if date_str in dates_to_check:
                available_dates.add(date_str)
    
    logger.info(f"Available dates found in raw directory: {available_dates}")
    
    # Check which dates have already been fully processed
    processed_dates = get_processed_dates()
    logger.info(f"Dates already processed: {processed_dates}")
    
    # Return available dates that haven't been processed
    pending_dates = sorted(list(available_dates - processed_dates))
    logger.info(f"Dates pending processing: {pending_dates}")
    
    return pending_dates

def get_processed_dates():
    """
    Retrieves dates that have already been successfully processed.
    
    Returns:
        Set of processed dates (YYYYMMDD format)
    """
    processed_dates = set()
    
    # Check the tracking file
    tracking_file = LOGS_DIR / "processed_dates.json"
    
    if tracking_file.exists():
        try:
            with open(tracking_file, 'r') as f:
                tracking_data = json.load(f)
                
            # Add dates marked as successfully processed
            for date_str, info in tracking_data.items():
                if info.get('status') == 'success':
                    # Verify that all expected tables have been processed
                    tables_status = info.get('tables', {})
                    all_tables_processed = all(
                        tables_status.get(table, False) 
                        for table in EXPECTED_TABLES 
                        if table in tables_status
                    )
                    
                    if all_tables_processed:
                        processed_dates.add(date_str)
        except Exception as e:
            logger.error(f"Error reading tracking file: {e}")
    
    return processed_dates

def mark_date_as_processed(date_str, table=None, status='success'):
    """
    Marks a date (and optionally a table) as processed.
    
    Args:
        date_str: Date in YYYYMMDD format
        table: Name of the processed table (if None, marks all tables)
        status: Processing status ('success' or 'failed')
    """
    tracking_file = LOGS_DIR / "processed_dates.json"
    
    # Load existing data
    if tracking_file.exists():
        try:
            with open(tracking_file, 'r') as f:
                tracking_data = json.load(f)
        except Exception:
            tracking_data = {}
    else:
        tracking_data = {}
    
    # Initialize entry for this date if it doesn't exist
    if date_str not in tracking_data:
        tracking_data[date_str] = {
            'last_processed': datetime.now().isoformat(),
            'status': status,
            'tables': {table_name: False for table_name in EXPECTED_TABLES}
        }
    
    # Update global status
    if status == 'failed':
        tracking_data[date_str]['status'] = 'failed'
    else:
        # Only update if status isn't already 'failed'
        if tracking_data[date_str]['status'] != 'failed':
            tracking_data[date_str]['status'] = status
    
    # Update last processing timestamp
    tracking_data[date_str]['last_processed'] = datetime.now().isoformat()
    
    # Update table status
    if table:
        # Ensure tables section exists
        if 'tables' not in tracking_data[date_str]:
            tracking_data[date_str]['tables'] = {}
            
        # Mark the table as processed
        tracking_data[date_str]['tables'][table] = (status == 'success')
    elif status == 'success':
        # If no specific table is mentioned and status is success,
        # mark all tables as processed
        if 'tables' not in tracking_data[date_str]:
            tracking_data[date_str]['tables'] = {}
        
        for table_name in EXPECTED_TABLES:
            tracking_data[date_str]['tables'][table_name] = True
    
    # Save changes
    with open(tracking_file, 'w') as f:
        json.dump(tracking_data, f, indent=2)
    
    logger.info(f"Date {date_str} marked as {status}" + 
                (f" for table {table}" if table else ""))

def clean_old_gold_files():
    """
    Removes gold files older than GOLD_RETENTION_DAYS.
    Only removes files, not directories.
    """
    if GOLD_RETENTION_DAYS <= 0:
        logger.info("Gold retention disabled, keeping all files")
        return
    
    logger.info(f"Cleaning gold files older than {GOLD_RETENTION_DAYS} days")
    
    try:
        today = datetime.now().date()
        cutoff_date = today - timedelta(days=GOLD_RETENTION_DAYS)
        cutoff_date_str = cutoff_date.strftime('%Y%m%d')
        
        # Find all parquet files in the gold directory
        for parquet_file in GOLD_DIR.glob('*.parquet'):
            # Extract date from filename (assuming TableName_YYYYMMDD.parquet format)
            parts = parquet_file.stem.split('_')
            if len(parts) >= 2 and parts[-1].isdigit() and len(parts[-1]) == 8:
                file_date = parts[-1]
                
                # Check if file is older than the cutoff date
                if file_date < cutoff_date_str:
                    logger.info(f"Removing old gold file: {parquet_file}")
                    parquet_file.unlink()
        
        logger.info("Gold cleanup completed")
    except Exception as e:
        logger.error(f"Error during gold cleanup: {e}")
        logger.error(traceback.format_exc())

def clean_old_raw_folders():
    """
    Removes raw folders older than RAW_RETENTION_DAYS.
    """
    if RAW_RETENTION_DAYS <= 0:
        logger.info("Raw retention disabled, keeping all folders")
        return
    
    logger.info(f"Cleaning raw folders older than {RAW_RETENTION_DAYS} days")
    
    try:
        today = datetime.now().date()
        cutoff_date = today - timedelta(days=RAW_RETENTION_DAYS)
        cutoff_date_str = cutoff_date.strftime('%Y%m%d')
        
        # Find all date folders in the raw directory
        for date_folder in RAW_DIR.glob('*'):
            if date_folder.is_dir() and date_folder.name.isdigit() and len(date_folder.name) == 8:
                folder_date = date_folder.name
                
                # Check if folder is older than the cutoff date
                if folder_date < cutoff_date_str:
                    logger.info(f"Removing old raw folder: {date_folder}")
                    shutil.rmtree(date_folder)
        
        logger.info("Raw cleanup completed")
    except Exception as e:
        logger.error(f"Error during raw cleanup: {e}")
        logger.error(traceback.format_exc())

def clean_silver_files_after_processing(date_str):
    """
    Removes silver files for a specific date after successful processing if configured.
    
    Args:
        date_str: Date in YYYYMMDD format
    """
    if not DELETE_SILVER_FILES_AFTER_LOAD:
        return
    
    silver_date_dir = SILVER_DIR / date_str
    
    if silver_date_dir.exists():
        logger.info(f"Removing silver files for {date_str}")
        try:
            shutil.rmtree(silver_date_dir)
            logger.info(f"Successfully removed silver directory: {silver_date_dir}")
        except Exception as e:
            logger.error(f"Error removing silver directory {silver_date_dir}: {e}")
            logger.error(traceback.format_exc())

def run_pipeline_step(script_name, date_str=None, additional_args=None):
    """
    Executes a specific pipeline step.
    
    Args:
        script_name: Name of the script to execute (without path)
        date_str: Date to process (optional)
        additional_args: Additional arguments to pass to the script
        
    Returns:
        Tuple (success, output) where success is a boolean and output is the script output
    """
    script_path = BASE_DIR / "scripts" / script_name
    
    if not script_path.exists():
        logger.error(f"Script not found: {script_path}")
        return False, f"Script not found: {script_path}"
    
    cmd = [sys.executable, str(script_path)]
    
    if date_str:
        cmd.extend(["--date", date_str])
    
    if additional_args:
        cmd.extend(additional_args)
    
    logger.info(f"Executing: {' '.join(cmd)}")
    
    try:
        process = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False  # Don't raise exception on error
        )
        
        success = process.returncode == 0
        output = process.stdout
        
        if not success:
            logger.error(f"Failed {script_name}: {process.stderr}")
        else:
            logger.info(f"Successful {script_name}")
        
        return success, output
    
    except Exception as e:
        logger.error(f"Error executing {script_name}: {e}")
        return False, str(e)

def run_pipeline(date_str=None, force=False):
    """
    Executes the complete pipeline (raw -> silver -> gold -> snowflake)
    
    Args:
        date_str: Specific date to process (YYYYMMDD format)
        force: If True, reprocess even already processed dates
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # If no date is specified, identify pending dates
        if not date_str:
            pending_dates = identify_pending_dates()
            if not pending_dates:
                logger.info("No new dates to process")
                return True
                
            logger.info(f"Dates to process: {pending_dates}")
            
            # Process each date
            overall_success = True
            for date in pending_dates:
                result = run_pipeline(date, force)
                overall_success = overall_success and result
                
            return overall_success
        
        # Check if this date has already been processed (unless force=True)
        processed_dates = get_processed_dates()
        if not force and date_str in processed_dates:
            logger.info(f"Date {date_str} has already been successfully processed")
            return True
        
        logger.info(f"====== Starting processing for date: {date_str} ======")
        
        # Step 1: raw_to_silver.py
        logger.info("Step 1: Processing RAW -> SILVER")
        raw_to_silver_success, _ = run_pipeline_step('raw_to_silver.py', date_str)
        
        if not raw_to_silver_success:
            logger.error("Failed at RAW -> SILVER step")
            mark_date_as_processed(date_str, status='failed')
            return False
        
        # Step 2: silver_to_gold.py
        logger.info("Step 2: Processing SILVER -> GOLD")
        silver_to_gold_success, _ = run_pipeline_step('silver_to_gold.py', date_str)
        
        if not silver_to_gold_success:
            logger.error("Failed at SILVER -> GOLD step")
            mark_date_as_processed(date_str, status='failed')
            return False
        
        # Step 3: gold_to_snowflake.py
        logger.info("Step 3: Processing GOLD -> SNOWFLAKE")
        gold_to_snowflake_success, _ = run_pipeline_step('gold_to_snowflake.py', date_str)
        
        if not gold_to_snowflake_success:
            logger.error("Failed at GOLD -> SNOWFLAKE step")
            mark_date_as_processed(date_str, status='failed')
            return False
        
        # If everything went well, mark the date as processed
        logger.info(f"====== Processing successful for date: {date_str} ======")
        mark_date_as_processed(date_str, status='success')
        
        # Clean up silver files if configured
        clean_silver_files_after_processing(date_str)
        
        return True
        
    except Exception as e:
        logger.error(f"Error during pipeline execution: {e}")
        logger.error(traceback.format_exc())
        
        if date_str:
            mark_date_as_processed(date_str, status='failed')
            
        return False

def data_maintenance():
    """Performs data maintenance tasks like cleanup of old files"""
    try:
        logger.info("Performing data maintenance tasks")
        
        # Clean old gold files based on retention policy
        clean_old_gold_files()
        
        # Clean old raw folders based on retention policy
        clean_old_raw_folders()
        
        logger.info("Data maintenance completed")
    except Exception as e:
        logger.error(f"Error during data maintenance: {e}")
        logger.error(traceback.format_exc())

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Execute the complete data processing pipeline")
    parser.add_argument("--date", type=str, help="Specific date to process (YYYYMMDD format)")
    parser.add_argument("--force", action="store_true", help="Reprocess even already processed dates")
    parser.add_argument("--max-days", type=int, default=MAX_DAYS_BACK, 
                        help=f"Maximum number of days to look back (default: {MAX_DAYS_BACK})")
    parser.add_argument("--maintenance", action="store_true", 
                        help="Run data maintenance tasks only (cleanup of old files)")
    return parser.parse_args()

if __name__ == "__main__":
    try:
        # Display configuration information
        logger.info("====== STARTING PIPELINE ======")
        logger.info(f"Base directory: {BASE_DIR}")
        logger.info(f"Data directory: {DATA_DIR}")
        logger.info(f"Expected tables: {EXPECTED_TABLES}")
        
        # Parse arguments
        args = parse_args()
        
        # Adjust maximum days if specified
        if args.max_days != MAX_DAYS_BACK:
            MAX_DAYS_BACK = args.max_days
            logger.info(f"Using MAX_DAYS_BACK = {MAX_DAYS_BACK}")
        
        # If maintenance flag is set, only run maintenance
        if args.maintenance:
            logger.info("Running maintenance tasks only")
            data_maintenance()
            sys.exit(0)
        
        # Execute the pipeline
        start_time = datetime.now()
        
        success = run_pipeline(args.date, args.force)
        
        # Run data maintenance after pipeline
        data_maintenance()
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        # Display summary
        logger.info("\n====== EXECUTION SUMMARY ======")
        logger.info(f"Started at   : {start_time}")
        logger.info(f"Finished at  : {end_time}")
        logger.info(f"Total duration: {duration}")
        logger.info(f"Status      : {'SUCCESS' if success else 'FAILURE'}")
        logger.info("====== PIPELINE COMPLETED ======\n")
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    except Exception as e:
        logger.error(f"CRITICAL ERROR: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)