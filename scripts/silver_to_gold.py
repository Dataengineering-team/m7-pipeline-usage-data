#!/usr/bin/env python3
"""
This script processes Parquet files from the silver layer and consolidates them into the gold layer.
It supports date-specific processing and data validation based on business rules.
"""
import os
import json
import re
import logging
import traceback
import argparse
import pandas as pd
import numpy as np
import sys
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import pyarrow as pa
import pyarrow.parquet as pq
import snowflake.connector
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.validation_utils import load_validation_rules, validate_with_rules

# Load environment variables from .env file
load_dotenv()

# ===== CONFIG =====
# Base paths
BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__))).parent

# Handle relative vs absolute paths from .env
data_dir_env = os.getenv('DATA_DIR')
if data_dir_env:
    # If path starts with ./ or is relative without slash, consider it relative to BASE_DIR
    if data_dir_env.startswith('./') or not ('/' in data_dir_env or '\\' in data_dir_env):
        DATA_DIR = BASE_DIR / data_dir_env.lstrip('./')
    # Otherwise use the path as is (absolute)
    else:
        DATA_DIR = Path(data_dir_env)
else:
    # Default value if DATA_DIR is not defined
    DATA_DIR = BASE_DIR / "data"

# Define explicit paths relative to DATA_DIR
SILVER_DIR = DATA_DIR / os.getenv('SILVER_SUBDIR', 'silver')
GOLD_DIR = DATA_DIR / os.getenv('GOLD_SUBDIR', 'gold')
LOGS_DIR = DATA_DIR / os.getenv('LOGS_SUBDIR', 'logs')

# Convert paths to absolute for logging
SILVER_DIR = SILVER_DIR.resolve()
GOLD_DIR = GOLD_DIR.resolve()
LOGS_DIR = LOGS_DIR.resolve()

# Processing parameters
EXPECTED_TABLES = os.getenv('EXPECTED_TABLES', "Device,EPG,Playback,User,Smartcard,VODCatalog,VODCatalogExtended").split(',')
MOCK_SNOWFLAKE = os.getenv('MOCK_SNOWFLAKE', 'true').lower() == 'true'
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
PARALLEL_PROCESSING = os.getenv('PARALLEL_PROCESSING', 'true').lower() == 'true'
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '4'))
DELETE_SILVER_FILES_AFTER_LOAD = os.getenv('DELETE_SILVER_FILES_AFTER_LOAD', 'false').lower() == 'true'

# Snowflake parameters
SF_USER = os.getenv('SNOWFLAKE_USER')
SF_PWD = os.getenv('SNOWFLAKE_PASSWORD')
SF_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SF_WHS = os.getenv('SNOWFLAKE_WAREHOUSE')
SF_DB = os.getenv('SNOWFLAKE_DATABASE')
SF_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'STG_SG_MONITORING')

# Ensure these directories exist
SILVER_DIR.mkdir(exist_ok=True, parents=True)
GOLD_DIR.mkdir(exist_ok=True, parents=True)
LOGS_DIR.mkdir(exist_ok=True, parents=True)

# Configure logging
log_file = LOGS_DIR / "silver_to_gold_execution.log"
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load validation rules
validation_rules = load_validation_rules()

# Custom JSON Encoder class to handle serialization
class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle non-serializable types"""
    def default(self, obj):
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        try:
            return json.JSONEncoder.default(self, obj)
        except TypeError:
            return str(obj)

# Display configuration at startup
def log_config():
    """Log the current configuration"""
    config = {
        "BASE_DIR": str(BASE_DIR),
        "DATA_DIR": str(DATA_DIR),
        "SILVER_DIR": str(SILVER_DIR),
        "GOLD_DIR": str(GOLD_DIR),
        "LOGS_DIR": str(LOGS_DIR),
        "EXPECTED_TABLES": EXPECTED_TABLES,
        "MOCK_SNOWFLAKE": MOCK_SNOWFLAKE,
        "LOG_LEVEL": LOG_LEVEL,
        "PARALLEL_PROCESSING": PARALLEL_PROCESSING,
        "MAX_WORKERS": MAX_WORKERS,
        "DELETE_SILVER_FILES_AFTER_LOAD": DELETE_SILVER_FILES_AFTER_LOAD
    }
    logger.info("Configuration:")
    for key, value in config.items():
        logger.info(f"  {key}: {value}")

# Utility: Extract brand and date from path
def extract_info_from_path(file_path):
    """
    Extracts brand and date from file path
    Example: silver/20250416/canal_digitaal/Device.parquet -> ('canal_digitaal', '20250416')
    """
    try:
        # Brand name is in the parent directory of the file
        brand_slug = file_path.parent.name
        # Convert slug to more readable name
        brand = brand_slug.replace('_', ' ').title()
        
        # Date is in the parent of the parent directory (structure: silver/DATE/BRAND/FILE.parquet)
        file_date = file_path.parent.parent.name
        
        return brand, file_date
    except Exception as e:
        logger.error(f"Error extracting information from {file_path}: {e}")
        return "unknown_brand", "unknown_date"

# Local function to simulate log insertion into Snowflake
def insert_log_local(proc_date, brand, extraction_date, table_name, rows_total, rows_ok, rows_ko, cols_error, status, details):
    """
    Local version of insert_log that writes to a JSON file
    """
    log_entry = {
        "PROCESS_DATE": proc_date,
        "BRAND": brand,
        "EXTRACTION_DATE": extraction_date,
        "TABLE_NAME": table_name,
        "ROWS_TOTAL": rows_total,
        "ROWS_OK": rows_ok,
        "ROWS_KO": rows_ko,
        "COLS_ERROR": cols_error,
        "STATUS": status,
        "DETAILS": details
    }
    
    # Create a unique filename based on date, brand and table
    filename = LOGS_DIR / f"silver_logs_{brand}_{table_name}_{extraction_date}.json"
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            # Use the custom encoder to handle non-serializable types
            json.dump(log_entry, f, indent=2, cls=CustomJSONEncoder)
        
        logger.info(f"Local log created: {filename}")
    except Exception as e:
        logger.error(f"Error writing local log: {e}")
        logger.error(traceback.format_exc())

# Log insertion into Snowflake
def insert_log_snowflake(proc_date, brand, extraction_date, table_name, rows_total, rows_ok, rows_ko, cols_error, status, details):
    """
    Inserts a log record into Snowflake
    """
    try:
        # Convert details to serializable JSON
        details_json = json.dumps(details, cls=CustomJSONEncoder)
        
        ctx = snowflake.connector.connect(
            user=SF_USER,
            password=SF_PWD,
            account=SF_ACCOUNT,
            warehouse=SF_WHS,
            database=SF_DB,
            schema=SF_SCHEMA
        )
        cs = ctx.cursor()
        
        sql = f"""
            INSERT INTO {SF_SCHEMA}.SILVER_LOGS
                (PROCESS_DATE, BRAND, EXTRACTION_DATE, TABLE_NAME, ROWS_TOTAL, ROWS_OK, ROWS_KO, COLS_ERROR, STATUS, DETAILS)
            VALUES (
                TO_TIMESTAMP_NTZ('{proc_date}'),
                '{brand}',
                TO_DATE('{extraction_date}'),
                '{table_name}',
                {rows_total},
                {rows_ok},
                {rows_ko},
                '{cols_error}',
                '{status}',
                PARSE_JSON('{details_json.replace("'", "''")}')
            );
        """
        cs.execute(sql)
        ctx.commit()
        logger.info(f"Inserted log for {brand} {table_name} {extraction_date} status={status}")
    except Exception as e:
        logger.error(f"Snowflake log error: {e}")
        logger.error(traceback.format_exc())
    finally:
        try:
            cs.close()
            ctx.close()
        except Exception:
            pass

# Validation function based on external rules
def get_validation_function(table_name):
    """
    Returns a validation function that applies the rules defined in the configuration file
    for the specified table
    """
    def validate_with_loaded_rules(df):
        """Validation function that uses loaded rules for the table"""
        return validate_with_rules(df, table_name, validation_rules)
    
    return validate_with_loaded_rules

# Harmonize data types across DataFrames
def harmonize_dataframe_types(dfs):
    """
    Harmonizes data types across multiple DataFrames to avoid conversion errors
    during merge and Parquet writing
    """
    if not dfs or len(dfs) == 0:
        return []
    
    # Identify all columns across all DataFrames
    all_columns = set()
    for df in dfs:
        all_columns.update(df.columns)
    
    harmonized_dfs = []
    
    for df in dfs:
        # Create a copy to avoid modifying the original
        df_copy = df.copy()
        
        # For each column in this DataFrame
        for col in df_copy.columns:
            # If column contains Python objects, convert to string
            if df_copy[col].dtype == 'object':
                # Convert NaN to None
                df_copy[col] = df_copy[col].where(pd.notna(df_copy[col]), None)
                
                # Convert all elements to strings to avoid mixed types
                df_copy[col] = df_copy[col].apply(lambda x: str(x) if x is not None else None)
            
            # Handle specific problematic columns
            if col == 'SSOID':
                # Convert SSOID to string to avoid "Expected bytes, got a 'float' object" error
                df_copy[col] = df_copy[col].apply(lambda x: str(x) if x is not None and pd.notna(x) else None)
        
        harmonized_dfs.append(df_copy)
    
    return harmonized_dfs

# Determine base table name from filename
def get_table_name_from_file(file_path):
    """
    Extracts base table name from filename
    Examples:
    - 'Canal Digitaal_Device_20250416.parquet' -> 'Device'
    - 'Canal Digitaal - Resellers_Playback_20250416.parquet' -> 'Playback'
    """
    filename = file_path.name
    # Remove extension
    if filename.lower().endswith('.parquet'):
        filename = filename[:-8]  # Remove '.parquet'
    
    # Find name parts
    parts = filename.split('_')
    
    # Table name is usually the second to last part
    # (Brand_Table_Date or Brand_SubBrand_Table_Date)
    if len(parts) >= 2:
        # If the last part is a date, table is the second to last part
        if parts[-1].isdigit() and len(parts[-1]) == 8:  # YYYYMMDD date format
            return parts[-2]
    
    # Detect known table names in the filename
    for known_table in EXPECTED_TABLES:
        if known_table in filename:
            return known_table
    
    # Fallback - use file stem without extension
    return file_path.stem

# Process individual Parquet file
def process_single_parquet(file_path):
    """
    Processes a single Parquet file, adds BRAND and FILEDATE columns and performs validations
    Returns an enriched DataFrame and validation information
    """
    try:
        # Read Parquet file
        df = pd.read_parquet(file_path)
        
        # Extract important information
        brand, file_date = extract_info_from_path(file_path)
        
        # Determine table name from filename
        table_name = get_table_name_from_file(file_path)
        
        # Format extraction date for display
        extraction_date = file_date
        if len(file_date) == 8:  # YYYYMMDD format
            extraction_date = f"{file_date[:4]}-{file_date[4:6]}-{file_date[6:8]}"
        
        logger.info(f"Processing {file_path}: brand={brand}, table={table_name}, date={file_date}")
        
        # Add BRAND and FILEDATE columns
        df['BRAND'] = brand
        df['FILEDATE'] = file_date
        
        # Perform validations specific to this table type
        validation_function = get_validation_function(table_name)
        validation_errors, error_rows = validation_function(df)
        
        # Calculate statistics
        rows_total = len(df)
        rows_ko = len(error_rows)
        rows_ok = rows_total - rows_ko
        
        # Create validation report
        validation_report = {
            'file_path': str(file_path),
            'brand': brand,
            'table_name': table_name,
            'extraction_date': extraction_date,
            'file_date': file_date,
            'rows_total': rows_total,
            'rows_ok': rows_ok,
            'rows_ko': rows_ko,
            'errors': validation_errors,
            'error_rows': error_rows[:100] if len(error_rows) > 100 else error_rows  # Limit number of errors to avoid too large logs
        }
        
        return df, validation_report
    
    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")
        logger.error(traceback.format_exc())
        # Return empty DataFrame and error report
        return pd.DataFrame(), {
            'file_path': str(file_path),
            'error': str(e),
            'traceback': traceback.format_exc()
        }

# Clean up silver files after successful processing
def clean_silver_files(date_str):
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
            # Instead of removing the entire directory, just remove .parquet files
            # to preserve the directory structure
            for parquet_file in silver_date_dir.glob('**/*.parquet'):
                parquet_file.unlink()
                logger.debug(f"Removed silver file: {parquet_file}")
            
            logger.info(f"Successfully removed silver files for {date_str}")
        except Exception as e:
            logger.error(f"Error removing silver files for {date_str}: {e}")
            logger.error(traceback.format_exc())

# Main processing function
def process_silver_to_gold(specific_date=None):
    """
    Processes files from Silver layer to convert them to Gold layer
    
    Args:
        specific_date: Optional date string in YYYYMMDD format to filter processing
        
    Returns:
        Boolean indicating success
    """
    logger.info("Starting SILVER -> GOLD processing")
    if specific_date:
        logger.info(f"Processing restricted to date: {specific_date}")
    
    # 1. Group all Parquet files by table type
    parquet_files_by_table = {}
    table_dates = {}  # To store unique dates per table
    
    try:
        # Process pattern based on whether specific_date is provided
        pattern = f"{specific_date}/**/*.parquet" if specific_date else "**/*.parquet"
        
        # Browse all Parquet files in Silver layer
        for parquet_file in SILVER_DIR.glob(pattern):
            # Extract table name from filename
            table_name = get_table_name_from_file(parquet_file)
            
            # Extract file date
            _, file_date = extract_info_from_path(parquet_file)
            
            # Group files by table
            if table_name not in parquet_files_by_table:
                parquet_files_by_table[table_name] = []
                table_dates[table_name] = set()
            
            parquet_files_by_table[table_name].append(parquet_file)
            table_dates[table_name].add(file_date)
            
    except Exception as e:
        logger.error(f"Error scanning Parquet files: {e}")
        logger.error(traceback.format_exc())
        return False
    
    # If no files found, terminate
    if not parquet_files_by_table:
        logger.warning("No Parquet files found in Silver layer")
        return False
    
    logger.info(f"Tables found: {list(parquet_files_by_table.keys())}")
    
    # 2. Process each table type
    all_validations = []
    success = True
    
    for table_name, files in parquet_files_by_table.items():
        logger.info(f"Processing table {table_name} ({len(files)} files)")
        
        # Create Gold directory if it doesn't exist
        GOLD_DIR.mkdir(exist_ok=True, parents=True)
        
        # For each unique date of this table
        for file_date in table_dates[table_name]:
            # Filter files for this date
            date_files = [f for f in files if extract_info_from_path(f)[1] == file_date]
            
            if not date_files:
                continue
                
            processed_dfs = []
            table_validations = []
            
            # Parallel or sequential processing
            if PARALLEL_PROCESSING and len(date_files) > 1:
                futures = {}
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    # Submit tasks
                    for file_path in date_files:
                        future = executor.submit(process_single_parquet, file_path)
                        futures[future] = file_path
                    
                    # Collect results
                    for future in as_completed(futures):
                        file_path = futures[future]
                        try:
                            df, validation = future.result()
                            if not df.empty:
                                processed_dfs.append(df)
                                table_validations.append(validation)
                        except Exception as e:
                            logger.error(f"Error in parallel processing of {file_path}: {e}")
                            logger.error(traceback.format_exc())
            else:
                # Sequential processing
                for file_path in date_files:
                    df, validation = process_single_parquet(file_path)
                    if not df.empty:
                        processed_dfs.append(df)
                        table_validations.append(validation)
            
            # If there are no valid data, move to next date
            if not processed_dfs:
                logger.warning(f"No valid data for table {table_name} on date {file_date}")
                continue
            
            # 3. Merge all DataFrames for this table and date
            try:
                # Harmonize data types before merging
                harmonized_dfs = harmonize_dataframe_types(processed_dfs)
                
                # Merge harmonized DataFrames
                merged_df = pd.concat(harmonized_dfs, ignore_index=True)
                
                # Path of Gold file for this table and date
                gold_file_path = GOLD_DIR / f"{table_name}_{file_date}.parquet"
                
                # Write Gold file with error handling
                try:
                    # Try to write file directly
                    merged_df.to_parquet(gold_file_path, index=False)
                except Exception as e:
                    logger.warning(f"Error in direct writing of {gold_file_path}: {e}")
                    # Alternative strategy: convert to CSV then load again
                    csv_temp = LOGS_DIR / f"temp_{table_name}_{file_date}.csv"
                    logger.info(f"Trying conversion through CSV: {csv_temp}")
                    
                    # Write to CSV
                    merged_df.to_csv(csv_temp, index=False)
                    
                    # Reload CSV and write to Parquet
                    temp_df = pd.read_csv(csv_temp)
                    # Resolve data type issues
                    for col in temp_df.columns:
                        if col == 'SSOID' or col.endswith('ID') or 'id' in col.lower():
                            temp_df[col] = temp_df[col].astype(str)
                    
                    temp_df.to_parquet(gold_file_path, index=False)
                    
                    # Clean up
                    if csv_temp.exists():
                        os.remove(csv_temp)
                
                logger.info(f"Gold file created: {gold_file_path} ({len(merged_df)} rows)")
                
                # 4. Write logs for each brand/table
                brands = merged_df['BRAND'].unique()
                
                for brand in brands:
                    brand_df = merged_df[merged_df['BRAND'] == brand]
                    
                    # Find validations corresponding to this brand
                    brand_validations = [v for v in table_validations if v.get('brand') == brand]
                    
                    # Aggregate errors
                    cols_error = []
                    rows_total = len(brand_df)
                    rows_ko = 0
                    rows_ok = rows_total
                    details = {}
                    
                    for v in brand_validations:
                        if 'errors' in v and v['errors']:
                            for error_type, error_info in v['errors'].items():
                                if error_type not in details:
                                    details[error_type] = error_info
                                else:
                                    # Merge error information
                                    if isinstance(error_info, list):
                                        if isinstance(details[error_type], list):
                                            details[error_type].extend(error_info)
                                        else:
                                            details[error_type] = error_info
                                    elif isinstance(error_info, dict):
                                        if isinstance(details[error_type], dict):
                                            details[error_type].update(error_info)
                                        else:
                                            details[error_type] = error_info
                                    else:
                                        # For numeric counters, add them
                                        if isinstance(details[error_type], (int, float)) and isinstance(error_info, (int, float)):
                                            details[error_type] += error_info
                                        else:
                                            details[error_type] = error_info
                                
                                # Add error type to the list of error columns
                                if error_type not in cols_error:
                                    cols_error.append(error_type)
                        
                        # Update counters
                        rows_ko += v.get('rows_ko', 0)
                    
                    rows_ok = rows_total - rows_ko
                    
                    # Determine status
                    status = "OK" if rows_ko == 0 else "KO"
                    
                    # Extraction date format (YYYY-MM-DD)
                    extraction_date = brand_validations[0].get('extraction_date') if brand_validations else f"{file_date[:4]}-{file_date[4:6]}-{file_date[6:8]}"
                    
                    # Insert log
                    proc_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    cols_error_str = ','.join(cols_error) if cols_error else ''
                    
                    if MOCK_SNOWFLAKE:
                        insert_log_local(proc_date, brand, extraction_date, table_name, rows_total, rows_ok, rows_ko, cols_error_str, status, details)
                    else:
                        insert_log_snowflake(proc_date, brand, extraction_date, table_name, rows_total, rows_ok, rows_ko, cols_error_str, status, details)
                
                # Add validations to global list
                all_validations.extend(table_validations)
                    
            except Exception as e:
                logger.error(f"Error merging data for {table_name} on date {file_date}: {e}")
                logger.error(traceback.format_exc())
                success = False
    
    # Clean up silver files if all processing was successful
    if success and specific_date:
        clean_silver_files(specific_date)
    
    # Return True if at least one table was processed successfully
    return success and len(all_validations) > 0

# Parse command line arguments
def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Process files from Silver layer to Gold layer")
    parser.add_argument("--date", type=str, help="Specific date to process (YYYYMMDD format)")
    return parser.parse_args()

# Lambda handler
def lambda_handler(event, context):
    """
    Handler function for AWS Lambda or AWS Glue
    """
    logger.info("Lambda/Glue invocation started")
    try:
        # Display configuration
        log_config()
        
        # Get date from event if provided
        date_to_process = event.get('date') if isinstance(event, dict) else None
        
        # Execute processing
        success = process_silver_to_gold(date_to_process)
        
        response = {
            'statusCode': 200 if success else 500, 
            'body': json.dumps({
                'success': success,
                'message': 'Processing completed successfully' if success else 'Processing failed'
            })
        }
    except Exception as e:
        logger.error(f"Lambda/Glue execution error: {e}")
        logger.error(traceback.format_exc())
        response = {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Processing failed'
            })
        }
    
    logger.info(f"Lambda/Glue finished with response: {response}")
    return response

# Main execution
if __name__ == '__main__':
    try:
        # Display paths for debugging
        print(f"BASE_DIR: {BASE_DIR}")
        print(f"DATA_DIR: {DATA_DIR}")
        print(f"SILVER_DIR: {SILVER_DIR}")
        print(f"GOLD_DIR: {GOLD_DIR}")
        print(f"LOGS_DIR: {LOGS_DIR}")
        
        logger.info("Starting SILVER to GOLD processing")
        
        # Display configuration
        log_config()
        
        # Parse command line arguments
        args = parse_args()
        specific_date = args.date
        
        # Record start time
        start_time = datetime.now()
        logger.info(f"Process started at: {start_time}")
        
        # Execute main processing
        success = process_silver_to_gold(specific_date)
        
        # Record end time and duration
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"Process completed at: {end_time}")
        logger.info(f"Total duration: {duration}")
        
        # Display summary on console
        print(f"\n=== EXECUTION SUMMARY ===")
        print(f"Started at   : {start_time}")
        print(f"Finished at  : {end_time}")
        print(f"Total duration: {duration}")
        print(f"Status      : {'SUCCESS' if success else 'FAILURE'}")
        print(f"Execution log: {log_file}")
        print(f"=======================\n")
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    except Exception as e:
        # Capture all unhandled exceptions
        logger.error(f"CRITICAL ERROR: {e}")
        logger.error(traceback.format_exc())
        print(f"\n[CRITICAL ERROR]")
        print(f"An error occurred during execution:")
        print(f"{str(e)}")
        print(f"Check the log for details: {log_file}\n")
        sys.exit(1)