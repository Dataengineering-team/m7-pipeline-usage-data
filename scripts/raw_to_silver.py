#!/usr/bin/env python3
"""
This script processes CSV files from the landing/raw layer and converts them to Parquet format in the silver layer.
It supports date-specific processing for handling files that arrive late or need reprocessing.
"""
import os
import io
import json
import re
import logging
import shutil
import traceback
import csv
import argparse
import sys
from datetime import datetime
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ===== CONFIGURATION =====
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
LANDING_DIR = DATA_DIR / os.getenv('LANDING_SUBDIR', 'landing')
RAW_DIR = DATA_DIR / os.getenv('RAW_SUBDIR', 'raw')
SILVER_DIR = DATA_DIR / os.getenv('SILVER_SUBDIR', 'silver')
LOGS_DIR = DATA_DIR / os.getenv('LOGS_SUBDIR', 'logs')

# Convert paths to absolute for logging
LANDING_DIR = LANDING_DIR.resolve()
RAW_DIR = RAW_DIR.resolve()
SILVER_DIR = SILVER_DIR.resolve()
LOGS_DIR = LOGS_DIR.resolve()

# Processing parameters
EXPECTED_TABLES = os.getenv('EXPECTED_TABLES', "Device,EPG,Playback,User,Smartcard,VODCatalog,VODCatalogExtended").split(',')
MOCK_SNOWFLAKE = os.getenv('MOCK_SNOWFLAKE', 'true').lower() == 'true'
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
DELETE_SOURCE_FILES_AFTER_LOAD = os.getenv('DELETE_SOURCE_FILES_AFTER_LOAD', 'false').lower() == 'true'

# Snowflake parameters (for future use)
SF_USER = os.getenv('SNOWFLAKE_USER')
SF_PWD = os.getenv('SNOWFLAKE_PASSWORD')
SF_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SF_WHS = os.getenv('SNOWFLAKE_WAREHOUSE')
SF_DB = os.getenv('SNOWFLAKE_DATABASE')
SF_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'MONITORING')

# Ensure these directories exist
LANDING_DIR.mkdir(exist_ok=True, parents=True)
RAW_DIR.mkdir(exist_ok=True, parents=True)
SILVER_DIR.mkdir(exist_ok=True, parents=True)
LOGS_DIR.mkdir(exist_ok=True, parents=True)

# Configure logging
log_file = LOGS_DIR / "raw_to_silver_execution.log"
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Custom JSON Encoder class to handle serialization
class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle non-serializable types"""
    def default(self, obj):
        # Convert mappingproxy objects (like dialect.__dict__) to standard dictionaries
        if str(type(obj)) == "<class 'mappingproxy'>":
            return dict(obj)
        # Convert other non-serializable objects to their string representation
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
        "LANDING_DIR": str(LANDING_DIR),
        "RAW_DIR": str(RAW_DIR),
        "SILVER_DIR": str(SILVER_DIR),
        "LOGS_DIR": str(LOGS_DIR),
        "EXPECTED_TABLES": EXPECTED_TABLES,
        "MOCK_SNOWFLAKE": MOCK_SNOWFLAKE,
        "LOG_LEVEL": LOG_LEVEL,
        "DELETE_SOURCE_FILES_AFTER_LOAD": DELETE_SOURCE_FILES_AFTER_LOAD
    }
    logger.info("Configuration:")
    for key, value in config.items():
        logger.info(f"  {key}: {value}")

# Utility: slugify brand name
def slugify(name: str) -> str:
    """
    Transform 'My Brand Name' -> 'my_brand_name'
    Keep only a-z, 0-9, underscore.
    """
    s = name.lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_]", "", s)
    return s

# Filename parser
def parse_filename(fn: str):
    """Parse the filename to extract brand, table and date"""
    if not fn.lower().endswith('.csv'):
        logger.debug(f"Skipping non-CSV file: {fn}")
        return None, None, None
    
    base = fn[:-4]  # Remove .csv extension
    
    # Expected format: Brand_Table_Date.csv or Brand - Resellers_Table_Date.csv
    parts = base.split('_')
    
    if len(parts) < 3:
        logger.warning(f"Filename does not follow expected pattern: {fn}")
        return None, None, None
    
    date = parts[-1]
    table = parts[-2]
    
    # Brand name may contain underscores, so reconstruct it
    brand = '_'.join(parts[:-2])
    
    return brand, table, date

# Local function to simulate log insertion into Snowflake
def insert_log_local(proc_date, brand, dt, details, status):
    """
    Local version of the insert_log function that writes to a JSON file
    """
    # Create a dictionary without complex objects to avoid serialization issues
    log_entry = {
        "PROCESS_DATE": proc_date,
        "BRAND": brand,
        "EXTRACTION_DATE": dt,
        "FILE_COUNT": details.get('files_present', 0),
        "STATUS": status,
        "DETAILS": {}
    }
    
    # Copy only simple details and convert others to strings
    for key, value in details.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            log_entry["DETAILS"][key] = value
        elif isinstance(value, dict):
            simplified_dict = {}
            for k, v in value.items():
                if isinstance(v, (str, int, float, bool)) or v is None:
                    simplified_dict[k] = v
                else:
                    simplified_dict[k] = str(v)
            log_entry["DETAILS"][key] = simplified_dict
        else:
            log_entry["DETAILS"][key] = str(value)
    
    filename = LOGS_DIR / f"raw_logs_{brand}_{dt}.json"
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            # Use the custom encoder to handle non-serializable types
            json.dump(log_entry, f, indent=2, cls=CustomJSONEncoder)
        
        logger.info(f"Local log created: {filename}")
    except Exception as e:
        logger.error(f"Error writing local log: {e}")
        logger.error(traceback.format_exc())

# Log insertion into Snowflake (real function)
def insert_log_snowflake(proc_date, brand, dt, details, status):
    """
    Inserts a log record into Snowflake
    """
    try:
        import snowflake.connector
        
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
        jd = details_json.replace("'", "''")
        cnt = details.get('files_present', 0)
        sql = f"""
            INSERT INTO {SF_SCHEMA}.RAW_LOGS
                (PROCESS_DATE, BRAND, EXTRACTION_DATE, FILE_COUNT, STATUS, DETAILS)
            VALUES (
                TO_TIMESTAMP_NTZ('{proc_date}'),
                '{brand}',
                '{dt}',
                {cnt},
                '{status}',
                '{jd}'
            );
        """
        cs.execute(sql)
        ctx.commit()
        logger.info(f"Inserted log for {brand} {dt} status={status}")
    except Exception as e:
        logger.error(f"Snowflake log error: {e}")
        logger.error(traceback.format_exc())
    finally:
        try:
            cs.close()
            ctx.close()
        except Exception:
            pass

# CSV format validation
def validate_csv(file_path):
    """
    Checks if the CSV file is well-formed and returns information about its content
    """
    try:
        with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
            # Analyze the first 5 lines to detect format
            lines = []
            for _ in range(5):
                try:
                    lines.append(next(csvfile))
                except StopIteration:
                    break
                    
            sample = ''.join(lines)
            csvfile.seek(0)  # Return to the beginning of the file
            
            # Delimiter detection
            dialect = csv.Sniffer().sniff(sample)
            delimiter = dialect.delimiter
            
            # Read with pandas for validation and information extraction
            df = pd.read_csv(csvfile, sep=delimiter, nrows=5)
            
            # Extract relevant information from dialect as standard dictionary
            dialect_info = {
                "delimiter": dialect.delimiter,
                "doublequote": dialect.doublequote,
                "escapechar": str(dialect.escapechar),
                "lineterminator": repr(dialect.lineterminator),
                "quotechar": str(dialect.quotechar),
                "quoting": dialect.quoting,
                "skipinitialspace": dialect.skipinitialspace
            }
            
            return {
                'valid': True,
                'delimiter': delimiter,
                'columns': list(df.columns),
                'row_count_sample': len(df),
                'format_details': {
                    'has_header': csv.Sniffer().has_header(sample),
                    'dialect': dialect_info
                }
            }
    except Exception as e:
        logger.warning(f"CSV validation failed for {file_path}: {str(e)}")
        return {
            'valid': False,
            'error': str(e)
        }

# Main processing function
def process_and_convert(specific_date=None):
    """
    Process CSV files from landing to organize them in raw and convert to Parquet in silver
    
    Args:
        specific_date: Optional date string in YYYYMMDD format to filter processing
    
    Returns:
        Number of files moved
    """
    logger.info("Starting process_and_convert")
    if specific_date:
        logger.info(f"Processing restricted to date: {specific_date}")
    
    # Check that the landing directory exists and list its contents for debugging
    logger.info(f"LANDING_DIR exists: {LANDING_DIR.exists()}")
    try:
        landing_files = list(LANDING_DIR.glob('*'))
        logger.info(f"LANDING_DIR contents: {[f.name for f in landing_files]}")
    except Exception as e:
        logger.error(f"Error reading LANDING_DIR contents: {e}")
    
    # List all files in the landing directory
    contents = []
    for file_path in LANDING_DIR.glob('*.csv'):
        brand, tbl, dt = parse_filename(file_path.name)
        # If a specific date is provided, filter files
        if specific_date and dt != specific_date:
            continue
        contents.append({
            'Key': file_path.name,
            'Path': file_path,
            'Brand': brand,
            'Table': tbl,
            'Date': dt
        })
    
    if not contents:
        logger.warning(f"No CSV files found in {LANDING_DIR}" + 
                      (f" for date {specific_date}" if specific_date else ""))
        return 0
    
    # Group files by (brand, date)
    groups = {}
    for obj in contents:
        key = obj['Key']
        brand = obj['Brand']
        tbl = obj['Table']
        dt = obj['Date']
        
        if not brand or not tbl or not dt:
            continue
            
        if (brand, dt) not in groups:
            groups[(brand, dt)] = {}
        groups[(brand, dt)][tbl] = obj
    
    if not groups:
        logger.warning("No valid CSV files to process")
        return 0
    
    total_moved = 0
    # Process each group
    for (brand, dt), files in groups.items():
        logger.info(f"Processing group: brand={brand}, date={dt}")
        brand_slug = slugify(brand)
        detail = {'files_present': len(files), 'details': {}}
        
        # Flag presence for expected tables
        for tbl in EXPECTED_TABLES:
            detail['details'][tbl] = {'status': 'OK' if tbl in files else 'NoK'}
        
        # Always process found files
        for tbl, obj in files.items():
            fn = obj['Key']
            try:
                # Create destination directories
                raw_dest_dir = RAW_DIR / dt / brand_slug
                raw_dest_dir.mkdir(exist_ok=True, parents=True)
                
                silver_dest_dir = SILVER_DIR / dt / brand_slug
                silver_dest_dir.mkdir(exist_ok=True, parents=True)
                
                # Source and destination file paths
                src_file = obj['Path']
                dest_csv = raw_dest_dir / fn
                
                # Validate CSV before processing
                csv_validation = validate_csv(src_file)
                if not csv_validation['valid']:
                    raise ValueError(f"Invalid CSV format: {csv_validation.get('error', 'Unknown error')}")
                
                # Store validation information in details
                # Store only simplified information to avoid serialization issues
                detail['details'][tbl]['csv_validation'] = {
                    'valid': csv_validation['valid'],
                    'delimiter': csv_validation['delimiter'],
                    'columns_count': len(csv_validation.get('columns', [])),
                    'row_count_sample': csv_validation.get('row_count_sample', 0)
                }
                
                # Copy to raw
                logger.info(f"Copying {src_file} to {dest_csv}")
                shutil.copy2(src_file, dest_csv)
                
                # Delete original (if configured)
                if DELETE_SOURCE_FILES_AFTER_LOAD and dest_csv.exists():
                    os.remove(src_file)
                    logger.info(f"Removed original file: {src_file}")
                elif not DELETE_SOURCE_FILES_AFTER_LOAD:
                    logger.info(f"Keeping original file (DELETE_SOURCE_FILES_AFTER_LOAD=false): {src_file}")
                
                detail['details'][tbl]['moved_to'] = str(dest_csv)
                total_moved += 1
                
                # Convert to Parquet
                logger.info(f"Converting {dest_csv} to Parquet format")
                
                # Use the detected delimiter
                delimiter = csv_validation.get('delimiter', ',')
                df = pd.read_csv(dest_csv, sep=delimiter)
                
                # Parquet filename is the same but with .parquet extension
                parquet_filename = f"{fn[:-4]}.parquet"
                dest_parquet = silver_dest_dir / parquet_filename
                
                # Write the Parquet
                df.to_parquet(dest_parquet, index=False)
                detail['details'][tbl]['parquet_key'] = str(dest_parquet)
                logger.info(f"Parquet written to {dest_parquet}")
                
            except Exception as e:
                logger.error(f"Error processing {fn}: {e}")
                logger.error(traceback.format_exc())
                detail['details'][tbl]['error'] = str(e)
                detail['details'][tbl]['traceback'] = str(traceback.format_exc())
        
        # Determine status: OK if all expected present, otherwise Partial
        status = 'OK' if all(detail['details'].get(tbl, {}).get('status') == 'OK' for tbl in EXPECTED_TABLES) else 'Partial'
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Log insertion (local or Snowflake)
        if MOCK_SNOWFLAKE:
            insert_log_local(now, brand, dt, detail, status)
        else:
            insert_log_snowflake(now, brand, dt, detail, status)
    
    logger.info(f"Finished processing. Total moved: {total_moved}")
    return total_moved

# Function to verify generated Parquet files
def verify_parquet_generation(specific_date=None):
    """
    Verifies that Parquet files have been correctly generated
    
    Args:
        specific_date: Optional date string in YYYYMMDD format to filter verification
        
    Returns:
        Boolean indicating success
    """
    try:
        # Define the glob pattern based on whether a specific date is provided
        if specific_date:
            parquet_files = list(SILVER_DIR.glob(f'{specific_date}/**/*.parquet'))
        else:
            parquet_files = list(SILVER_DIR.glob('**/*.parquet'))
        
        if not parquet_files:
            logger.warning(f"No Parquet files found in {SILVER_DIR}" + 
                          (f" for date {specific_date}" if specific_date else ""))
            return False
        
        # Display found files
        logger.info(f"Parquet files found ({len(parquet_files)}):")
        for pq_file in parquet_files:
            logger.info(f" - {pq_file}")
        
        # Check content of a file
        if parquet_files:
            first_file = parquet_files[0]
            df = pd.read_parquet(first_file)
            logger.info(f"Content of first file ({first_file.name}): {len(df)} rows, columns: {list(df.columns)}")
        
        return len(parquet_files) > 0
    except Exception as e:
        logger.error(f"Error verifying Parquet files: {e}")
        logger.error(traceback.format_exc())
        return False

# Parse command line arguments
def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Process CSV files from landing to raw/silver layers")
    parser.add_argument("--date", type=str, help="Specific date to process (YYYYMMDD format)")
    return parser.parse_args()

# Lambda handler
def lambda_handler(event, context):
    """
    Handler function for AWS Lambda
    """
    logger.info("Lambda invocation started")
    try:
        # Display configuration
        log_config()
        
        # Get date from event if provided
        date_to_process = event.get('date') if isinstance(event, dict) else None
        
        # Execute processing
        moved = process_and_convert(date_to_process)
        response = {
            'statusCode': 200, 
            'body': json.dumps({
                'files_moved': moved,
                'message': 'Processing completed successfully'
            })
        }
    except Exception as e:
        logger.error(f"Lambda execution error: {e}")
        logger.error(traceback.format_exc())
        response = {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Processing failed'
            })
        }
    
    logger.info(f"Lambda finished with response: {response}")
    return response

# Main execution
if __name__ == '__main__':
    try:
        # Display paths for debugging
        print(f"BASE_DIR: {BASE_DIR}")
        print(f"DATA_DIR: {DATA_DIR}")
        print(f"LANDING_DIR: {LANDING_DIR}")
        print(f"RAW_DIR: {RAW_DIR}")
        print(f"SILVER_DIR: {SILVER_DIR}")
        print(f"LOGS_DIR: {LOGS_DIR}")
        
        logger.info("Starting RAW to SILVER processing")
        
        # Display configuration
        log_config()
        
        # Parse command line arguments
        args = parse_args()
        specific_date = args.date
        
        # Record start time
        start_time = datetime.now()
        logger.info(f"Process started at: {start_time}")
        
        # Execute main processing
        moved_count = process_and_convert(specific_date)
        logger.info(f"Moved {moved_count} files.")
        
        # Verify results
        if verify_parquet_generation(specific_date):
            logger.info("[SUCCESS] Parquet conversion successful in silver")
        else:
            logger.warning("[FAILURE] Problem with Parquet file generation")
        
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
        print(f"Files processed: {moved_count}")
        print(f"Execution log: {log_file}")
        print(f"=======================\n")
        
        # Exit with appropriate code
        sys.exit(0 if moved_count > 0 or specific_date else 1)
        
    except Exception as e:
        # Capture all unhandled exceptions
        logger.error(f"CRITICAL ERROR: {e}")
        logger.error(traceback.format_exc())
        print(f"\n[CRITICAL ERROR]")
        print(f"An error occurred during execution:")
        print(f"{str(e)}")
        print(f"Check the log for details: {log_file}\n")
        sys.exit(1)