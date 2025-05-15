#!/usr/bin/env python3
"""
silver_to_gold.py - Transform silver data (Parquet) to gold layer (aggregated Parquet) for ETL pipeline
AWS Lambda version with S3 integration and Snowflake logging
"""
import os
import json
import logging
import traceback
import argparse
import sys
import uuid
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
import boto3
from botocore.exceptions import ClientError

# ===== CONFIGURATION =====
# Base paths - adaptation pour Lambda
if 'AWS_LAMBDA_FUNCTION_NAME' in os.environ:
    # En environnement Lambda, utiliser /tmp pour les données temporaires
    DATA_DIR = Path(os.getenv('DATA_DIR', '/tmp/data'))
    CONFIG_DIR = Path(os.getenv('CONFIG_DIR', '/tmp/config'))
    # Créer les répertoires nécessaires
    DATA_DIR.mkdir(exist_ok=True, parents=True)
    CONFIG_DIR.mkdir(exist_ok=True, parents=True)
else:
    # Configuration locale simplifiée
    BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__))).parent
    DATA_DIR = Path(os.getenv('DATA_DIR', BASE_DIR / "data"))
    CONFIG_DIR = Path(os.getenv('CONFIG_DIR', BASE_DIR / "config"))

# S3 bucket pour les données
S3_DATA_BUCKET = os.getenv('DATA_BUCKET')

# Define explicit paths relative to DATA_DIR
SILVER_DIR = DATA_DIR / os.getenv('SILVER_SUBDIR', 'silver')
GOLD_DIR = DATA_DIR / os.getenv('GOLD_SUBDIR', 'gold')
LOGS_DIR = DATA_DIR / os.getenv('LOGS_SUBDIR', 'logs')

# Processing parameters
EXPECTED_TABLES = os.getenv('EXPECTED_TABLES', "Device,EPG,Playback,User,Smartcard,VODCatalog,VODCatalogExtended").split(',')
MOCK_SNOWFLAKE = os.getenv('MOCK_SNOWFLAKE', 'true').lower() == 'true'
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
DELETE_SILVER_FILES_AFTER_LOAD = os.getenv('DELETE_SILVER_FILES_AFTER_LOAD', 'false').lower() == 'true'
PARALLEL_PROCESSING = os.getenv('PARALLEL_PROCESSING', 'true').lower() == 'true'
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '6'))
MAX_DAYS_BACK = int(os.getenv('MAX_DAYS_BACK', '3'))

# Snowflake parameters
SF_USER = os.getenv('SNOWFLAKE_USER')
SF_PWD = os.getenv('SNOWFLAKE_PASSWORD')
SF_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SF_WHS = os.getenv('SNOWFLAKE_WAREHOUSE')
SF_DB = os.getenv('SNOWFLAKE_DATABASE')
SF_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'MONITORING')
SF_MONITORING_SCHEMA = os.getenv('SNOWFLAKE_MONITORING_SCHEMA', 'STG_SG_MONITORING')

# Ensure these directories exist
for directory in [SILVER_DIR, GOLD_DIR, LOGS_DIR, CONFIG_DIR]:
    directory.mkdir(exist_ok=True, parents=True)

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

# Custom JSON Encoder class to handle serialization
class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle non-serializable types"""
    def default(self, obj):
        if str(type(obj)) == "<class 'mappingproxy'>":
            return dict(obj)
        try:
            return json.JSONEncoder.default(self, obj)
        except TypeError:
            return str(obj)

# Display configuration at startup
def log_config():
    """Log the current configuration"""
    config = {
        "DATA_DIR": str(DATA_DIR),
        "CONFIG_DIR": str(CONFIG_DIR),
        "SILVER_DIR": str(SILVER_DIR),
        "GOLD_DIR": str(GOLD_DIR),
        "LOGS_DIR": str(LOGS_DIR),
        "S3_DATA_BUCKET": S3_DATA_BUCKET,
        "EXPECTED_TABLES": EXPECTED_TABLES,
        "MOCK_SNOWFLAKE": MOCK_SNOWFLAKE,
        "LOG_LEVEL": LOG_LEVEL,
        "DELETE_SILVER_FILES_AFTER_LOAD": DELETE_SILVER_FILES_AFTER_LOAD,
        "PARALLEL_PROCESSING": PARALLEL_PROCESSING,
        "MAX_WORKERS": MAX_WORKERS,
        "MAX_DAYS_BACK": MAX_DAYS_BACK
    }
    logger.info("Configuration:")
    for key, value in config.items():
        logger.info(f"  {key}: {value}")

# S3 Functions
def download_from_s3(bucket, key, local_path):
    """Download a file from S3 to local path"""
    try:
        s3_client = boto3.client('s3')
        logger.info(f"Downloading s3://{bucket}/{key} to {local_path}")
        s3_client.download_file(bucket, key, local_path)
        return True
    except ClientError as e:
        logger.error(f"Error downloading from S3: {e}")
        return False

def upload_to_s3(local_path, bucket, key):
    """Upload a local file to S3"""
    try:
        s3_client = boto3.client('s3')
        logger.info(f"Uploading {local_path} to s3://{bucket}/{key}")
        s3_client.upload_file(local_path, bucket, key)
        return True
    except ClientError as e:
        logger.error(f"Error uploading to S3: {e}")
        return False

def list_s3_files(bucket, prefix):
    """List files in S3 bucket with given prefix"""
    try:
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        result = []
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    result.append({
                        'Key': obj['Key'],
                        'Size': obj['Size'],
                        'LastModified': obj['LastModified']
                    })
        return result
    except ClientError as e:
        logger.error(f"Error listing S3 files: {e}")
        return []

def delete_s3_object(bucket, key):
    """Delete an S3 object"""
    try:
        s3_client = boto3.client('s3')
        logger.info(f"Deleting s3://{bucket}/{key}")
        s3_client.delete_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        logger.error(f"Error deleting S3 object: {e}")
        return False

# Snowflake integration
def get_snowflake_credentials():
    """Get Snowflake credentials from environment or Secrets Manager"""
    try:
        # If environment variables are defined, use them
        if all([SF_USER, SF_PWD, SF_ACCOUNT, SF_WHS, SF_DB]):
            return {
                'user': SF_USER,
                'password': SF_PWD,
                'account': SF_ACCOUNT,
                'warehouse': SF_WHS,
                'database': SF_DB
            }
            
        # Otherwise, try to get from Secrets Manager
        secret_name = os.getenv('SNOWFLAKE_SECRET_NAME')
        if not secret_name:
            raise ValueError("SNOWFLAKE_SECRET_NAME environment variable is not set")
            
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager')
        
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response['SecretString']
        secret_dict = json.loads(secret)
        
        return {
            'user': secret_dict.get('user'),
            'password': secret_dict.get('password'),
            'account': secret_dict.get('account'),
            'warehouse': secret_dict.get('warehouse'),
            'database': secret_dict.get('database')
        }
    except Exception as e:
        logger.error(f"Failed to get Snowflake credentials: {e}")
        raise

def insert_log_local(proc_date, table_name, details, status):
    """Local version of the insert_log function that writes to a JSON file"""
    log_entry = {
        "PROCESS_ID": str(uuid.uuid4()),
        "PROCESS_DATE": proc_date,
        "TABLE_NAME": table_name,
        "FILE_COUNT": details.get('file_count', 0),
        "ROW_COUNT": details.get('row_count', 0),
        "STATUS": status,
        "DETAILS": {}
    }
    
    # Simplify details to avoid serialization issues
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
    
    filename = LOGS_DIR / f"gold_logs_{table_name}.json"
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(log_entry, f, indent=2, cls=CustomJSONEncoder)
        
        logger.info(f"Local log created: {filename}")
        
        # Upload log to S3 if in Lambda environment
        if 'AWS_LAMBDA_FUNCTION_NAME' in os.environ and S3_DATA_BUCKET:
            s3_log_key = f"logs/gold_logs_{table_name}.json"
            upload_to_s3(filename, S3_DATA_BUCKET, s3_log_key)
    except Exception as e:
        logger.error(f"Error writing local log: {e}")
        logger.error(traceback.format_exc())

def insert_log_snowflake(proc_date, table_name, details, status):
    """Inserts a log record into Snowflake"""
    try:
        import snowflake.connector
        
        # Get Snowflake credentials
        credentials = get_snowflake_credentials()
        
        # Connect to Snowflake
        ctx = snowflake.connector.connect(
            user=credentials['user'],
            password=credentials['password'],
            account=credentials['account'],
            warehouse=credentials['warehouse'],
            database=credentials['database'],
            schema=SF_MONITORING_SCHEMA
        )
        cs = ctx.cursor()
        
        # Get counts
        file_count = details.get('file_count', 0)
        row_count = details.get('row_count', 0)
        
        # Simplify details to fit in VARCHAR(4000)
        simplified_details = {
            "file_count": file_count,
            "row_count": row_count,
            "process_info": {}
        }
        
        # Add simplified processing information
        for key, value in details.items():
            if key not in ('file_count', 'row_count'):
                if isinstance(value, (str, int, float, bool, type(None))):
                    simplified_details["process_info"][key] = value
                else:
                    simplified_details["process_info"][key] = str(value)
                
        # Convert to JSON and truncate if necessary
        json_details = json.dumps(simplified_details, cls=CustomJSONEncoder)
        if len(json_details) > 4000:
            logger.warning(f"Details JSON too large ({len(json_details)} chars), truncating to 4000 chars")
            json_details = json_details[:3997] + "..."
        
        # SQL query
        sql = f"""
            INSERT INTO {SF_MONITORING_SCHEMA}.GOLD_LOGS
                (PROCESS_DATE, TABLE_NAME, FILE_COUNT, ROW_COUNT, STATUS, DETAILS)
            VALUES (
                TO_TIMESTAMP_NTZ(%s), %s, %s, %s, %s, %s
            );
        """
        
        # Parameters
        params = (
            proc_date,      # PROCESS_DATE
            table_name,     # TABLE_NAME
            file_count,     # FILE_COUNT
            row_count,      # ROW_COUNT
            status,         # STATUS
            json_details    # DETAILS (standard JSON format)
        )
        
        cs.execute(sql, params)
        ctx.commit()
        logger.info(f"Inserted log for {table_name} status={status}")
        
        return True
    except Exception as e:
        logger.error(f"Snowflake log error: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        try:
            if 'cs' in locals():
                cs.close()
            if 'ctx' in locals():
                ctx.close()
        except Exception:
            pass

# Get available dates to process
def get_available_dates(specific_date=None):
    """
    Get list of available dates in silver layer
    
    Args:
        specific_date: Optional date string in YYYYMMDD format to restrict to a single date
        
    Returns:
        List of dates as strings in YYYYMMDD format
    """
    available_dates = []
    
    # Handle Lambda environment with S3
    if 'AWS_LAMBDA_FUNCTION_NAME' in os.environ and S3_DATA_BUCKET:
        # List all silver objects
        s3_objects = list_s3_files(S3_DATA_BUCKET, 'silver/')
        
        # Extract dates from object keys
        # Expected format: silver/YYYYMMDD/...
        for obj in s3_objects:
            key = obj['Key']
            parts = key.split('/')
            if len(parts) > 2 and parts[0] == 'silver':
                date_str = parts[1]
                if len(date_str) == 8 and date_str.isdigit():
                    if specific_date and date_str != specific_date:
                        continue
                    if date_str not in available_dates:
                        available_dates.append(date_str)
    else:
        # Local filesystem
        for item in SILVER_DIR.iterdir():
            if item.is_dir():
                date_str = item.name
                if len(date_str) == 8 and date_str.isdigit():
                    if specific_date and date_str != specific_date:
                        continue
                    available_dates.append(date_str)
    
    # Sort dates chronologically
    available_dates.sort()
    
    # Limit by MAX_DAYS_BACK if no specific date provided
    if not specific_date and MAX_DAYS_BACK > 0 and len(available_dates) > MAX_DAYS_BACK:
        available_dates = available_dates[-MAX_DAYS_BACK:]
    
    return available_dates

# Get tables available for a specific date
def get_tables_for_date(date_str):
    """
    Get list of tables available for a specific date in silver layer
    
    Args:
        date_str: Date string in YYYYMMDD format
        
    Returns:
        Dictionary mapping table names to lists of file information
    """
    tables = {}
    
    # Handle Lambda environment with S3
    if 'AWS_LAMBDA_FUNCTION_NAME' in os.environ and S3_DATA_BUCKET:
        # List all silver objects for this date
        prefix = f"silver/{date_str}/"
        s3_objects = list_s3_files(S3_DATA_BUCKET, prefix)
        
        # Group by table name
        for obj in s3_objects:
            key = obj['Key']
            if not key.lower().endswith('.parquet'):
                continue
                
            # Extract table name from filename
            filename = key.split('/')[-1]
            name_parts = filename.split('_')
            if len(name_parts) < 2:
                continue
                
            # Assume filename format: Brand_TableName_YYYYMMDD.parquet
            # The table name can be inferred from the second-to-last part before the date
            table_name = name_parts[-2]
            
            # Skip if not in expected tables
            if table_name not in EXPECTED_TABLES:
                continue
                
            if table_name not in tables:
                tables[table_name] = []
                
            tables[table_name].append({
                'Key': key,
                'Size': obj['Size'],
                'LastModified': obj['LastModified']
            })
    else:
        # Local filesystem
        date_dir = SILVER_DIR / date_str
        if not date_dir.exists():
            return tables
            
        # Recursively find all Parquet files
        for parquet_file in date_dir.glob('**/*.parquet'):
            filename = parquet_file.name
            name_parts = filename.split('_')
            if len(name_parts) < 2:
                continue
                
            table_name = name_parts[-2]
            
            # Skip if not in expected tables
            if table_name not in EXPECTED_TABLES:
                continue
                
            if table_name not in tables:
                tables[table_name] = []
                
            tables[table_name].append({
                'Path': parquet_file,
                'Size': parquet_file.stat().st_size,
                'LastModified': datetime.fromtimestamp(parquet_file.stat().st_mtime)
            })
    
    return tables

# Read configuration from S3 or local file
def get_table_config(table_name):
    """
    Get configuration for a specific table
    
    Args:
        table_name: Name of the table
        
    Returns:
        Dictionary with table configuration or None if not found
    """
    config_filename = f"{table_name.lower()}_config.json"
    local_config_path = CONFIG_DIR / config_filename
    
    # Try to load from S3 first if we're in Lambda environment
    if 'AWS_LAMBDA_FUNCTION_NAME' in os.environ and S3_DATA_BUCKET:
        s3_config_key = f"config/{config_filename}"
        
        try:
            # Download config file from S3
            if download_from_s3(S3_DATA_BUCKET, s3_config_key, local_config_path):
                with open(local_config_path, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load config from S3: {e}")
    
    # Fallback to local file
    try:
        if local_config_path.exists():
            with open(local_config_path, 'r') as f:
                return json.load(f)
    except Exception as e:
        logger.warning(f"Could not load local config: {e}")
    
    # Provide default configuration if no config found
    logger.info(f"No configuration found for {table_name}, using defaults")
    return {
        "table_name": table_name,
        "key_columns": [],
        "aggregate_columns": {},
        "filter_conditions": "",
        "partition_columns": []
    }

# Process a single table for a date
def process_table(table_name, date_str, files):
    """
    Process a table for a specific date
    
    Args:
        table_name: Name of the table
        date_str: Date string in YYYYMMDD format
        files: List of file information
        
    Returns:
        Dictionary with processing details
    """
    logger.info(f"Processing table {table_name} for date {date_str} ({len(files)} files)")
    start_time = datetime.now()
    
    results = {
        'table_name': table_name,
        'date': date_str,
        'file_count': len(files),
        'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
        'status': 'Started'
    }
    
    try:
        # Get table config
        config = get_table_config(table_name)
        
        # Load all Parquet files into a single DataFrame
        all_dfs = []
        
        for file_info in files:
            try:
                if 'Key' in file_info:  # S3 file
                    # Download file first
                    filename = file_info['Key'].split('/')[-1]
                    local_path = SILVER_DIR / date_str / filename
                    local_path.parent.mkdir(exist_ok=True, parents=True)
                    
                    if download_from_s3(S3_DATA_BUCKET, file_info['Key'], local_path):
                        df = pd.read_parquet(local_path)
                        all_dfs.append(df)
                    else:
                        logger.error(f"Failed to download {file_info['Key']}")
                else:  # Local file
                    df = pd.read_parquet(file_info['Path'])
                    all_dfs.append(df)
            except Exception as e:
                logger.error(f"Error reading Parquet file: {e}")
                logger.error(traceback.format_exc())
        
        if not all_dfs:
            results['status'] = 'Failed'
            results['error'] = 'No data could be loaded'
            return results
        
        # Combine all DataFrames
        combined_df = pd.concat(all_dfs, ignore_index=True)
        results['row_count_raw'] = len(combined_df)
        
        # Apply filter if provided
        if 'filter_conditions' in config and config['filter_conditions']:
            try:
                filtered_df = combined_df.query(config['filter_conditions'])
                logger.info(f"Applied filter: {config['filter_conditions']}")
                logger.info(f"Rows before filter: {len(combined_df)}, after filter: {len(filtered_df)}")
                results['rows_filtered'] = len(combined_df) - len(filtered_df)
                combined_df = filtered_df
            except Exception as e:
                logger.warning(f"Error applying filter: {e}")
                results['filter_error'] = str(e)
        
        # Apply aggregations if configured
        if 'key_columns' in config and config['key_columns'] and 'aggregate_columns' in config and config['aggregate_columns']:
            try:
                # Group by key columns
                grouped = combined_df.groupby(config['key_columns'])
                
                # Apply aggregations
                agg_df = grouped.agg(config['aggregate_columns'])
                
                # Reset index to convert back to regular columns
                combined_df = agg_df.reset_index()
                
                logger.info(f"Applied aggregation on {', '.join(config['key_columns'])}")
                logger.info(f"Rows after aggregation: {len(combined_df)}")
                results['aggregation_applied'] = True
                results['aggregation_details'] = {
                    'key_columns': config['key_columns'],
                    'aggregate_columns': config['aggregate_columns']
                }
            except Exception as e:
                logger.warning(f"Error applying aggregation: {e}")
                results['aggregation_error'] = str(e)
        
        # Create the gold file
        gold_filename = f"{table_name}_{date_str}.parquet"
        gold_dir = GOLD_DIR / table_name
        gold_dir.mkdir(exist_ok=True, parents=True)
        gold_file_path = gold_dir / gold_filename
        
        # Add date column for traceability
        combined_df['processing_date'] = datetime.now().strftime('%Y-%m-%d')
        combined_df['extraction_date'] = date_str
        
        # Write to Parquet
        partition_cols = config.get('partition_columns', [])
        if partition_cols:
            combined_df.to_parquet(gold_file_path, partition_cols=partition_cols, index=False)
            logger.info(f"Wrote partitioned Parquet file with columns: {', '.join(partition_cols)}")
        else:
            combined_df.to_parquet(gold_file_path, index=False)
        
        logger.info(f"Created gold file: {gold_file_path}")
        results['gold_file'] = str(gold_file_path)
        results['row_count'] = len(combined_df)
        
        # Upload to S3 if in Lambda environment
        if 'AWS_LAMBDA_FUNCTION_NAME' in os.environ and S3_DATA_BUCKET:
            s3_gold_key = f"gold/{table_name}/{gold_filename}"
            if upload_to_s3(gold_file_path, S3_DATA_BUCKET, s3_gold_key):
                results['s3_location'] = f"s3://{S3_DATA_BUCKET}/{s3_gold_key}"
        
        # Delete silver files if configured
        if DELETE_SILVER_FILES_AFTER_LOAD:
            for file_info in files:
                if 'Key' in file_info:  # S3 file
                    delete_s3_object(S3_DATA_BUCKET, file_info['Key'])
                elif 'Path' in file_info:  # Local file
                    os.remove(file_info['Path'])
            logger.info(f"Deleted {len(files)} silver files")
            results['silver_files_deleted'] = len(files)
        
        end_time = datetime.now()
        duration = end_time - start_time
        results['end_time'] = end_time.strftime('%Y-%m-%d %H:%M:%S')
        results['duration_seconds'] = duration.total_seconds()
        results['status'] = 'Completed'
        
        return results
    except Exception as e:
        logger.error(f"Error processing table {table_name}: {e}")
        logger.error(traceback.format_exc())
        
        end_time = datetime.now()
        duration = end_time - start_time
        results['end_time'] = end_time.strftime('%Y-%m-%d %H:%M:%S')
        results['duration_seconds'] = duration.total_seconds()
        results['status'] = 'Failed'
        results['error'] = str(e)
        results['traceback'] = traceback.format_exc()
        
        return results

# Process all tables for a date
def process_date(date_str):
    """
    Process all tables for a specific date
    
    Args:
        date_str: Date string in YYYYMMDD format
        
    Returns:
        Dictionary with processing details for each table
    """
    logger.info(f"Processing data for date: {date_str}")
    
    # Get available tables
    tables = get_tables_for_date(date_str)
    
    if not tables:
        logger.warning(f"No tables found for date {date_str}")
        return {'date': date_str, 'tables_processed': 0, 'tables': {}}
    
    logger.info(f"Found {len(tables)} tables for date {date_str}: {', '.join(tables.keys())}")
    
    results = {'date': date_str, 'tables_processed': 0, 'tables': {}}
    
    # Process each table
    if PARALLEL_PROCESSING and len(tables) > 1:
        logger.info(f"Using parallel processing with {MAX_WORKERS} workers")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_table = {
                executor.submit(process_table, table_name, date_str, files): table_name
                for table_name, files in tables.items()
            }
            
            for future in concurrent.futures.as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    table_result = future.result()
                    results['tables'][table_name] = table_result
                    results['tables_processed'] += 1
                    
                    # Log to Snowflake/local if processing completed
                    if table_result['status'] in ('Completed', 'Failed'):
                        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        if MOCK_SNOWFLAKE:
                            insert_log_local(now, table_name, table_result, table_result['status'])
                        else:
                            insert_log_snowflake(now, table_name, table_result, table_result['status'])
                except Exception as e:
                    logger.error(f"Exception processing table {table_name}: {e}")
                    logger.error(traceback.format_exc())
                    results['tables'][table_name] = {
                        'status': 'Failed',
                        'error': str(e),
                        'traceback': traceback.format_exc()
                    }
    else:
        # Sequential processing
        logger.info("Using sequential processing")
        for table_name, files in tables.items():
            try:
                table_result = process_table(table_name, date_str, files)
                results['tables'][table_name] = table_result
                results['tables_processed'] += 1
                
                # Log to Snowflake/local
                if table_result['status'] in ('Completed', 'Failed'):
                    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    if MOCK_SNOWFLAKE:
                        insert_log_local(now, table_name, table_result, table_result['status'])
                    else:
                        insert_log_snowflake(now, table_name, table_result, table_result['status'])
            except Exception as e:
                logger.error(f"Exception processing table {table_name}: {e}")
                logger.error(traceback.format_exc())
                results['tables'][table_name] = {
                    'status': 'Failed',
                    'error': str(e),
                    'traceback': traceback.format_exc()
                }
    
    return results

# Main processing function
def process_silver_to_gold(specific_date=None):
    """
    Main function to process all available dates or a specific date
    
    Args:
        specific_date: Optional date string in YYYYMMDD format
        
    Returns:
        Dictionary with processing results
    """
    logger.info("Starting silver to gold processing")
    
    # Get available dates
    dates = get_available_dates(specific_date)
    
    if not dates:
        logger.warning("No dates available for processing")
        return {'dates_processed': 0, 'tables_processed': 0}
    
    logger.info(f"Found {len(dates)} dates for processing: {', '.join(dates)}")
    
    results = {'dates_processed': 0, 'tables_processed': 0, 'dates': {}}
    
    # Process each date
    for date_str in dates:
        date_result = process_date(date_str)
        results['dates'][date_str] = date_result
        results['dates_processed'] += 1
        results['tables_processed'] += date_result['tables_processed']
    
    return results

# Lambda handler function
def lambda_handler(event, context):
    """
    AWS Lambda handler
    
    Args:
        event: Lambda event object
        context: Lambda context object
        
    Returns:
        Dictionary with processing results
    """
    logger.info(f"Lambda invocation started with event: {json.dumps(event)}")
    
    try:
        # Log configuration
        log_config()
        
        # Extract date from event
        specific_date = None
        if isinstance(event, dict):
            specific_date = event.get('date')
        
        logger.info(f"Processing date: {specific_date if specific_date else 'all available'}")
        
        # Execute main processing
        results = process_silver_to_gold(specific_date)
        
        response = {
            'statusCode': 200,
            'body': json.dumps({
                'dates_processed': results['dates_processed'],
                'tables_processed': results['tables_processed'],
                'message': 'Processing completed successfully' if results['tables_processed'] > 0 else 'No tables processed'
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
    
    logger.info(f"Lambda finished with response status: {response['statusCode']}")
    return response

# Parse command line arguments
def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Process Parquet files from silver to gold layer")
    parser.add_argument("--date", type=str, help="Specific date to process (YYYYMMDD format)")
    parser.add_argument("--mock", action="store_true", help="Run in mock mode without connecting to Snowflake")
    parser.add_argument("--s3", action="store_true", help="Simulate S3 environment locally")
    parser.add_argument("--sequential", action="store_true", help="Force sequential processing (no parallel)")
    return parser.parse_args()

# Main execution
if __name__ == '__main__':
    try:
        # Parse arguments
        args = parse_args()
        
        # Set mock mode if requested
        if args.mock:
            MOCK_SNOWFLAKE = True
            logger.info("Running in MOCK mode (no Snowflake connection)")
        
        # Disable parallel processing if requested
        if args.sequential:
            PARALLEL_PROCESSING = False
            logger.info("Forced sequential processing")
        
        # Simulate S3 environment if requested
        if args.s3:
            os.environ['AWS_LAMBDA_FUNCTION_NAME'] = 'local_simulation'
            logger.info("Simulating Lambda/S3 environment locally")
            
            # Ensure S3 bucket is specified
            if not S3_DATA_BUCKET:
                logger.warning("S3_DATA_BUCKET is not set. Please set DATA_BUCKET environment variable for S3 simulation.")
        
        # Display configuration
        log_config()
        
        # Specific date from arguments
        specific_date = args.date
        
        # Record start time
        start_time = datetime.now()
        logger.info(f"Process started at: {start_time}")
        
        # Execute main processing
        results = process_silver_to_gold(specific_date)
        
        # Record end time and duration
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"Process completed at: {end_time}")
        logger.info(f"Total duration: {duration}")
        
        # Display summary
        print(f"\n=== EXECUTION SUMMARY ===")
        print(f"Started at   : {start_time}")
        print(f"Finished at  : {end_time}")
        print(f"Total duration: {duration}")
        print(f"Dates processed: {results['dates_processed']}")
        print(f"Tables processed: {results['tables_processed']}")
        print(f"Execution log: {log_file}")
        print(f"=======================\n")
        
        # Exit with success if anything was processed, failure otherwise
        sys.exit(0 if results['tables_processed'] > 0 else 1)
    
    except Exception as e:
        # Capture all unhandled exceptions
        logger.error(f"CRITICAL ERROR: {e}")
        logger.error(traceback.format_exc())
        print(f"\n[CRITICAL ERROR]")
        print(f"An error occurred during execution:")
        print(f"{str(e)}")
        print(f"Check the log for details: {log_file}\n")
        sys.exit(1)