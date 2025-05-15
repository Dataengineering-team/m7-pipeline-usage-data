#!/usr/bin/env python3
"""
This script processes CSV files from the landing/raw layer and converts them to Parquet format in the silver layer.
AWS Lambda version with S3 integration.
"""
import os
import json
import re
import logging
import traceback
import csv
import argparse
import sys
import uuid
from datetime import datetime
import pandas as pd
from pathlib import Path
import boto3
from botocore.exceptions import ClientError

# ===== CONFIGURATION =====
# Base paths - adaptation pour Lambda
if 'AWS_LAMBDA_FUNCTION_NAME' in os.environ:
    # En environnement Lambda, utiliser /tmp pour les données temporaires
    DATA_DIR = Path(os.getenv('DATA_DIR', '/tmp/data'))
    # Créer les répertoires nécessaires
    DATA_DIR.mkdir(exist_ok=True, parents=True)
else:
    # Configuration locale simplifiée
    BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__))).parent
    DATA_DIR = Path(os.getenv('DATA_DIR', BASE_DIR / "data"))

# S3 bucket pour les données
S3_DATA_BUCKET = os.getenv('DATA_BUCKET')

# Define explicit paths relative to DATA_DIR
LANDING_DIR = DATA_DIR / os.getenv('LANDING_SUBDIR', 'landing')
RAW_DIR = DATA_DIR / os.getenv('RAW_SUBDIR', 'raw')
SILVER_DIR = DATA_DIR / os.getenv('SILVER_SUBDIR', 'silver')
LOGS_DIR = DATA_DIR / os.getenv('LOGS_SUBDIR', 'logs')

# Processing parameters
EXPECTED_TABLES = os.getenv('EXPECTED_TABLES', "Device,EPG,Playback,User,Smartcard,VODCatalog,VODCatalogExtended").split(',')
MOCK_SNOWFLAKE = os.getenv('MOCK_SNOWFLAKE', 'true').lower() == 'true'
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
DELETE_SOURCE_FILES_AFTER_LOAD = os.getenv('DELETE_SOURCE_FILES_AFTER_LOAD', 'false').lower() == 'true'

# Snowflake parameters
SF_USER = os.getenv('SNOWFLAKE_USER')
SF_PWD = os.getenv('SNOWFLAKE_PASSWORD')
SF_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SF_WHS = os.getenv('SNOWFLAKE_WAREHOUSE')
SF_DB = os.getenv('SNOWFLAKE_DATABASE')
SF_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'MONITORING')
SF_MONITORING_SCHEMA = os.getenv('SNOWFLAKE_MONITORING_SCHEMA', 'STG_SG_MONITORING')

# Ensure these directories exist
for directory in [LANDING_DIR, RAW_DIR, SILVER_DIR, LOGS_DIR]:
    directory.mkdir(exist_ok=True, parents=True)

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
        "LANDING_DIR": str(LANDING_DIR),
        "RAW_DIR": str(RAW_DIR),
        "SILVER_DIR": str(SILVER_DIR),
        "LOGS_DIR": str(LOGS_DIR),
        "S3_DATA_BUCKET": S3_DATA_BUCKET,
        "EXPECTED_TABLES": EXPECTED_TABLES,
        "MOCK_SNOWFLAKE": MOCK_SNOWFLAKE,
        "LOG_LEVEL": LOG_LEVEL,
        "DELETE_SOURCE_FILES_AFTER_LOAD": DELETE_SOURCE_FILES_AFTER_LOAD
    }
    logger.info("Configuration:")
    for key, value in config.items():
        logger.info(f"  {key}: {value}")

# Utility functions
def slugify(name: str) -> str:
    """Transform 'My Brand Name' -> 'my_brand_name'"""
    s = name.lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_]", "", s)
    return s

def parse_filename(fn: str):
    """Parse the filename to extract brand, table and date"""
    if not fn.lower().endswith('.csv'):
        return None, None, None
    
    base = fn[:-4]  # Remove .csv extension
    parts = base.split('_')
    
    if len(parts) < 3:
        logger.warning(f"Filename does not follow expected pattern: {fn}")
        return None, None, None
    
    date = parts[-1]
    table = parts[-2]
    brand = '_'.join(parts[:-2])
    
    return brand, table, date

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

def insert_log_local(proc_date, brand, dt, details, status):
    """Local version of the insert_log function that writes to a JSON file"""
    log_entry = {
        "PROCESS_ID": str(uuid.uuid4()),
        "PROCESS_DATE": proc_date,
        "BRAND": brand,
        "EXTRACTION_DATE": dt,
        "FILE_COUNT": details.get('files_present', 0),
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
    
    filename = LOGS_DIR / f"raw_logs_{brand}_{dt}.json"
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(log_entry, f, indent=2, cls=CustomJSONEncoder)
        
        logger.info(f"Local log created: {filename}")
        
        # Upload log to S3 if in Lambda environment
        if 'AWS_LAMBDA_FUNCTION_NAME' in os.environ and S3_DATA_BUCKET:
            s3_log_key = f"logs/raw_logs_{brand}_{dt}.json"
            upload_to_s3(filename, S3_DATA_BUCKET, s3_log_key)
    except Exception as e:
        logger.error(f"Error writing local log: {e}")
        logger.error(traceback.format_exc())

def insert_log_snowflake(proc_date, brand, dt, details, status):
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
        
        # Count files present
        cnt = details.get('files_present', 0)
        
        # Simplify details to fit in VARCHAR(4000)
        simplified_details = {
            "files_present": cnt,
            "tables": {}
        }
        
        # Add simplified table information
        for table_name, table_info in details.get('details', {}).items():
            simplified_details["tables"][table_name] = {
                "status": table_info.get("status", "Unknown")
            }
            
            # Add basic CSV validation info if available
            if "csv_validation" in table_info:
                csv_validation = table_info["csv_validation"]
                simplified_details["tables"][table_name]["csv_valid"] = csv_validation.get("valid", False)
                
        # Convert to JSON and truncate if necessary
        json_details = json.dumps(simplified_details, cls=CustomJSONEncoder)
        if len(json_details) > 4000:
            logger.warning(f"Details JSON too large ({len(json_details)} chars), truncating to 4000 chars")
            json_details = json_details[:3997] + "..."
        
        # SQL query
        sql = f"""
            INSERT INTO {SF_MONITORING_SCHEMA}.RAW_LOGS
                (PROCESS_DATE, BRAND, EXTRACTION_DATE, FILE_COUNT, STATUS, DETAILS, RESELLER)
            VALUES (
                TO_TIMESTAMP_NTZ(%s), %s, %s, %s, %s, %s, %s
            );
        """
        
        # Default empty value for RESELLER
        reseller = ""
        
        # Parameters with RESELLER included
        params = (
            proc_date,      # PROCESS_DATE
            brand,          # BRAND
            dt,             # EXTRACTION_DATE
            cnt,            # FILE_COUNT
            status,         # STATUS
            json_details,   # DETAILS (standard JSON format)
            reseller        # RESELLER (empty by default)
        )
        
        cs.execute(sql, params)
        ctx.commit()
        logger.info(f"Inserted log for {brand} {dt} status={status}")
        
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

# CSV and file processing
def validate_csv(file_path):
    """Check if CSV file is well-formed and return information about its content"""
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
            
            # Extract relevant information from dialect
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

def read_csv_with_chunking(file_path, delimiter=',', chunk_size=100000):
    """Read CSV file using chunks to save memory"""
    try:
        chunks = pd.read_csv(file_path, sep=delimiter, chunksize=chunk_size)
        df = pd.concat(chunks)
        return df
    except Exception as e:
        logger.error(f"Error reading CSV with chunking: {e}")
        raise

def write_parquet_with_partitioning(df, dest_path, partition_cols=None):
    """Write DataFrame to Parquet format, with optional partitioning"""
    try:
        # If DataFrame is large, consider partitioning
        if len(df) > 1000000 and partition_cols:
            df.to_parquet(dest_path, partition_cols=partition_cols, index=False)
        else:
            df.to_parquet(dest_path, index=False)
        return True
    except Exception as e:
        logger.error(f"Error writing Parquet: {e}")
        return False

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
    
    # Collect files for processing
    contents = []
    
    # Get files from S3 or local filesystem depending on environment
    if 'AWS_LAMBDA_FUNCTION_NAME' in os.environ and S3_DATA_BUCKET:
        s3_objects = list_s3_files(S3_DATA_BUCKET, 'landing/')
        logger.info(f"Found {len(s3_objects)} objects in s3://{S3_DATA_BUCKET}/landing/")
        
        # Download and process each S3 file
        for obj in s3_objects:
            key = obj['Key']
            if not key.lower().endswith('.csv'):
                continue
                
            filename = key.split('/')[-1]  # Get just the filename
            brand, tbl, dt = parse_filename(filename)
            
            # Filter by date if specified
            if specific_date and dt != specific_date:
                continue
                
            # Download file for local processing
            local_file = LANDING_DIR / filename
            if download_from_s3(S3_DATA_BUCKET, key, local_file):
                contents.append({
                    'Key': filename,
                    'S3Key': key,
                    'Path': local_file,
                    'Brand': brand,
                    'Table': tbl,
                    'Date': dt
                })
    else:
        # Local file processing
        logger.info(f"Scanning local directory: {LANDING_DIR}")
        for file_path in LANDING_DIR.glob('*.csv'):
            brand, tbl, dt = parse_filename(file_path.name)
            # Filter by date if specified
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
        logger.warning(f"No CSV files found for processing" + 
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
        
        # Process each file in the group
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
                    logger.error(f"Invalid CSV format: {csv_validation.get('error', 'Unknown error')}")
                    detail['details'][tbl]['csv_validation'] = {
                        'valid': False,
                        'error': csv_validation.get('error', 'Unknown error')
                    }
                    continue
                
                # Store validation information
                detail['details'][tbl]['csv_validation'] = {
                    'valid': csv_validation['valid'],
                    'delimiter': csv_validation['delimiter'],
                    'columns_count': len(csv_validation.get('columns', [])),
                    'row_count_sample': csv_validation.get('row_count_sample', 0)
                }
                
                # Copy file to raw layer
                logger.info(f"Copying {src_file} to {dest_csv}")
                import shutil
                shutil.copy2(src_file, dest_csv)
                
                # Upload to S3 raw if in Lambda environment
                if 'AWS_LAMBDA_FUNCTION_NAME' in os.environ and S3_DATA_BUCKET:
                    s3_raw_key = f"raw/{dt}/{brand_slug}/{fn}"
                    upload_to_s3(dest_csv, S3_DATA_BUCKET, s3_raw_key)
                
                # Delete original if configured
                if DELETE_SOURCE_FILES_AFTER_LOAD and dest_csv.exists():
                    # Delete from S3 if in Lambda environment
                    if 'AWS_LAMBDA_FUNCTION_NAME' in os.environ and S3_DATA_BUCKET and 'S3Key' in obj:
                        delete_s3_object(S3_DATA_BUCKET, obj['S3Key'])
                    
                    # Delete local file
                    os.remove(src_file)
                    logger.info(f"Removed original file: {src_file}")
                
                detail['details'][tbl]['moved_to'] = str(dest_csv)
                total_moved += 1
                
                # Convert to Parquet
                logger.info(f"Converting {dest_csv} to Parquet format")
                
                # Use detected delimiter
                delimiter = csv_validation.get('delimiter', ',')
                
                try:
                    # Handle large files with chunking if needed
                    file_size = os.path.getsize(dest_csv)
                    
                    if file_size > 100 * 1024 * 1024:  # If file is larger than 100MB
                        logger.info(f"Large file detected ({file_size} bytes), using chunking")
                        df = read_csv_with_chunking(dest_csv, delimiter=delimiter)
                    else:
                        df = pd.read_csv(dest_csv, sep=delimiter)
                    
                    # Parquet filename is the same but with .parquet extension
                    parquet_filename = f"{fn[:-4]}.parquet"
                    dest_parquet = silver_dest_dir / parquet_filename
                    
                    # Write the Parquet file
                    write_parquet_with_partitioning(df, dest_parquet)
                    detail['details'][tbl]['parquet_key'] = str(dest_parquet)
                    logger.info(f"Parquet written to {dest_parquet}")
                    
                    # Upload to S3 silver if in Lambda environment
                    if 'AWS_LAMBDA_FUNCTION_NAME' in os.environ and S3_DATA_BUCKET:
                        s3_silver_key = f"silver/{dt}/{brand_slug}/{parquet_filename}"
                        upload_to_s3(dest_parquet, S3_DATA_BUCKET, s3_silver_key)
                        
                except Exception as e:
                    logger.error(f"Error converting {fn} to Parquet: {e}")
                    logger.error(traceback.format_exc())
                    detail['details'][tbl]['parquet_error'] = str(e)
                
            except Exception as e:
                logger.error(f"Error processing {fn}: {e}")
                logger.error(traceback.format_exc())
                detail['details'][tbl]['error'] = str(e)
                detail['details'][tbl]['traceback'] = str(traceback.format_exc())
        
        # Determine status: OK if all expected tables present, otherwise Partial
        expected_in_this_brand = [tbl for tbl in EXPECTED_TABLES if detail['details'].get(tbl, {}).get('status') == 'OK']
        status = 'OK' if len(expected_in_this_brand) == len(EXPECTED_TABLES) else 'Partial'
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Log insertion (local or Snowflake)
        if MOCK_SNOWFLAKE:
            insert_log_local(now, brand, dt, detail, status)
        else:
            insert_log_snowflake(now, brand, dt, detail, status)
    
    logger.info(f"Finished processing. Total moved: {total_moved}")
    return total_moved

# Main execution
if __name__ == '__main__':
    try:
        # Parse arguments
        args = parse_args()
        
        # Set mock mode if requested
        if args.mock:
            MOCK_SNOWFLAKE = True
            logger.info("Running in MOCK mode (no Snowflake connection)")
            
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
        moved_count = process_and_convert(specific_date)
        logger.info(f"Moved {moved_count} files.")
        
        # Verify results
        if verify_parquet_generation(specific_date):
            logger.info("[SUCCESS] Parquet conversion successful")
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

# Parse command line arguments
def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Process CSV files from landing to raw/silver layers")
    parser.add_argument("--date", type=str, help="Specific date to process (YYYYMMDD format)")
    parser.add_argument("--mock", action="store_true", help="Run in mock mode without connecting to Snowflake")
    parser.add_argument("--s3", action="store_true", help="Simulate S3 environment locally")
    return parser.parse_args()

# Lambda handler
def lambda_handler(event, context):
    """
    Handler function for AWS Lambda
    """
    logger.info(f"Lambda invocation started with event: {json.dumps(event)}")
    try:
        # Log configuration
        log_config()
        
        # Extract date from event
        date_to_process = None
        if isinstance(event, dict):
            # Extract date directly from event
            date_to_process = event.get('date')
            
            # If event is an S3 event, extract date from file path
            if 'Records' in event and event['Records']:
                for record in event['Records']:
                    if record.get('eventSource') == 'aws:s3' and 'object' in record.get('s3', {}):
                        s3_key = record['s3']['object']['key']
                        # Try to extract date from filename
                        filename = s3_key.split('/')[-1]
                        _, _, extracted_date = parse_filename(filename)
                        if extracted_date:
                            date_to_process = extracted_date
                            logger.info(f"Extracted date from S3 event: {extracted_date}")
                            break
        
        logger.info(f"Processing date: {date_to_process}")
        
        # Execute main processing
        moved = process_and_convert(date_to_process)
        
        # Verify results
        verification_successful = verify_parquet_generation(date_to_process)
        
        response = {
            'statusCode': 200, 
            'body': json.dumps({
                'files_moved': moved,
                'verification_successful': verification_successful,
                'message': 'Processing completed successfully' if moved > 0 else 'No files processed'
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