#!/usr/bin/env python3
"""
This script processes Parquet files from the gold layer and loads them into Snowflake.
It supports date-specific processing, data validation, and enrichment with brand codes.
"""
import os
import io
import csv
import sys
import uuid
import json
import re
import logging
import traceback
import argparse
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector
import psutil
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add parent directory to path to allow importing utilities
sys.path.append(str(Path(__file__).parent))
from utils import file_utils, validation_utils

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
GOLD_DIR = DATA_DIR / os.getenv('GOLD_SUBDIR', 'gold')
LOGS_DIR = DATA_DIR / os.getenv('LOGS_SUBDIR', 'logs')
CONFIG_DIR = BASE_DIR / "config"

# Convert paths to absolute for logging
GOLD_DIR = GOLD_DIR.resolve()
LOGS_DIR = LOGS_DIR.resolve()
CONFIG_DIR = CONFIG_DIR.resolve()

# Processing parameters
EXPECTED_TABLES = os.getenv('EXPECTED_TABLES', "Device,EPG,Playback,User,Smartcard,VODCatalog,VODCatalogExtended").split(',')
MOCK_SNOWFLAKE = os.getenv('MOCK_SNOWFLAKE', 'true').lower() == 'true'
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
PARALLEL_PROCESSING = os.getenv('PARALLEL_PROCESSING', 'true').lower() == 'true'
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '4'))
LOADED_BY = os.getenv('LOADED_BY', 'ETL_GOLD_TO_SNOWFLAKE')  # Identifier for the loading process

# Snowflake parameters
SF_USER = os.getenv('SNOWFLAKE_USER')
SF_PWD = os.getenv('SNOWFLAKE_PASSWORD')
SF_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SF_WHS = os.getenv('SNOWFLAKE_WAREHOUSE')
SF_DB = os.getenv('SNOWFLAKE_DATABASE')
SF_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'STG_SG')
SF_MONITORING_SCHEMA = os.getenv('SNOWFLAKE_MONITORING_SCHEMA', 'STG_SG_MONITORING')

# Ensure these directories exist
LOGS_DIR.mkdir(exist_ok=True, parents=True)

# Configure logging
log_file = LOGS_DIR / "gold_to_snowflake_execution.log"
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
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, uuid.UUID):
            return str(obj)
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
        "GOLD_DIR": str(GOLD_DIR),
        "LOGS_DIR": str(LOGS_DIR),
        "CONFIG_DIR": str(CONFIG_DIR),
        "EXPECTED_TABLES": EXPECTED_TABLES,
        "MOCK_SNOWFLAKE": MOCK_SNOWFLAKE,
        "LOG_LEVEL": LOG_LEVEL,
        "PARALLEL_PROCESSING": PARALLEL_PROCESSING,
        "MAX_WORKERS": MAX_WORKERS,
        "LOADED_BY": LOADED_BY
    }
    logger.info("Configuration:")
    for key, value in config.items():
        logger.info(f"  {key}: {value}")

# Load configuration files
def load_config_files():
    """Load configuration files (brand_configs.json, table_schemas.json, validation_rules.json)"""
    configs = {}
    
    # Load brand mapping file
    brand_config_path = CONFIG_DIR / "brand_configs.json"
    try:
        with open(brand_config_path, 'r', encoding='utf-8') as f:
            configs['brands'] = json.load(f)
        logger.info(f"Brand configuration loaded from {brand_config_path}")
    except Exception as e:
        logger.error(f"Error loading brand configuration: {e}")
        configs['brands'] = {"brands": []}
    
    # Load table schema
    table_schema_path = CONFIG_DIR / "table_schemas.json"
    try:
        with open(table_schema_path, 'r', encoding='utf-8') as f:
            configs['schemas'] = json.load(f)
        logger.info(f"Table schemas loaded from {table_schema_path}")
    except Exception as e:
        logger.error(f"Error loading table schemas: {e}")
        configs['schemas'] = {}
    
    # Load validation rules
    validation_rules_path = CONFIG_DIR / "validation_rules.json"
    try:
        with open(validation_rules_path, 'r', encoding='utf-8') as f:
            configs['validations'] = json.load(f)
        logger.info(f"Validation rules loaded from {validation_rules_path}")
    except Exception as e:
        logger.error(f"Error loading validation rules: {e}")
        configs['validations'] = {}
    
    return configs

# Function to extract table name and date from filename
def parse_gold_filename(filename):
    """
    Extracts table name and date from Gold filename
    Example: Device_20250416.parquet -> ('Device', '20250416')
    """
    parts = filename.stem.split('_')
    if len(parts) >= 2 and parts[-1].isdigit() and len(parts[-1]) == 8:
        date = parts[-1]
        table_name = '_'.join(parts[:-1])
        return table_name, date
    
    return None, None

# Function to enrich data with brand information
def enrich_brand_data(df, brand_configs):
    """
    Enriches data with complete brand information based on configuration file
    """
    if 'BRAND' not in df.columns:
        logger.warning("BRAND column missing in data")
        return df
    
    # Create a copy of the DataFrame
    enriched_df = df.copy()
    
    # Create brand mapping dictionary
    brand_map = {
        brand.get('file_name', ''): brand.get('brand_code', '') 
        for brand in brand_configs.get('brands', [])
    }
    
    # Apply mapping to each row
    def map_brand(row):
        brand_name = row['BRAND']
        row['BRAND_CODE'] = brand_map.get(brand_name, brand_name)
        return row
    
    enriched_df = enriched_df.apply(map_brand, axis=1)
    return enriched_df

# Generic validation function that uses rules from validation_rules.json
def validate_data(df, table_name, validation_rules):
    """
    Validate data using rules defined in validation_rules.json
    
    Args:
        df: DataFrame to validate
        table_name: Name of the table being validated
        validation_rules: Dictionary containing validation rules from validation_rules.json
        
    Returns:
        Tuple of (errors_dict, error_rows_list)
    """
    errors = {}
    error_rows = []
    
    # If no rules for this table or table not in rules, return empty results
    if table_name not in validation_rules:
        logger.warning(f"No validation rules found for table {table_name}")
        return {}, []
    
    table_rules = validation_rules.get(table_name, {})
    
    # Process valid_types rule (allowed values in a column)
    if 'valid_types' in table_rules:
        rule = table_rules['valid_types']
        column = rule.get('column')
        allowed_values = rule.get('allowed_values', [])
        error_type = rule.get('error_type', 'invalid_types')
        
        if column in df.columns:
            invalid_values = df[~df[column].isin(allowed_values) & ~df[column].isna()]
            if len(invalid_values) > 0:
                errors[error_type] = len(invalid_values)
                error_rows.extend(invalid_values.index.tolist())
    
    # Process required_fields rule (non-null values)
    if 'required_fields' in table_rules:
        rule = table_rules['required_fields']
        columns = rule.get('columns', [])
        error_type = rule.get('error_type', 'missing_required_fields')
        
        for field in columns:
            if field in df.columns:
                missing_field = df[df[field].isna()]
                if len(missing_field) > 0:
                    errors.setdefault(error_type, {})[field] = len(missing_field)
                    error_rows.extend(missing_field.index.tolist())
    
    # Process uniqueness rule (no duplicates in columns)
    if 'uniqueness' in table_rules:
        rule = table_rules['uniqueness']
        columns = rule.get('columns', [])
        error_type = rule.get('error_type', 'duplicate_records')
        
        if all(col in df.columns for col in columns):
            duplicates = df[df.duplicated(subset=columns, keep='first')]
            if len(duplicates) > 0:
                errors[error_type] = len(duplicates)
                error_rows.extend(duplicates.index.tolist())
    
    # Process duration_validation rule for Playback
    if 'duration_validation' in table_rules:
        rule = table_rules['duration_validation']
        error_type = rule.get('error_type', 'invalid_duration')
        
        # For PlayDurationExPause <= AssetDuration OR AssetDuration == 0
        if 'PlayDurationExPause' in df.columns and 'AssetDuration' in df.columns:
            # Convert to numeric
            df['PlayDurationExPause_num'] = pd.to_numeric(df['PlayDurationExPause'], errors='coerce')
            df['AssetDuration_num'] = pd.to_numeric(df['AssetDuration'], errors='coerce')
            
            # Apply validation logic
            invalid_duration = df[
                (df['PlayDurationExPause_num'] > df['AssetDuration_num']) & 
                (df['AssetDuration_num'] != 0)
            ]
            
            if len(invalid_duration) > 0:
                errors[error_type] = len(invalid_duration)
                error_rows.extend(invalid_duration.index.tolist())
    
    # Process duration_consistency rule for Playback
    if 'duration_consistency' in table_rules:
        rule = table_rules['duration_consistency']
        error_type = rule.get('error_type', 'inconsistent_duration')
        
        # For PlayDuration >= PlayDurationExPause
        if 'PlayDuration' in df.columns and 'PlayDurationExPause' in df.columns:
            # Convert to numeric
            df['PlayDuration_num'] = pd.to_numeric(df['PlayDuration'], errors='coerce')
            df['PlayDurationExPause_num'] = pd.to_numeric(df['PlayDurationExPause'], errors='coerce')
            
            # Apply validation logic
            inconsistent_duration = df[df['PlayDuration_num'] < df['PlayDurationExPause_num']]
            
            if len(inconsistent_duration) > 0:
                errors[error_type] = len(inconsistent_duration)
                error_rows.extend(inconsistent_duration.index.tolist())
    
    # Process user_existence rule for Playback
    if 'user_existence' in table_rules:
        rule = table_rules['user_existence']
        column = rule.get('column')
        error_type = rule.get('error_type', 'missing_user')
        
        if column in df.columns:
            missing_values = df[df[column].isna()]
            if len(missing_values) > 0:
                errors[error_type] = len(missing_values)
                error_rows.extend(missing_values.index.tolist())
    
    # Process episode_validity rule for EPG
    if 'episode_validity' in table_rules:
        rule = table_rules['episode_validity']
        error_type = rule.get('error_type', 'invalid_episodes')
        
        # For NOT (season IS NULL AND episode IS NOT NULL)
        if 'season' in df.columns and 'episode' in df.columns:
            invalid_episodes = df[df['season'].isna() & ~df['episode'].isna()]
            
            if len(invalid_episodes) > 0:
                errors[error_type] = len(invalid_episodes)
                error_rows.extend(invalid_episodes.index.tolist())
    
    # Process episode_consistency rule for VODCatalog
    if 'episode_consistency' in table_rules:
        rule = table_rules['episode_consistency']
        error_type = rule.get('error_type', 'invalid_episodes')
        
        # For series_episode <= series_episode_count OR series_episode IS NULL OR series_episode_count IS NULL OR series_episode_count == 0
        if 'series_episode' in df.columns and 'series_episode_count' in df.columns:
            # Convert to numeric
            df['series_episode_num'] = pd.to_numeric(df['series_episode'], errors='coerce')
            df['series_episode_count_num'] = pd.to_numeric(df['series_episode_count'], errors='coerce')
            
            # Apply validation logic
            invalid_episodes = df[
                (df['series_episode_num'] > df['series_episode_count_num']) & 
                (~df['series_episode_num'].isna()) & 
                (~df['series_episode_count_num'].isna()) & 
                (df['series_episode_count_num'] != 0)
            ]
            
            if len(invalid_episodes) > 0:
                errors[error_type] = len(invalid_episodes)
                error_rows.extend(invalid_episodes.index.tolist())
    
    # Return unique error rows
    return errors, list(set(error_rows))

# Function to insert logs in Snowflake
def insert_gold_log(process_id, process_date, execution_start, execution_end, date_folder, 
                   table_name, brand, reseller, records_before_agg, records_after_agg, 
                   duplicates_removed, brands_aggregated, checks_total, checks_passed, 
                   checks_failed, checks_skipped, records_valid, records_warning, records_error, 
                   snowflake_rows_inserted, snowflake_rows_updated, snowflake_rows_rejected, 
                   processing_time_ms, memory_usage_mb, status, error_message, 
                   business_checks_details, aggregation_details, loading_details):
    """
    Inserts a log record into the GOLD_LOGS table in Snowflake
    """
    try:
        # Convert details to serializable JSON
        business_checks_json = json.dumps(business_checks_details, cls=CustomJSONEncoder)
        aggregation_details_json = json.dumps(aggregation_details, cls=CustomJSONEncoder)
        loading_details_json = json.dumps(loading_details, cls=CustomJSONEncoder)
        
        if MOCK_SNOWFLAKE:
            # Local version for testing
            log_entry = {
                "PROCESS_ID": str(process_id),
                "PROCESS_DATE": process_date,
                "EXECUTION_START": execution_start,
                "EXECUTION_END": execution_end,
                "DATE_FOLDER": date_folder,
                "TABLE_NAME": table_name,
                "BRAND": brand,
                "RESELLER": reseller,
                "RECORDS_BEFORE_AGG": records_before_agg,
                "RECORDS_AFTER_AGG": records_after_agg,
                "DUPLICATES_REMOVED": duplicates_removed,
                "BRANDS_AGGREGATED": brands_aggregated,
                "CHECKS_TOTAL": checks_total,
                "CHECKS_PASSED": checks_passed,
                "CHECKS_FAILED": checks_failed,
                "CHECKS_SKIPPED": checks_skipped,
                "RECORDS_VALID": records_valid,
                "RECORDS_WARNING": records_warning,
                "RECORDS_ERROR": records_error,
                "SNOWFLAKE_ROWS_INSERTED": snowflake_rows_inserted,
                "SNOWFLAKE_ROWS_UPDATED": snowflake_rows_updated,
                "SNOWFLAKE_ROWS_REJECTED": snowflake_rows_rejected,
                "PROCESSING_TIME_MS": processing_time_ms,
                "MEMORY_USAGE_MB": memory_usage_mb,
                "STATUS": status,
                "ERROR_MESSAGE": error_message,
                "BUSINESS_CHECKS_DETAILS": json.loads(business_checks_json),
                "AGGREGATION_DETAILS": json.loads(aggregation_details_json),
                "LOADING_DETAILS": json.loads(loading_details_json)
            }
            
            # Create a unique filename for the log
            filename = LOGS_DIR / f"gold_logs_{table_name}_{brand}_{date_folder}_{process_id}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(log_entry, f, indent=2, cls=CustomJSONEncoder)
            
            logger.info(f"Local log created: {filename}")
            return True
        else:
            # Snowflake connection
            ctx = snowflake.connector.connect(
                user=SF_USER,
                password=SF_PWD,
                account=SF_ACCOUNT,
                warehouse=SF_WHS,
                database=SF_DB,
                schema=SF_MONITORING_SCHEMA
            )
            cs = ctx.cursor()
            
            # Prepare SQL query
            sql = f"""
                INSERT INTO {SF_MONITORING_SCHEMA}.GOLD_LOGS
                (
                    PROCESS_ID, PROCESS_DATE, EXECUTION_START, EXECUTION_END, 
                    DATE_FOLDER, TABLE_NAME, BRAND, RESELLER, 
                    RECORDS_BEFORE_AGG, RECORDS_AFTER_AGG, DUPLICATES_REMOVED, BRANDS_AGGREGATED, 
                    CHECKS_TOTAL, CHECKS_PASSED, CHECKS_FAILED, CHECKS_SKIPPED, 
                    RECORDS_VALID, RECORDS_WARNING, RECORDS_ERROR, 
                    SNOWFLAKE_ROWS_INSERTED, SNOWFLAKE_ROWS_UPDATED, SNOWFLAKE_ROWS_REJECTED, 
                    PROCESSING_TIME_MS, MEMORY_USAGE_MB, STATUS, ERROR_MESSAGE, 
                    BUSINESS_CHECKS_DETAILS, AGGREGATION_DETAILS, LOADING_DETAILS
                )
                VALUES (
                    '{process_id}', 
                    '{process_date}',
                    '{execution_start}',
                    '{execution_end}',
                    '{date_folder}',
                    '{table_name}',
                    '{brand}',
                    '{reseller if reseller else ''}',
                    {records_before_agg},
                    {records_after_agg},
                    {duplicates_removed},
                    {brands_aggregated},
                    {checks_total},
                    {checks_passed},
                    {checks_failed},
                    {checks_skipped},
                    {records_valid},
                    {records_warning},
                    {records_error},
                    {snowflake_rows_inserted},
                    {snowflake_rows_updated},
                    {snowflake_rows_rejected},
                    {processing_time_ms},
                    {memory_usage_mb if memory_usage_mb is not None else 'NULL'},
                    '{status}',
                    '{error_message if error_message else ''}',
                    PARSE_JSON('{business_checks_json.replace("'", "''")}'),
                    PARSE_JSON('{aggregation_details_json.replace("'", "''")}'),
                    PARSE_JSON('{loading_details_json.replace("'", "''")}')
                );
            """
            
            cs.execute(sql)
            ctx.commit()
            logger.info(f"Log inserted in Snowflake for {table_name} {brand} {date_folder}")
            
            cs.close()
            ctx.close()
            return True
            
    except Exception as e:
        logger.error(f"Error inserting log: {e}")
        logger.error(traceback.format_exc())
        return False

# Function to load data into Snowflake
def load_to_snowflake(df, table_name):
    """
    Loads DataFrame data into the corresponding Snowflake table
    Returns a tuple (inserted, updated, rejected)
    """
    try:
        if MOCK_SNOWFLAKE:
            # For testing, simulate loading
            rows = len(df)
            logger.info(f"[MOCK] Simulated loading of {rows} rows into table {table_name}")
            # For testing, consider all rows as inserted
            return rows, 0, 0
        
        # Target Snowflake table name
        sf_table_name = f"AGG_{table_name.upper()}"
        
        # Convert DataFrame to CSV for loading
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_NONNUMERIC)
        csv_buffer.seek(0)
        
        # Snowflake connection
        conn = snowflake.connector.connect(
            user=SF_USER,
            password=SF_PWD,
            account=SF_ACCOUNT,
            warehouse=SF_WHS,
            database=SF_DB,
            schema=SF_SCHEMA
        )
        
        try:
            cursor = conn.cursor()
            
            # Create a temporary stage for loading
            stage_name = f"TEMP_STAGE_{uuid.uuid4().hex}"
            cursor.execute(f"CREATE TEMPORARY STAGE {stage_name}")
            
            # Determine available columns in target table
            cursor.execute(f"DESCRIBE TABLE {SF_SCHEMA}.{sf_table_name}")
            table_columns = [row[0].upper() for row in cursor.fetchall()]
            
            # Filter DataFrame to include only columns that exist in the table
            columns_to_use = [col for col in df.columns if col.upper() in table_columns]
            df_filtered = df[columns_to_use]
            
            # Recreate CSV with only existing columns
            csv_buffer = io.StringIO()
            df_filtered.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_NONNUMERIC)
            csv_buffer.seek(0)
            
            # Load data into stage
            put_sql = f"PUT file://{csv_buffer} @{stage_name}"
            cursor.execute(put_sql)
            
            # Determine target columns for COPY
            columns_str = ", ".join([f'"{col.upper()}"' for col in columns_to_use])
            
            # Load data into table
            copy_sql = f"""
            COPY INTO {SF_SCHEMA}.{sf_table_name} ({columns_str})
            FROM @{stage_name}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
            ON_ERROR = 'CONTINUE'
            """
            cursor.execute(copy_sql)
            
            # Get loading results
            copy_results = cursor.fetchall()
            rows_loaded = sum(int(row[3]) for row in copy_results) if copy_results else 0
            errors = sum(int(row[4]) for row in copy_results) if copy_results else 0
            
            # In COPY mode, all correct rows are inserted
            # (Snowflake will update if primary key already exists)
            rows_inserted = rows_loaded
            rows_updated = 0  # No distinction between insertion and update in COPY mode
            
            logger.info(f"Snowflake loading successful for {sf_table_name}: {rows_inserted} rows loaded, {errors} errors")
            
            return rows_inserted, rows_updated, errors
            
        finally:
            # Clean up stage
            try:
                cursor.execute(f"DROP STAGE IF EXISTS {stage_name}")
            except:
                pass
            
            # Close connection
            cursor.close()
            conn.close()
        
    except Exception as e:
        logger.error(f"Error loading into Snowflake: {e}")
        logger.error(traceback.format_exc())
        return 0, 0, len(df)

# Function to process a Gold file
def process_gold_file(file_path, configs):
    """
    Processes a single Gold file and loads the data into Snowflake
    """
    try:
        start_time = datetime.now()
        process_id = str(uuid.uuid4())
        memory_start = psutil.Process().memory_info().rss / (1024 * 1024)  # In MB
        
        # Extract table name and date from filename
        table_name, date_folder = parse_gold_filename(file_path)
        
        if not table_name or not date_folder:
            logger.error(f"Invalid filename format: {file_path}")
            return False
        
        logger.info(f"Processing file {file_path}: table={table_name}, date={date_folder}")
        
        # Read Parquet file
        df = pd.read_parquet(file_path)
        records_before_agg = len(df)
        
        if records_before_agg == 0:
            logger.warning(f"No data found in file {file_path}")
            return False
        
        # Enrich data with brand information
        df = enrich_brand_data(df, configs.get('brands', {}))
        
        # Check if data contains BRAND column
        if 'BRAND' not in df.columns:
            logger.error(f"BRAND column missing in file {file_path}")
            return False
        
        # Process by brand
        brands = df['BRAND'].unique()
        
        for brand in brands:
            brand_df = df[df['BRAND'] == brand]
            brand_records = len(brand_df)
            
            # Get brand code
            brand_code = brand_df['BRAND_CODE'].iloc[0] if 'BRAND_CODE' in brand_df.columns else brand
            
            # Remove temporary columns
            if 'BRAND_CODE' in brand_df.columns:
                brand_df = brand_df.drop(columns=['BRAND_CODE'])
            
            # Determine key columns for deduplication
            key_map = {
                'User': ['Userid'],
                'Device': ['Userid', 'Serial'],
                'Smartcard': ['SmartcardId'],
                'Playback': ['PlaySessionID'],
                'EPG': ['broadcast_datetime', 'station_id'],
                'VODCatalog': ['external_id'],
                'VODCatalogExtended': ['external_id']
            }
            key_columns = key_map.get(table_name, [])
            
            # Deduplicate data if key columns are defined
            duplicates_removed = 0
            if key_columns and all(col in brand_df.columns for col in key_columns):
                before_dedup = len(brand_df)
                brand_df = brand_df.drop_duplicates(subset=key_columns, keep='first')
                duplicates_removed = before_dedup - len(brand_df)
            
            records_after_agg = len(brand_df)
            
            # Perform business validations using rules from JSON
            validation_errors, error_rows = validate_data(brand_df, table_name, configs.get('validations', {}))
            
            # Calculate validation statistics
            checks_total = sum(1 for _ in validation_errors)
            checks_failed = len(validation_errors)
            checks_passed = checks_total - checks_failed
            records_valid = records_after_agg - len(error_rows)
            
            # Prepare validation details for log
            business_checks_details = {
                "validation_errors": validation_errors,
                "error_count": len(error_rows)
            }
            
            # Prepare aggregation details for log
            aggregation_details = {
                "records_before": brand_records,
                "records_after": records_after_agg,
                "duplicates_removed": duplicates_removed,
                "key_columns": key_columns
            }
            
            # Determine validation status
            status = "OK" if checks_failed == 0 else "WARNING" if records_valid > 0 else "ERROR"
            
            # Load data into Snowflake
            execution_start = start_time.strftime('%Y-%m-%d %H:%M:%S')
            
            # Normalize column names for Snowflake (if needed)
            snowflake_df = brand_df.copy()
            
            # Add LOADDATE and LOADEDBY columns
            current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            snowflake_df['LOADDATE'] = current_timestamp
            snowflake_df['LOADEDBY'] = LOADED_BY  # Use global variable
            
            # Resolve potential typing issues for Snowflake
            for col in snowflake_df.columns:
                if snowflake_df[col].dtype == 'object':
                    snowflake_df[col] = snowflake_df[col].astype(str)
                elif pd.api.types.is_datetime64_any_dtype(snowflake_df[col]):
                    snowflake_df[col] = snowflake_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Load into Snowflake
            rows_inserted, rows_updated, rows_rejected = load_to_snowflake(snowflake_df, table_name)
            
            # Prepare loading details for log
            loading_details = {
                "snowflake_rows_inserted": rows_inserted,
                "snowflake_rows_updated": rows_updated,
                "snowflake_rows_rejected": rows_rejected,
                "table_name": f"AGG_{table_name.upper()}"
            }
            
            # Calculate performance metrics
            end_time = datetime.now()
            execution_end = end_time.strftime('%Y-%m-%d %H:%M:%S')
            processing_time_ms = int((end_time - start_time).total_seconds() * 1000)
            memory_end = psutil.Process().memory_info().rss / (1024 * 1024)  # In MB
            memory_usage_mb = round(memory_end - memory_start, 2)
            
            # Insert log
            process_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            error_message = None if status == "OK" else f"{checks_failed} checks failed with {len(error_rows)} records in error"
            
            insert_gold_log(
                process_id=process_id,
                process_date=process_date,
                execution_start=execution_start,
                execution_end=execution_end,
                date_folder=date_folder,
                table_name=table_name,
                brand=brand_code,  # Use brand code
                reseller="",       # Empty field as not used anymore
                records_before_agg=brand_records,
                records_after_agg=records_after_agg,
                duplicates_removed=duplicates_removed,
                brands_aggregated=1,  # Only one brand per iteration
                checks_total=checks_total,
                checks_passed=checks_passed,
                checks_failed=checks_failed,
                checks_skipped=0,
                records_valid=records_valid,
                records_warning=0,
                records_error=len(error_rows),
                snowflake_rows_inserted=rows_inserted,
                snowflake_rows_updated=rows_updated,
                snowflake_rows_rejected=rows_rejected,
                processing_time_ms=processing_time_ms,
                memory_usage_mb=memory_usage_mb,
                status=status,
                error_message=error_message,
                business_checks_details=business_checks_details,
                aggregation_details=aggregation_details,
                loading_details=loading_details
            )
            
            logger.info(f"Processing completed for {table_name} / {brand} / {date_folder} with status {status}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        logger.error(traceback.format_exc())
        return False

# Main processing function
def process_gold_to_snowflake(specific_date=None):
    """
    Processes files from Gold layer to load them into Snowflake
    
    Args:
        specific_date: Optional date string in YYYYMMDD format to filter processing
        
    Returns:
        Boolean indicating success
    """
    logger.info("Starting GOLD -> SNOWFLAKE processing")
    if specific_date:
        logger.info(f"Processing restricted to date: {specific_date}")
    
    # 1. Load configuration files
    configs = load_config_files()
    
    # 2. List all Parquet files in Gold layer
    try:
        # Define pattern based on specific_date
        if specific_date:
            gold_files = list(GOLD_DIR.glob(f'*_{specific_date}.parquet'))
        else:
            gold_files = list(GOLD_DIR.glob('*.parquet'))
            
        if not gold_files:
            logger.warning(f"No Parquet files found in {GOLD_DIR}" + 
                         (f" for date {specific_date}" if specific_date else ""))
            return False
        
        logger.info(f"Files found in Gold: {len(gold_files)}")
        
        # 3. Process each Gold file
        success = True
        
        if PARALLEL_PROCESSING and len(gold_files) > 1:
            logger.info(f"Parallel processing of {len(gold_files)} files with {MAX_WORKERS} workers")
            futures = {}
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Submit tasks
                for file_path in gold_files:
                    future = executor.submit(process_gold_file, file_path, configs)
                    futures[future] = file_path
                
                # Collect results
                for future in as_completed(futures):
                    file_path = futures[future]
                    try:
                        result = future.result()
                        if not result:
                            logger.warning(f"Processing failed for {file_path}")
                            success = False
                    except Exception as e:
                        logger.error(f"Error in parallel processing of {file_path}: {e}")
                        logger.error(traceback.format_exc())
                        success = False
        else:
            logger.info(f"Sequential processing of {len(gold_files)} files")
            for file_path in gold_files:
                try:
                    result = process_gold_file(file_path, configs)
                    if not result:
                        logger.warning(f"Processing failed for {file_path}")
                        success = False
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {e}")
                    logger.error(traceback.format_exc())
                    success = False
    
    except Exception as e:
        logger.error(f"Error during GOLD -> SNOWFLAKE processing: {e}")
        logger.error(traceback.format_exc())
        return False
    
    return success

# Parse command line arguments
def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Process files from Gold layer to Snowflake")
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
        success = process_gold_to_snowflake(date_to_process)
        
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
        print(f"GOLD_DIR: {GOLD_DIR}")
        print(f"LOGS_DIR: {LOGS_DIR}")
        print(f"CONFIG_DIR: {CONFIG_DIR}")
        
        logger.info("Starting GOLD to SNOWFLAKE processing")
        
        # Display configuration
        log_config()
        
        # Parse command line arguments
        args = parse_args()
        specific_date = args.date
        
        # Record start time
        start_time = datetime.now()
        logger.info(f"Process started at: {start_time}")
        
        # Execute main processing
        success = process_gold_to_snowflake(specific_date)
        
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