#!/usr/bin/env python3
"""
This script processes Parquet files from the gold layer and loads them directly into Snowflake.
Modified version for AWS Glue with Spark.
"""
import sys
import os
import uuid
import json
import logging
import traceback
from datetime import datetime
from pathlib import Path

# AWS Glue imports
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# PySpark imports
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType, IntegerType, FloatType, BooleanType
import pyspark.sql.utils

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SNOWFLAKE_USER',
    'SNOWFLAKE_PASSWORD',
    'SNOWFLAKE_ACCOUNT',
    'SNOWFLAKE_WAREHOUSE',
    'SNOWFLAKE_DATABASE',
    'SNOWFLAKE_SCHEMA',
    'SNOWFLAKE_MONITORING_SCHEMA',
    'S3_GOLD_PATH',
    'CONFIG_PATH',
    'LOGS_PATH',
    'DATE_FILTER',
    'TABLE_FILTER',
    'LOADED_BY',
    'MOCK_MODE'
])

# Extract job parameters
SNOWFLAKE_USER = args.get('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = args.get('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = args.get('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = args.get('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = args.get('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = args.get('SNOWFLAKE_SCHEMA', 'STG_SG')
SNOWFLAKE_MONITORING_SCHEMA = args.get('SNOWFLAKE_MONITORING_SCHEMA', 'STG_SG_MONITORING')
S3_GOLD_PATH = args.get('S3_GOLD_PATH')
CONFIG_PATH = args.get('CONFIG_PATH')
LOGS_PATH = args.get('LOGS_PATH')
DATE_FILTER = args.get('DATE_FILTER', '')
TABLE_FILTER = args.get('TABLE_FILTER', '')
LOADED_BY = args.get('LOADED_BY', 'ETL_GOLD_TO_SNOWFLAKE')
MOCK_MODE = args.get('MOCK_MODE', 'false').lower() in ['true', 'yes', '1', 't', 'y']

# Table name mapping - from file name to Snowflake table name
TABLE_NAME_MAPPING = {
    "VODCatalog": "AGG_VOD_CATALOG",
    "VODCatalogExtended": "AGG_VOD_CATALOG_EXTENDED"
}

# Expected tables
EXPECTED_TABLES = "Device,EPG,Playback,User,Smartcard,VODCatalog,VODCatalogExtended".split(',')

# Special column treatment - fields that need special handling during load
COLUMN_TYPE_CONVERSIONS = {
    "AGG_VOD_CATALOG": {
        "series_last_episode_date": "string"
    }
}

# Initialize Spark context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ===== HELPER FUNCTIONS =====

def extract_table_date(file_path):
    """Extract table name and date from file path"""
    # Get the filename from the full path (handle both S3 and local paths)
    if file_path.startswith('s3://'):
        # For S3 paths
        filename = file_path.split('/')[-1]
    else:
        # For local paths
        filename = Path(file_path).name
    
    # Strip the extension
    filename_without_ext = filename.split('.')[0]
    
    parts = filename_without_ext.split('_')
    if len(parts) >= 2 and parts[-1].isdigit() and len(parts[-1]) == 8:
        date = parts[-1]
        table_name = '_'.join(parts[:-1])
        return table_name, date
    return None, None

def get_snowflake_table_name(table_name):
    """Get the correct Snowflake table name for a given table"""
    # Use mapping if exists, otherwise use standard AGG_TABLENAME format
    return TABLE_NAME_MAPPING.get(table_name, f"AGG_{table_name.upper()}")

def get_s3_file_list(base_path, specific_date=None, specific_table=None):
    """Get list of files to process from S3 based on filters"""
    import boto3
    from urllib.parse import urlparse
    
    # Parse S3 path
    parsed_url = urlparse(base_path)
    bucket_name = parsed_url.netloc
    prefix = parsed_url.path.lstrip('/')
    
    # Initialize S3 client
    s3 = boto3.client('s3')
    
    # Build prefix based on filters
    search_prefix = prefix
    if specific_table and specific_date:
        search_prefix = f"{prefix}/{specific_table}_{specific_date}.parquet"
    elif specific_table:
        search_prefix = f"{prefix}/{specific_table}_"
    elif specific_date:
        search_prefix = f"{prefix}/"  # No specific pattern for date-only filtering
    
    # List objects
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=search_prefix)
    
    files = []
    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('.parquet'):
                # Apply date filter if specified and not already in prefix
                if specific_date and not specific_table:
                    table_name, date = extract_table_date(key)
                    if date == specific_date:
                        files.append(f"s3://{bucket_name}/{key}")
                else:
                    files.append(f"s3://{bucket_name}/{key}")
    
    logger.info(f"Found {len(files)} files to process")
    return files

def read_s3_json(path):
    """Read a JSON file from S3"""
    try:
        # Use Spark to read the JSON file
        df = spark.read.json(path)
        
        # Convert to a Python dictionary
        if df.count() > 0:
            return df.toJSON().first()
        else:
            return {}
    except Exception as e:
        logger.error(f"Error reading JSON from {path}: {e}")
        return {}

def determine_key_columns(table_name):
    """Determine key columns for deduplication"""
    key_map = {
        'User': ['UserId'],
        'Device': ['UserId', 'Serial'],
        'Smartcard': ['SmartcardId'],
        'Playback': ['PlaySessionID'],
        'EPG': ['broadcast_datetime', 'station_id'],
        'VODCatalog': ['external_id'],
        'VODCatalogExtended': ['external_id']
    }
    return key_map.get(table_name, [])

def load_config_files():
    """Load configuration files from S3"""
    configs = {}
    
    # Load brand mapping file
    brand_config_path = f"{CONFIG_PATH}/brand_configs.json"
    try:
        configs['brands'] = json.loads(read_s3_json(brand_config_path))
        logger.info(f"Brand configuration loaded from {brand_config_path}")
    except Exception as e:
        logger.error(f"Error loading brand configuration: {e}")
        configs['brands'] = {"brands": []}
    
    # Load table schema
    table_schema_path = f"{CONFIG_PATH}/table_schemas.json"
    try:
        configs['schemas'] = json.loads(read_s3_json(table_schema_path))
        logger.info(f"Table schemas loaded from {table_schema_path}")
    except Exception as e:
        logger.error(f"Error loading table schemas: {e}")
        configs['schemas'] = {}
    
    # Load validation rules
    validation_rules_path = f"{CONFIG_PATH}/validation_rules.json"
    try:
        configs['validations'] = json.loads(read_s3_json(validation_rules_path))
        logger.info(f"Validation rules loaded from {validation_rules_path}")
    except Exception as e:
        logger.error(f"Error loading validation rules: {e}")
        configs['validations'] = {}
    
    return configs

def enrich_brand_data(df, brand_configs):
    """Enriches data with complete brand information using Spark"""
    if 'BRAND' not in df.columns:
        logger.warning("BRAND column missing in data")
        return df
    
    # Create a mapping DataFrame
    brand_data = []
    for brand in brand_configs.get('brands', []):
        brand_data.append({
            'file_name': brand.get('file_name', ''),
            'brand_code': brand.get('brand_code', '')
        })
    
    if not brand_data:
        # If no brand data, just return original DataFrame
        return df
    
    # Create brand mapping DataFrame
    brands_df = spark.createDataFrame(brand_data)
    
    # Join with the main DataFrame
    return df.join(
        brands_df,
        df['BRAND'] == brands_df['file_name'],
        'left'
    ).withColumnRenamed('brand_code', 'BRAND_CODE')

def apply_schema_validations(df, table_name, schema_config):
    """Apply schema validations using Spark"""
    if table_name not in schema_config:
        return df
    
    table_schema = schema_config[table_name]
    logger.info(f"Applying schema validations for {table_name}")
    
    # Apply column type conversions
    for column, column_type in table_schema.get('columns', {}).items():
        if column in df.columns:
            logger.info(f"Applying type conversion to {column}: {column_type}")
            
            # Convert based on specified type
            if column_type == 'string':
                df = df.withColumn(column, F.col(column).cast(StringType()))
            elif column_type == 'integer':
                df = df.withColumn(column, F.col(column).cast(IntegerType()))
            elif column_type == 'float':
                df = df.withColumn(column, F.col(column).cast(FloatType()))
            elif column_type == 'boolean':
                df = df.withColumn(column, F.col(column).cast(BooleanType()))
            elif column_type == 'date':
                # We'll just validate dates but not convert them
                # Create a temporary column to validate date format
                df = df.withColumn(
                    f"{column}_valid", 
                    F.to_timestamp(F.col(column)).isNotNull()
                )
    
    return df

def validate_data(df, table_name, validation_rules):
    """Validate data using Spark and return error counts and details"""
    errors = {}
    
    # If no rules for this table or table not in rules, return empty results
    if not validation_rules or table_name not in validation_rules:
        logger.info(f"No validation rules found for table {table_name}")
        return errors, 0
    
    table_rules = validation_rules.get(table_name, {})
    
    # Counter for validation errors
    total_error_count = 0
    
    # Process valid_types rule (allowed values in a column)
    if 'valid_types' in table_rules:
        rule = table_rules['valid_types']
        column = rule.get('column')
        allowed_values = rule.get('allowed_values', [])
        error_type = rule.get('error_type', 'invalid_types')
        
        if column in df.columns:
            # Count invalid values
            invalid_count = df.filter(
                ~F.col(column).isin(allowed_values) & 
                ~F.col(column).isNull()
            ).count()
            
            if invalid_count > 0:
                errors[error_type] = invalid_count
                total_error_count += invalid_count
    
    # Process required_fields rule (non-null values)
    if 'required_fields' in table_rules:
        rule = table_rules['required_fields']
        columns = rule.get('columns', [])
        error_type = rule.get('error_type', 'missing_required_fields')
        
        missing_fields = {}
        for field in columns:
            if field in df.columns:
                # Count missing values
                missing_count = df.filter(
                    F.col(field).isNull() | (F.col(field) == '')
                ).count()
                
                if missing_count > 0:
                    missing_fields[field] = missing_count
                    total_error_count += missing_count
        
        if missing_fields:
            errors[error_type] = missing_fields
    
    # Process uniqueness rule (no duplicates in columns)
    if 'uniqueness' in table_rules:
        rule = table_rules['uniqueness']
        columns = rule.get('columns', [])
        error_type = rule.get('error_type', 'duplicate_records')
        
        if all(col in df.columns for col in columns):
            # Count duplicates
            # First, calculate row counts
            total_rows = df.count()
            distinct_rows = df.select(*columns).distinct().count()
            
            duplicate_count = total_rows - distinct_rows
            if duplicate_count > 0:
                errors[error_type] = duplicate_count
                total_error_count += duplicate_count
    
    # Process date_validation rule
    if 'date_validation' in table_rules:
        rule = table_rules['date_validation']
        columns = rule.get('columns', [])
        error_type = rule.get('error_type', 'invalid_dates')
        
        date_errors = {}
        for column in columns:
            if column in df.columns:
                # Count invalid dates
                try:
                    invalid_count = df.filter(
                        ~F.to_timestamp(F.col(column)).isNotNull() & 
                        ~F.col(column).isNull()
                    ).count()
                    
                    if invalid_count > 0:
                        date_errors[column] = invalid_count
                        total_error_count += invalid_count
                        logger.warning(f"Found {invalid_count} invalid dates in column {column}")
                except Exception as e:
                    logger.error(f"Error validating dates in {column}: {e}")
        
        if date_errors:
            errors[error_type] = date_errors
    
    # Process duration_validation rule for Playback
    if 'duration_validation' in table_rules:
        rule = table_rules['duration_validation']
        error_type = rule.get('error_type', 'invalid_playback_duration')
        
        # For PlayDurationExPause <= AssetDuration OR AssetDuration == 0
        if 'PlayDurationExPause' in df.columns and 'AssetDuration' in df.columns:
            # Convert to numeric and validate
            try:
                # Count invalid durations
                invalid_count = df.filter(
                    (F.col('PlayDurationExPause').cast('float') > 
                     F.col('AssetDuration').cast('float')) & 
                    (F.col('AssetDuration').cast('float') != 0)
                ).count()
                
                if invalid_count > 0:
                    errors[error_type] = invalid_count
                    total_error_count += invalid_count
            except Exception as e:
                logger.error(f"Error validating durations: {e}")
    
    return errors, total_error_count

def apply_column_conversions(df, table_name):
    """Apply special conversions to specific columns based on table"""
    sf_table_name = get_snowflake_table_name(table_name)
    
    if sf_table_name in COLUMN_TYPE_CONVERSIONS:
        conversions = COLUMN_TYPE_CONVERSIONS[sf_table_name]
        
        for column, conversion_type in conversions.items():
            if column in df.columns:
                logger.info(f"Applying special conversion to column {column} in {table_name}")
                
                if conversion_type == 'string':
                    df = df.withColumn(column, F.col(column).cast(StringType()))
                # Add other conversions as needed
    
    return df

def insert_gold_log(spark, process_id, process_date, execution_start, execution_end, date_folder, 
                   table_name, brand, reseller, records_before_agg, records_after_agg, 
                   duplicates_removed, brands_aggregated, checks_total, checks_passed, 
                   checks_failed, checks_skipped, records_valid, records_warning, records_error, 
                   snowflake_rows_inserted, snowflake_rows_updated, snowflake_rows_rejected, 
                   processing_time_ms, memory_usage_mb, status, error_message, 
                   business_checks_details, aggregation_details, loading_details):
    """Insert a log record into Snowflake"""
    try:
        if MOCK_MODE:
            logger.info(f"[MOCK] Would log execution details for {table_name} / {brand} / {date_folder}")
            
            # Create fallback log file in S3
            log_data = {
                "process_id": str(process_id),
                "process_date": process_date,
                "table_name": table_name,
                "brand": brand,
                "status": status,
                "execution_start": execution_start,
                "execution_end": execution_end,
                "date_folder": date_folder,
                "records_before_agg": records_before_agg,
                "records_after_agg": records_after_agg,
                "duplicates_removed": duplicates_removed,
                "brands_aggregated": brands_aggregated,
                "checks_total": checks_total,
                "checks_passed": checks_passed,
                "checks_failed": checks_failed,
                "checks_skipped": checks_skipped,
                "records_valid": records_valid,
                "records_warning": records_warning,
                "records_error": records_error,
                "snowflake_rows_inserted": snowflake_rows_inserted,
                "snowflake_rows_updated": snowflake_rows_updated,
                "snowflake_rows_rejected": snowflake_rows_rejected,
                "processing_time_ms": processing_time_ms,
                "memory_usage_mb": memory_usage_mb,
                "error_message": error_message or "",
                "business_checks_details": business_checks_details,
                "aggregation_details": aggregation_details,
                "loading_details": loading_details
            }
            
            # Convert the log data to a DataFrame and write to S3
            log_df = spark.createDataFrame([log_data])
            log_path = f"{LOGS_PATH}/gold_logs_{table_name}_{brand}_{date_folder}_{process_id}.json"
            log_df.write.mode("overwrite").json(log_path)
            logger.info(f"Log saved to {log_path}")
            
            return True
        else:
            # Create a DataFrame with the log data
            business_checks_json = json.dumps({
                "validation_error_count": sum(1 for _ in business_checks_details.get("validation_errors", {})),
                "total_error_count": business_checks_details.get("error_count", 0)
            })
            
            aggregation_details_json = json.dumps({
                "records_before": aggregation_details.get("records_before", 0),
                "records_after": aggregation_details.get("records_after", 0),
                "duplicates_removed": aggregation_details.get("duplicates_removed", 0)
            })
            
            loading_details_json = json.dumps({
                "rows_inserted": loading_details.get("snowflake_rows_inserted", 0),
                "rows_updated": loading_details.get("snowflake_rows_updated", 0),
                "rows_rejected": loading_details.get("snowflake_rows_rejected", 0)
            })
            
            # Create a log record
            log_data = [{
                "PROCESS_ID": str(process_id),
                "PROCESS_DATE": process_date,
                "EXECUTION_START": execution_start,
                "EXECUTION_END": execution_end,
                "DATE_FOLDER": date_folder,
                "TABLE_NAME": table_name,
                "BRAND": brand,
                "RESELLER": reseller or "",
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
                "ERROR_MESSAGE": error_message or "",
                "BUSINESS_CHECKS_DETAILS": business_checks_json,
                "AGGREGATION_DETAILS": aggregation_details_json,
                "LOADING_DETAILS": loading_details_json
            }]
            
            log_df = spark.createDataFrame(log_data)
            
            # Snowflake connection options for writing the log
            sf_options = {
                "sfUrl": f"{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com",
                "sfUser": SNOWFLAKE_USER,
                "sfPassword": SNOWFLAKE_PASSWORD,
                "sfDatabase": SNOWFLAKE_DATABASE,
                "sfSchema": SNOWFLAKE_MONITORING_SCHEMA,
                "sfWarehouse": SNOWFLAKE_WAREHOUSE,
                "dbtable": "GOLD_LOGS"
            }
            
            # Write the log to Snowflake
            log_df.write \
                .format("snowflake") \
                .options(**sf_options) \
                .mode("append") \
                .save()
            
            logger.info(f"Log inserted in Snowflake for {table_name} {brand} {date_folder}")
            return True
    
    except Exception as e:
        logger.error(f"Error inserting log: {e}")
        logger.error(traceback.format_exc())
        
        # Create fallback log file in S3
        try:
            error_data = [{
                "error": str(e),
                "traceback": traceback.format_exc(),
                "process_id": str(process_id),
                "table_name": table_name,
                "brand": brand,
                "status": status
            }]
            
            error_df = spark.createDataFrame(error_data)
            error_path = f"{LOGS_PATH}/error_log_{table_name}_{brand}_{date_folder}_{process_id}.json"
            error_df.write.mode("overwrite").json(error_path)
            logger.info(f"Created fallback error log: {error_path}")
        except:
            pass
        
        return False

def load_to_snowflake(df, table_name):
    """Load DataFrame to Snowflake using Spark connector"""
    if MOCK_MODE:
        row_count = df.count()
        logger.info(f"[MOCK] Would load {row_count} rows to {table_name}")
        return row_count, 0
    
    # Get the correct Snowflake table name
    sf_table_name = get_snowflake_table_name(table_name)
    logger.info(f"Loading to Snowflake table: {sf_table_name}")
    
    try:
        # Snowflake connection options
        sf_options = {
            "sfUrl": f"{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com",
            "sfUser": SNOWFLAKE_USER,
            "sfPassword": SNOWFLAKE_PASSWORD,
            "sfDatabase": SNOWFLAKE_DATABASE,
            "sfSchema": SNOWFLAKE_SCHEMA,
            "sfWarehouse": SNOWFLAKE_WAREHOUSE,
            "dbtable": sf_table_name
        }
        
        # Get table columns from Snowflake (for column filtering)
        # This requires a temporary connection to get metadata
        try:
            # Create a temporary DataFrame to query Snowflake for metadata
            temp_df = spark.createDataFrame([{"dummy": "value"}])
            
            # Use the Snowflake connector to get table metadata
            metadata_query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = '{SNOWFLAKE_SCHEMA}' 
            AND table_name = '{sf_table_name}'
            """
            
            metadata_options = sf_options.copy()
            metadata_options["query"] = metadata_query
            metadata_options.pop("dbtable", None)
            
            table_columns_df = spark.read \
                .format("snowflake") \
                .options(**metadata_options) \
                .load()
            
            # Convert to list of column names
            table_columns = [row["COLUMN_NAME"] for row in table_columns_df.collect()]
            logger.info(f"Retrieved {len(table_columns)} columns from {SNOWFLAKE_SCHEMA}.{sf_table_name}")
            
        except Exception as e:
            logger.warning(f"Could not retrieve columns for {SNOWFLAKE_SCHEMA}.{sf_table_name}: {e}")
            logger.warning("Will attempt to load all columns")
            table_columns = []
        
        # Filter DataFrame to only include columns that exist in the target table
        if table_columns:
            # Create mapping between DataFrame columns and Snowflake columns (case-insensitive)
            common_columns = []
            for df_col in df.columns:
                for sf_col in table_columns:
                    if df_col.upper() == sf_col.upper():
                        common_columns.append(df_col)
                        break
            
            if not common_columns:
                logger.error(f"No common columns found between DataFrame and Snowflake table {sf_table_name}")
                logger.info(f"DataFrame columns: {df.columns}")
                logger.info(f"Snowflake table columns: {table_columns}")
                return 0, df.count()
            
            # Select only common columns
            filtered_df = df.select(*common_columns)
            logger.info(f"Filtered DataFrame from {len(df.columns)} to {len(filtered_df.columns)} columns")
        else:
            filtered_df = df
        
        # Apply special column conversions
        filtered_df = apply_column_conversions(filtered_df, table_name)
        
        # Special handling for VODCatalogExtended
        if table_name == 'VODCatalogExtended':
            # Handle the timestamp field that's causing errors
            if 'license_start' in filtered_df.columns:
                filtered_df = filtered_df.withColumn(
                    'license_start',
                    F.when(F.col('license_start') == '', None).otherwise(F.col('license_start'))
                )
            
            if 'license_end' in filtered_df.columns:
                filtered_df = filtered_df.withColumn(
                    'license_end',
                    F.when(F.col('license_end') == '', None).otherwise(F.col('license_end'))
                )
                
            if 'series_last_episode_date' in filtered_df.columns:
                filtered_df = filtered_df.withColumn(
                    'series_last_episode_date',
                    F.when(F.col('series_last_episode_date') == '', None).otherwise(F.col('series_last_episode_date'))
                )
        
        # Replace empty strings with nulls for string columns
        string_columns = [f.name for f in filtered_df.schema.fields 
                         if isinstance(f.dataType, StringType) and 
                         f.name not in ['license_start', 'license_end', 'series_last_episode_date']]
        
        for col in string_columns:
            filtered_df = filtered_df.withColumn(
                col,
                F.when(F.col(col) == '', None).otherwise(F.col(col))
            )
        
        # Write to Snowflake
        row_count = filtered_df.count()
        logger.info(f"Writing {row_count} rows to Snowflake table {sf_table_name}")
        
        filtered_df.write \
            .format("snowflake") \
            .options(**sf_options) \
            .mode("append") \
            .save()
        
        logger.info(f"Successfully loaded {row_count} rows to {sf_table_name}")
        return row_count, 0
    
    except Exception as e:
        logger.error(f"Error loading into Snowflake: {e}")
        logger.error(traceback.format_exc())
        return 0, df.count()

def process_gold_file(spark, file_path, configs):
    """Process a single Gold file using Spark"""
    start_time = datetime.now()
    process_id = str(uuid.uuid4())
    
    logger.info(f"Processing file: {file_path}")
    
    try:
        # Extract table name and date
        table_name, date_str = extract_table_date(file_path)
        if not table_name or not date_str:
            logger.error(f"Invalid filename format: {file_path}")
            return False
        
        logger.info(f"Extracted table={table_name}, date={date_str}")
        
        # Read Parquet file using Spark
        df = spark.read.parquet(file_path)
        row_count = df.count()
        logger.info(f"Read {row_count} rows from Parquet file")
        
        # Skip empty files
        if row_count == 0:
            logger.warning(f"File {file_path} is empty, skipping")
            return False
        
        # Add LOADDATE and LOADEDBY columns
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df = df.withColumn("LOADDATE", F.lit(current_timestamp))
        df = df.withColumn("LOADEDBY", F.lit(LOADED_BY))
        
        # Handle data types - convert to strings where appropriate
        # In Spark, we need to use withColumn to modify columns
        for col_name in df.columns:
            col_type = df.schema[col_name].dataType
            # Convert complex types to strings
            if not isinstance(col_type, (StringType, IntegerType, FloatType, BooleanType, TimestampType)):
                df = df.withColumn(col_name, F.col(col_name).cast(StringType()))
        
        # Enrich data with brand information
        df = enrich_brand_data(df, configs.get('brands', {}))
        
        # Process by brand
        # First, get unique brands
        brands = [row.BRAND for row in df.select("BRAND").distinct().collect()]
        
        for brand in brands:
            # Filter data for this brand
            brand_df = df.filter(F.col("BRAND") == brand)
            brand_records = brand_df.count()
            
            # Get brand code if available
            if "BRAND_CODE" in brand_df.columns:
                brand_code_row = brand_df.select("BRAND_CODE").limit(1).collect()
                brand_code = brand_code_row[0].BRAND_CODE if brand_code_row else brand
                
                # Remove temporary columns
                brand_df = brand_df.drop("BRAND_CODE")
            else:
                brand_code = brand
            
            # Determine key columns for deduplication
            key_columns = determine_key_columns(table_name)
            
            # Deduplicate data if key columns are defined
            duplicates_removed = 0
            if key_columns and all(col in brand_df.columns for col in key_columns):
                before_dedup = brand_df.count()
                brand_df = brand_df.dropDuplicates(key_columns)
                after_dedup = brand_df.count()
                duplicates_removed = before_dedup - after_dedup
                logger.info(f"Removed {duplicates_removed} duplicate rows using keys: {key_columns}")
            
            records_after_agg = brand_df.count()
            
            # Apply schema validations if available
            brand_df = apply_schema_validations(brand_df, table_name, configs.get('schemas', {}))
            
            # Perform business validations
            validation_errors, error_count = validate_data(brand_df, table_name, configs.get('validations', {}))
            
            # Calculate validation statistics
            checks_total = sum(1 for _ in configs.get('validations', {}).get(table_name, {}))
            checks_failed = len(validation_errors)
            checks_passed = checks_total - checks_failed
            checks_skipped = 0
            records_valid = records_after_agg - error_count
            records_warning = 0
            records_error = error_count
            
            # Prepare validation details for log
            business_checks_details = {
                "validation_errors": validation_errors,
                "error_count": error_count
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
            
            # Record execution info for logging
            execution_start = start_time.strftime('%Y-%m-%d %H:%M:%S')
            
            # Load to Snowflake
            rows_inserted, rows_rejected = load_to_snowflake(brand_df, table_name)
            
            # Calculate performance metrics
            end_time = datetime.now()
            execution_end = end_time.strftime('%Y-%m-%d %H:%M:%S')
            processing_time_ms = int((end_time - start_time).total_seconds() * 1000)
            
            # Memory usage is harder to track in Spark, so we use a placeholder
            memory_usage_mb = 0
            
            # Prepare loading details for log
            loading_details = {
                "snowflake_rows_inserted": rows_inserted,
                "snowflake_rows_updated": 0,
                "snowflake_rows_rejected": rows_rejected,
                "table_name": get_snowflake_table_name(table_name)
            }
            
            # Insert log
            process_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            error_message = None if status == "OK" else f"{checks_failed} checks failed with {error_count} records in error"
            
            insert_gold_log(
                spark=spark,
                process_id=process_id,
                process_date=process_date,
                execution_start=execution_start,
                execution_end=execution_end,
                date_folder=date_str,
                table_name=table_name,
                brand=brand_code,
                reseller="",
                records_before_agg=brand_records,
                records_after_agg=records_after_agg,
                duplicates_removed=duplicates_removed,
                brands_aggregated=1,
                checks_total=checks_total,
                checks_passed=checks_passed,
                checks_failed=checks_failed,
                checks_skipped=checks_skipped,
                records_valid=records_valid,
                records_warning=records_warning,
                records_error=records_error,
                snowflake_rows_inserted=rows_inserted,
                snowflake_rows_updated=0,
                snowflake_rows_rejected=rows_rejected,
                processing_time_ms=processing_time_ms,
                memory_usage_mb=memory_usage_mb,
                status=status,
                error_message=error_message,
                business_checks_details=business_checks_details,
                aggregation_details=aggregation_details,
                loading_details=loading_details
            )
            
            logger.info(f"Processing completed for {table_name} / {brand_code} / {date_str} with status {status}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        logger.error(traceback.format_exc())
        return False

def process_gold_files(spark):
    """Process all Gold files, optionally filtering by date and/or table"""
    logger.info(f"Starting processing of Gold files" + 
              (f" for date {DATE_FILTER}" if DATE_FILTER else "") +
              (f" for table {TABLE_FILTER}" if TABLE_FILTER else ""))
    
    try:
        # Load configuration files
        configs = load_config_files()
        
        # Get list of files to process
        gold_files = get_s3_file_list(S3_GOLD_PATH, DATE_FILTER, TABLE_FILTER)
        
        if not gold_files:
            logger.warning(f"No Gold files found matching filters")
            return False
        
        logger.info(f"Found {len(gold_files)} files to process")
        for f in gold_files:
            logger.info(f"  - {f}")
        
        # Process each file
        success_count = 0
        for file_path in gold_files:
            if process_gold_file(spark, file_path, configs):
                success_count += 1
        
        logger.info(f"Processing completed: {success_count}/{len(gold_files)} files successful")
        return success_count > 0
        
    except Exception as e:
        logger.error(f"Error in process_gold_files: {e}")
        logger.error(traceback.format_exc())
        return False

# Main execution
def main():
    logger.info("Starting gold_to_snowflake Glue job")
    
    try:
        # Check if Snowflake connection parameters are available
        if not all([SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE]):
            if not MOCK_MODE:
                logger.error("Missing Snowflake connection parameters in job parameters, but MOCK_MODE is set to false.")
                logger.error("Either enable MOCK_MODE or provide all Snowflake connection parameters.")
                return 1
        
        # Display connection mode
        if MOCK_MODE:
            logger.info("MOCK MODE: No actual Snowflake connections will be made")
        else:
            logger.info("LIVE MODE: Will connect to Snowflake")
            logger.info(f"Will connect to Snowflake: {SNOWFLAKE_ACCOUNT} as {SNOWFLAKE_USER}")
            logger.info(f"Database: {SNOWFLAKE_DATABASE}, Schema: {SNOWFLAKE_SCHEMA}")
        
        # Record start time
        start_time = datetime.now()
        logger.info(f"Process started at: {start_time}")
        
        # Process files
        success = process_gold_files(spark)
        
        # Record end time
        end_time = datetime.now()
        duration = end_time - start_time
        
        # Display summary
        logger.info(f"=== EXECUTION SUMMARY ===")
        logger.info(f"Started at   : {start_time}")
        logger.info(f"Finished at  : {end_time}")
        logger.info(f"Total duration: {duration}")
        logger.info(f"Status      : {'SUCCESS' if success else 'FAILURE'}")
        logger.info(f"Mode        : {'MOCK' if MOCK_MODE else 'LIVE'}")
        logger.info(f"=======================")
        
        # Return appropriate exit code
        return 0 if success else 1
        
    except Exception as e:
        logger.error(f"Critical error: {e}")
        logger.error(traceback.format_exc())
        return 1

# Execute the job
exit_code = main()
job.commit()
sys.exit(exit_code)