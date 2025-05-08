#!/usr/bin/env python3
"""
This script processes Parquet files from the gold layer and loads them directly into Snowflake.
It supports data validation, brand mapping, and column type conversions based on configuration files.
"""
import os
import sys
import uuid
import json
import logging
import traceback
import argparse
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ===== CONFIG =====
# Base paths
BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__))).parent
DATA_DIR = BASE_DIR / "data"
GOLD_DIR = DATA_DIR / "gold"
LOGS_DIR = DATA_DIR / "logs"
CONFIG_DIR = BASE_DIR / "config"

# Make sure log directory exists
LOGS_DIR.mkdir(exist_ok=True, parents=True)

# Configure logging
log_file = LOGS_DIR / "gold_to_snowflake_execution.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Snowflake parameters
SF_USER = os.getenv('SNOWFLAKE_USER')
SF_PWD = os.getenv('SNOWFLAKE_PASSWORD')
SF_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SF_WHS = os.getenv('SNOWFLAKE_WAREHOUSE')
SF_DB = os.getenv('SNOWFLAKE_DATABASE')
SF_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'STG_SG')
SF_MONITORING_SCHEMA = os.getenv('SNOWFLAKE_MONITORING_SCHEMA', 'STG_SG_MONITORING')
LOADED_BY = os.getenv('LOADED_BY', 'ETL_GOLD_TO_SNOWFLAKE')

# Flag to run in mock mode (without actual Snowflake connections)
MOCK_MODE = False

# Table name mapping - from file name to Snowflake table name
TABLE_NAME_MAPPING = {
    "VODCatalog": "AGG_VOD_CATALOG",
    "VODCatalogExtended": "AGG_VOD_CATALOG_EXTENDED"
}

# Special column treatment - fields that need special handling during load
COLUMN_TYPE_CONVERSIONS = {
    "AGG_VOD_CATALOG": {
        "series_last_episode_date": lambda x: str(x) if x is not None else None
    }
}

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

def get_snowflake_table_name(table_name):
    """Get the correct Snowflake table name for a given table"""
    # Use mapping if exists, otherwise use standard AGG_TABLENAME format
    return TABLE_NAME_MAPPING.get(table_name, f"AGG_{table_name.upper()}")

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Process files from Gold layer to Snowflake")
    parser.add_argument("--date", type=str, help="Specific date to process (YYYYMMDD format)")
    parser.add_argument("--mock", action="store_true", help="Run in mock mode without connecting to Snowflake")
    parser.add_argument("--table", type=str, help="Process only a specific table")
    return parser.parse_args()

def extract_table_date(filename):
    """Extract table name and date from filename"""
    parts = filename.stem.split('_')
    if len(parts) >= 2 and parts[-1].isdigit() and len(parts[-1]) == 8:
        date = parts[-1]
        table_name = '_'.join(parts[:-1])
        return table_name, date
    return None, None

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

def get_table_columns(conn, schema, table):
    """Get the list of columns for a Snowflake table"""
    cursor = conn.cursor()
    try:
        cursor.execute(f"DESC TABLE {schema}.{table}")
        # Column name is the first element in each row
        columns = [row[0].upper() for row in cursor.fetchall()]
        logger.info(f"Retrieved {len(columns)} columns from {schema}.{table}")
        return columns
    except Exception as e:
        logger.error(f"Error retrieving columns for {schema}.{table}: {e}")
        return []
    finally:
        cursor.close()

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

def apply_schema_validations(df, table_name, schema_config):
    """Apply schema validations from table_schemas.json"""
    # If schema configuration exists for this table
    if table_name in schema_config:
        table_schema = schema_config[table_name]
        logger.info(f"Applying schema validations for {table_name}")
        
        # Apply column type conversions
        for column, column_type in table_schema.get('columns', {}).items():
            if column in df.columns:
                logger.info(f"Applying type conversion to {column}: {column_type}")
                
                # Convert based on specified type
                if column_type == 'string':
                    # Remplacer None/NaN par une chaîne vide pour éviter les problèmes de longueur
                    df[column] = df[column].fillna('')
                    df[column] = df[column].astype(str)
                elif column_type == 'integer':
                    df[column] = pd.to_numeric(df[column], errors='coerce')
                elif column_type == 'float':
                    df[column] = pd.to_numeric(df[column], errors='coerce', downcast='float')
                elif column_type == 'boolean':
                    df[column] = df[column].astype(bool)
                elif column_type == 'date':
                    # Valider les dates avant conversion
                    try:
                        # Convertir en dates avec gestion des erreurs
                        df[column] = pd.to_datetime(df[column], errors='coerce')
                        
                        # Remplacer les dates invalides par NULL
                        mask = pd.isna(df[column]) | (df[column].dt.year < 1900) | (df[column].dt.year > 2100)
                        if mask.any():
                            logger.warning(f"Found {mask.sum()} invalid dates in column {column}, replacing with NULL")
                            df.loc[mask, column] = pd.NaT
                    except Exception as e:
                        logger.error(f"Error converting column {column} to date: {e}")
                        # En cas d'erreur, conserver les valeurs originales
                        pass
    
    return df

def apply_column_conversions(df, table_name):
    """Apply special conversions to specific columns based on table"""
    sf_table_name = get_snowflake_table_name(table_name)
    
    if sf_table_name in COLUMN_TYPE_CONVERSIONS:
        conversions = COLUMN_TYPE_CONVERSIONS[sf_table_name]
        
        for column, conversion_func in conversions.items():
            if column in df.columns:
                logger.info(f"Applying special conversion to column {column} in {table_name}")
                df[column] = df[column].apply(conversion_func)
    
    return df

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
        
        if MOCK_MODE:
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
            import snowflake.connector
            
            ctx = snowflake.connector.connect(
                user=SF_USER,
                password=SF_PWD,
                account=SF_ACCOUNT,
                warehouse=SF_WHS,
                database=SF_DB,
                schema=SF_MONITORING_SCHEMA
            )
            cs = ctx.cursor()
            
            # Utiliser TO_VARIANT pour convertir les chaînes JSON en type VARIANT de Snowflake
            sql = f"""
                INSERT INTO {SF_MONITORING_SCHEMA}.GOLD_LOGS (
                    PROCESS_ID, PROCESS_DATE, EXECUTION_START, EXECUTION_END, 
                    DATE_FOLDER, TABLE_NAME, BRAND, RESELLER, 
                    RECORDS_BEFORE_AGG, RECORDS_AFTER_AGG, DUPLICATES_REMOVED, BRANDS_AGGREGATED, 
                    CHECKS_TOTAL, CHECKS_PASSED, CHECKS_FAILED, CHECKS_SKIPPED, 
                    RECORDS_VALID, RECORDS_WARNING, RECORDS_ERROR, 
                    SNOWFLAKE_ROWS_INSERTED, SNOWFLAKE_ROWS_UPDATED, SNOWFLAKE_ROWS_REJECTED, 
                    PROCESSING_TIME_MS, MEMORY_USAGE_MB, STATUS, ERROR_MESSAGE, 
                    BUSINESS_CHECKS_DETAILS, AGGREGATION_DETAILS, LOADING_DETAILS
                ) 
                SELECT 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    TO_VARIANT(%s), TO_VARIANT(%s), TO_VARIANT(%s)
            """
            
            # Valeurs pour la requête
            values = [
                str(process_id), process_date, execution_start, execution_end,
                date_folder, table_name, brand, reseller if reseller else '',
                records_before_agg, records_after_agg, duplicates_removed, brands_aggregated,
                checks_total, checks_passed, checks_failed, checks_skipped,
                records_valid, records_warning, records_error,
                snowflake_rows_inserted, snowflake_rows_updated, snowflake_rows_rejected,
                processing_time_ms, memory_usage_mb, status, error_message if error_message else "",
                business_checks_json, aggregation_details_json, loading_details_json
            ]
            
            # Exécuter la requête SQL
            cs.execute(sql, values)
            ctx.commit()
            logger.info(f"Log inserted in Snowflake for {table_name} {brand} {date_folder}")
            
            cs.close()
            ctx.close()
            return True
            
    except Exception as e:
        logger.error(f"Error inserting log: {e}")
        logger.error(traceback.format_exc())
        return False
    
def load_to_snowflake_direct(df, table_name):
    """
    Load DataFrame to Snowflake using write_pandas method with column mapping
    """
    if MOCK_MODE:
        logger.info(f"[MOCK] Would load {len(df)} rows to {table_name}")
        return len(df), 0
    
    # Get the correct Snowflake table name
    sf_table_name = get_snowflake_table_name(table_name)
    logger.info(f"Loading to Snowflake table: {sf_table_name}")
    
    try:
        # Import the required module
        try:
            import snowflake.connector
            from snowflake.connector.pandas_tools import write_pandas
            logger.info("Successfully imported Snowflake modules")
        except ImportError as e:
            logger.error(f"Error importing Snowflake modules: {e}")
            logger.error("Make sure you have installed the Snowflake connector with pandas support:")
            logger.error("pip install 'snowflake-connector-python[pandas]'")
            return 0, 0
        
        # Connect to Snowflake
        logger.info(f"Connecting to Snowflake: account={SF_ACCOUNT}, user={SF_USER}, warehouse={SF_WHS}, database={SF_DB}, schema={SF_SCHEMA}")
        conn = snowflake.connector.connect(
            user=SF_USER,
            password=SF_PWD,
            account=SF_ACCOUNT,
            warehouse=SF_WHS,
            database=SF_DB,
            schema=SF_SCHEMA
        )
        
        # Log successful connection
        logger.info("Successfully connected to Snowflake")
        
        # Get the list of columns from the target table
        table_columns = get_table_columns(conn, SF_SCHEMA, sf_table_name)
        
        if not table_columns:
            logger.error(f"Could not retrieve columns for {SF_SCHEMA}.{sf_table_name}")
            return 0, 0
        
        # Filter DataFrame to only keep columns that exist in the target table
        # Case-insensitive match - Snowflake stores column names in uppercase
        common_columns = []
        column_mapping = {}
        
        # Create mapping between DataFrame columns and Snowflake columns
        for df_col in df.columns:
            for sf_col in table_columns:
                if df_col.upper() == sf_col:
                    # Si le nom de colonne est 'Group', il s'agit d'un mot réservé dans Snowflake
                    # Nous devons le traiter différemment
                    if df_col.upper() == 'GROUP':
                        # Pour GROUP, on doit utiliser un nom différent ou des guillemets
                        if '"GROUP"' in table_columns:
                            common_columns.append(df_col)
                            column_mapping[df_col] = '"GROUP"'
                            break
                        else:
                            # Ignorer cette colonne si la version avec guillemets n'est pas trouvée
                            logger.warning(f"Skipping reserved keyword column: {df_col}")
                            continue
                    
                    common_columns.append(df_col)
                    column_mapping[df_col] = sf_col
                    break
        
        if not common_columns:
            logger.error(f"No common columns found between DataFrame and Snowflake table {sf_table_name}")
            logger.info(f"DataFrame columns: {list(df.columns)}")
            logger.info(f"Snowflake table columns: {table_columns}")
            return 0, 0
        
        # Create a filtered DataFrame with only the common columns
        filtered_df = df[common_columns].copy()
        logger.info(f"Filtered DataFrame from {len(df.columns)} to {len(filtered_df.columns)} columns")
        
        # Apply special column conversions if needed
        filtered_df = apply_column_conversions(filtered_df, table_name)
        
        # Remplacer les valeurs None par des chaînes vides pour éviter l'erreur de longueur
        # dans les colonnes de type VARCHAR(2)
        for col in filtered_df.columns:
            if filtered_df[col].dtype == 'object' or pd.api.types.is_string_dtype(filtered_df[col]):
                filtered_df[col] = filtered_df[col].fillna('')
        
        # Rename columns to match Snowflake's uppercase convention
        filtered_df.columns = [column_mapping.get(col, col) for col in filtered_df.columns]
        logger.info("Renamed DataFrame columns to match Snowflake's uppercase convention")
        
        # Use write_pandas to load data directly
        logger.info(f"Using write_pandas to load {len(filtered_df)} rows to {sf_table_name}")
        
        # Execute the write_pandas function
        success, num_chunks, num_rows, output = write_pandas(
            conn=conn,
            df=filtered_df,
            table_name=sf_table_name,
            database=SF_DB,
            schema=SF_SCHEMA,
            auto_create_table=False,
            quote_identifiers=False
        )
        
        # Log the results
        logger.info(f"write_pandas results: success={success}, chunks={num_chunks}, rows={num_rows}")
        
        # If successful, return the number of rows loaded
        if success:
            logger.info(f"Successfully loaded {num_rows} rows to {sf_table_name}")
            return num_rows, 0
        else:
            logger.error(f"Failed to load data to {sf_table_name}: {output}")
            return 0, len(df)
        
    except Exception as e:
        logger.error(f"Error loading into Snowflake: {e}")
        logger.error(traceback.format_exc())
        return 0, 0
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def process_gold_file(file_path, configs):
    """Process a single Gold file"""
    start_time = datetime.now()
    process_id = str(uuid.uuid4())
    
    try:
        import psutil
        memory_start = psutil.Process().memory_info().rss / (1024 * 1024)  # In MB
    except ImportError:
        memory_start = 0
        logger.warning("psutil module not available, memory usage tracking disabled")
    
    logger.info(f"Processing file: {file_path}")
    
    try:
        # Extract table name and date
        table_name, date_str = extract_table_date(file_path)
        if not table_name or not date_str:
            logger.error(f"Invalid filename format: {file_path}")
            return False
        
        logger.info(f"Extracted table={table_name}, date={date_str}")
        
        # Read Parquet file
        df = pd.read_parquet(file_path)
        logger.info(f"Read {len(df)} rows from Parquet file")
        
        # Skip empty files
        if len(df) == 0:
            logger.warning(f"File {file_path} is empty, skipping")
            return False
        
        # Add LOADDATE and LOADEDBY columns
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['LOADDATE'] = current_timestamp
        df['LOADEDBY'] = LOADED_BY
        
        # Handle data types
        for col in df.columns:
            # Convert objects to strings
            if df[col].dtype == 'object':
                # Replace NaN with None
                df[col] = df[col].where(pd.notna(df[col]), None)
                # Convert non-None values to string
                df[col] = df[col].apply(lambda x: str(x) if x is not None else None)
            
            # Handle datetime columns
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                # Vérifier et remplacer les dates invalides
                invalid_dates_mask = (df[col].dt.year < 1900) | (df[col].dt.year > 2100)
                if invalid_dates_mask.any():
                    logger.warning(f"Found {invalid_dates_mask.sum()} invalid dates in column {col}, replacing with NULL")
                    df.loc[invalid_dates_mask, col] = pd.NaT
                
                # Formater les dates valides
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Enrich data with brand information
        df = enrich_brand_data(df, configs.get('brands', {}))
        
        # Process by brand
        brands = df['BRAND'].unique()
        
        for brand in brands:
            # Filter data for this brand
            brand_df = df[df['BRAND'] == brand].copy()
            brand_records = len(brand_df)
            
            # Get brand code
            brand_code = brand_df['BRAND_CODE'].iloc[0] if 'BRAND_CODE' in brand_df.columns else brand
            
            # Remove temporary columns
            if 'BRAND_CODE' in brand_df.columns:
                brand_df = brand_df.drop(columns=['BRAND_CODE'])
            
            # Determine key columns for deduplication
            key_columns = determine_key_columns(table_name)
            
            # Deduplicate data if key columns are defined
            duplicates_removed = 0
            if key_columns and all(col in brand_df.columns for col in key_columns):
                before_dedup = len(brand_df)
                brand_df = brand_df.drop_duplicates(subset=key_columns, keep='first')
                duplicates_removed = before_dedup - len(brand_df)
                logger.info(f"Removed {duplicates_removed} duplicate rows using keys: {key_columns}")
            
            records_after_agg = len(brand_df)
            
            # Apply schema validations if available
            brand_df = apply_schema_validations(brand_df, table_name, configs.get('schemas', {}))
            
            # Perform business validations using rules from JSON
            validation_errors, error_rows = validate_data(brand_df, table_name, configs.get('validations', {}))
            
            # Calculate validation statistics
            checks_total = sum(1 for _ in configs.get('validations', {}).get(table_name, {}))
            checks_failed = len(validation_errors)
            checks_passed = checks_total - checks_failed
            checks_skipped = 0
            records_valid = records_after_agg - len(error_rows)
            records_warning = 0
            
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
            
            # Load to Snowflake
            rows_inserted, rows_rejected = load_to_snowflake_direct(brand_df, table_name)
            
            # Prepare loading details for log
            loading_details = {
                "snowflake_rows_inserted": rows_inserted,
                "snowflake_rows_updated": 0,  # Not applicable with write_pandas
                "snowflake_rows_rejected": rows_rejected,
                "table_name": get_snowflake_table_name(table_name)
            }
            
            # Calculate performance metrics
            end_time = datetime.now()
            execution_end = end_time.strftime('%Y-%m-%d %H:%M:%S')
            processing_time_ms = int((end_time - start_time).total_seconds() * 1000)
            
            try:
                import psutil
                memory_end = psutil.Process().memory_info().rss / (1024 * 1024)  # In MB
                memory_usage_mb = round(memory_end - memory_start, 2)
            except:
                memory_usage_mb = 0
            
            # Insert log
            process_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            error_message = None if status == "OK" else f"{checks_failed} checks failed with {len(error_rows)} records in error"
            
            insert_gold_log(
                process_id=process_id,
                process_date=process_date,
                execution_start=execution_start,
                execution_end=execution_end,
                date_folder=date_str,
                table_name=table_name,
                brand=brand_code,
                reseller="",  # Empty field as not used anymore
                records_before_agg=brand_records,
                records_after_agg=records_after_agg,
                duplicates_removed=duplicates_removed,
                brands_aggregated=1,  # Only one brand per iteration
                checks_total=checks_total,
                checks_passed=checks_passed,
                checks_failed=checks_failed,
                checks_skipped=checks_skipped,
                records_valid=records_valid,
                records_warning=records_warning,
                records_error=len(error_rows),
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

def process_gold_files(date_str=None, specific_table=None):
    """Process all Gold files, optionally filtering by date and/or table"""
    logger.info(f"Starting processing of Gold files" + 
               (f" for date {date_str}" if date_str else "") +
               (f" for table {specific_table}" if specific_table else ""))
    
    try:
        # Load configuration files
        configs = load_config_files()
        
        # Define file pattern
        pattern = ""
        if specific_table and date_str:
            pattern = f"{specific_table}_{date_str}.parquet"
        elif specific_table:
            pattern = f"{specific_table}_*.parquet"
        elif date_str:
            pattern = f"*_{date_str}.parquet"
        else:
            pattern = "*.parquet"
            
        # Find files
        gold_files = list(GOLD_DIR.glob(pattern))
        
        if not gold_files:
            logger.warning(f"No Gold files found matching pattern: {pattern}")
            return False
        
        logger.info(f"Found {len(gold_files)} files to process")
        for f in gold_files:
            logger.info(f"  - {f.name}")
        
        # Process each file
        success_count = 0
        for file_path in gold_files:
            if process_gold_file(file_path, configs):
                success_count += 1
        
        logger.info(f"Processing completed: {success_count}/{len(gold_files)} files successful")
        return success_count > 0
        
    except Exception as e:
        logger.error(f"Error in process_gold_files: {e}")
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    print("Starting gold_to_snowflake script")
    
    try:
        # Parse arguments
        args = parse_args()
        
        # Set mock mode if requested
        if args.mock:
            MOCK_MODE = True
            logger.info("Running in MOCK mode (no Snowflake connection)")
        
        # Record start time
        start_time = datetime.now()
        logger.info(f"Process started at: {start_time}")
        
        # Process files with specific table if provided
        success = process_gold_files(args.date, args.table)
        
        # Record end time
        end_time = datetime.now()
        duration = end_time - start_time
        
        # Display summary
        print(f"\n=== EXECUTION SUMMARY ===")
        print(f"Started at   : {start_time}")
        print(f"Finished at  : {end_time}")
        print(f"Total duration: {duration}")
        print(f"Status      : {'SUCCESS' if success else 'FAILURE'}")
        print(f"Mock mode   : {MOCK_MODE}")
        print(f"Log file    : {log_file}")
        print(f"=======================\n")
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    except Exception as e:
        logger.error(f"Critical error: {e}")
        logger.error(traceback.format_exc())
        print(f"\n[CRITICAL ERROR] {str(e)}")
        print(f"Check log file: {log_file}")
        sys.exit(1)