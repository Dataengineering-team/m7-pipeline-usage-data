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
import uuid
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

# MOCK_SNOWFLAKE - initialisation par défaut (sera mis à jour par args)
MOCK_SNOWFLAKE = os.getenv('MOCK_SNOWFLAKE', 'false').lower() == 'true'

# Snowflake parameters
SF_USER = os.getenv('SNOWFLAKE_USER')
SF_PWD = os.getenv('SNOWFLAKE_PASSWORD')
SF_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SF_WHS = os.getenv('SNOWFLAKE_WAREHOUSE')
SF_DB = os.getenv('SNOWFLAKE_DATABASE')
SF_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'STG_SG')
SF_MONITORING_SCHEMA = os.getenv('SNOWFLAKE_MONITORING_SCHEMA', 'STG_SG_MONITORING')

# Alert parameters
ENABLE_EMAIL_ALERTS = os.getenv('ENABLE_EMAIL_ALERTS', 'false').lower() == 'true'
SMTP_SERVER = os.getenv('SMTP_SERVER')
SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
SMTP_USER = os.getenv('SMTP_USER')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD')
ADMIN_EMAIL = os.getenv('ADMIN_EMAIL')
ERROR_THRESHOLD = float(os.getenv('ERROR_THRESHOLD', '5.0'))  # 5% default
WARNING_THRESHOLD = float(os.getenv('WARNING_THRESHOLD', '10.0'))  # 10% default

# Pipeline version
PIPELINE_VERSION = "1.0.0"

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

# Custom JSON Encoder for serialization
class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle non-serializable types"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Path):
            return str(obj)
        if isinstance(obj, uuid.UUID):
            return str(obj)
        try:
            return json.JSONEncoder.default(self, obj)
        except TypeError:
            return str(obj)

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
            # Initialize empty file if it doesn't exist or is invalid
            with open(tracking_file, 'w') as f:
                json.dump({}, f)
    else:
        # Create empty tracking file if it doesn't exist
        with open(tracking_file, 'w') as f:
            json.dump({}, f)
    
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

def send_alert(message, severity="WARNING", email=None):
    """
    Sends an alert in case of critical problems
    
    Args:
        message: Alert message
        severity: Severity level (INFO, WARNING, ERROR, CRITICAL)
        email: Email address to notify (optional)
    """
    logger.warning(f"ALERT [{severity}]: {message}")
    
    # Log to alert file
    alert_file = LOGS_DIR / "pipeline_alerts.log"
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        with open(alert_file, 'a', encoding='utf-8') as f:
            f.write(f"{timestamp} - {severity} - {message}\n")
    except Exception as e:
        logger.error(f"Error writing alert to file: {e}")
    
    # Record data quality alert in Snowflake if not in mock mode
    if severity in ["WARNING", "ERROR", "CRITICAL"] and not MOCK_SNOWFLAKE:
        try:
            import snowflake.connector
            
            # Connect to Snowflake
            ctx = snowflake.connector.connect(
                user=SF_USER,
                password=SF_PWD,
                account=SF_ACCOUNT,
                warehouse=SF_WHS,
                database=SF_DB,
                schema=SF_MONITORING_SCHEMA
            )
            cs = ctx.cursor()
            
            # Get current date in YYYYMMDD format
            current_date = datetime.now().strftime('%Y%m%d')
            
            # Insert alert into DATA_QUALITY_ALERTS table
            sql = f"""
                INSERT INTO {SF_MONITORING_SCHEMA}.DATA_QUALITY_ALERTS 
                    (DATE_FOLDER, ALERT_TYPE, MESSAGE, DETAILS)
                VALUES (%s, %s, %s, %s)
            """
            
            # Format details as JSON
            details = {
                "timestamp": timestamp,
                "message": message,
                "severity": severity
            }
            details_json = json.dumps(details, cls=CustomJSONEncoder)
            
            params = (
                current_date,  # DATE_FOLDER
                severity,      # ALERT_TYPE
                message,       # MESSAGE
                details_json   # DETAILS
            )
            
            cs.execute(sql, params)
            ctx.commit()
            
            cs.close()
            ctx.close()
            
            logger.info(f"Alert recorded in Snowflake DATA_QUALITY_ALERTS")
            
        except Exception as e:
            logger.error(f"Error recording alert in Snowflake: {e}")
            logger.error(traceback.format_exc())
    
    # Send email if configured
    if email and ENABLE_EMAIL_ALERTS:
        try:
            import smtplib
            from email.message import EmailMessage
            
            if not all([SMTP_SERVER, SMTP_USER, SMTP_PASSWORD]):
                logger.error("SMTP configuration incomplete, email alert not sent")
                return
            
            msg = EmailMessage()
            msg['Subject'] = f"[{severity}] ETL Pipeline Alert"
            msg['From'] = SMTP_USER
            msg['To'] = email
            msg.set_content(f"{message}\n\nTimestamp: {timestamp}")
            
            server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
            server.quit()
            
            logger.info(f"Email alert sent to {email}")
        except Exception as e:
            logger.error(f"Error sending email alert: {e}")
            logger.error(traceback.format_exc())

def log_pipeline_execution(process_id, date_folder, execution_start, execution_end, 
                          status, tables_processed, tables_success, tables_warning, 
                          tables_error, error_message, processing_details):
    """
    Records pipeline execution in Snowflake monitoring table
    """
    try:
        # In mock mode, just log locally
        if MOCK_SNOWFLAKE:
            log_entry = {
                "PROCESS_ID": str(process_id),
                "PROCESS_DATE": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "PIPELINE_VERSION": PIPELINE_VERSION,
                "DATE_FOLDER": date_folder,
                "EXECUTION_START": execution_start.strftime('%Y-%m-%d %H:%M:%S'),
                "EXECUTION_END": execution_end.strftime('%Y-%m-%d %H:%M:%S'),
                "DURATION_SECONDS": (execution_end - execution_start).total_seconds(),
                "STATUS": status,
                "TABLES_PROCESSED": tables_processed,
                "TABLES_SUCCESS": tables_success,
                "TABLES_WARNING": tables_warning,
                "TABLES_ERROR": tables_error,
                "ERROR_MESSAGE": error_message,
                "PROCESSING_DETAILS": processing_details
            }
            
            # Save to local file
            filename = LOGS_DIR / f"pipeline_execution_{date_folder}_{process_id}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(log_entry, f, indent=2, cls=CustomJSONEncoder)
                
            logger.info(f"Pipeline execution log saved to {filename}")
            return True
            
        # Use Snowflake connector
        try:
            import snowflake.connector
        except ImportError:
            logger.error("Snowflake connector not installed, local log only")
            # Create a local log as fallback
            filename = LOGS_DIR / f"pipeline_execution_{date_folder}_{process_id}_fallback.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump({
                    "process_id": str(process_id),
                    "error": "Snowflake connector not installed",
                    "date_folder": date_folder,
                    "status": status
                }, f, indent=2)
            return False
            
        # Connect to Snowflake
        ctx = snowflake.connector.connect(
            user=SF_USER,
            password=SF_PWD,
            account=SF_ACCOUNT,
            warehouse=SF_WHS,
            database=SF_DB,
            schema=SF_MONITORING_SCHEMA
        )
        cs = ctx.cursor()
        
        # Save detailed processing information to a local file
        details_file = LOGS_DIR / f"pipeline_details_{date_folder}_{process_id}.json"
        with open(details_file, 'w', encoding='utf-8') as f:
            json.dump(processing_details, f, indent=2, cls=CustomJSONEncoder)
        
        # Format timestamps for SQL
        execution_start_str = execution_start.strftime("%Y-%m-%d %H:%M:%S")
        execution_end_str = execution_end.strftime("%Y-%m-%d %H:%M:%S")
        
        # Duration in seconds
        duration_seconds = int((execution_end - execution_start).total_seconds())
        
        # Extract steps completed for the STEPS_COMPLETED array column
        steps_completed = processing_details.get('steps_completed', [])
        steps_json = json.dumps(steps_completed)
        
        # Convert processing details to JSON string (for VARCHAR column)
        details_json = json.dumps(processing_details, cls=CustomJSONEncoder)
        error_msg = "" if not error_message else error_message
        
        # Calculate additional metrics if available
        total_records = processing_details.get('total_records', 0)
        valid_records = processing_details.get('valid_records', 0)
        error_records = processing_details.get('error_records', 0)
        warning_records = processing_details.get('warning_records', 0)
        
        # Création de variables temporaires pour le JSON de steps_completed
        # Le JSON doit être correctement échappé pour SQL
        setup_steps_sql = "SET steps_json_var = '" + steps_json.replace("'", "''") + "';"
        cs.execute(setup_steps_sql)
        
        # Insertion en utilisant TO_ARRAY() pour la colonne ARRAY et le VARCHAR direct pour PROCESSING_DETAILS
        sql = f"""
            INSERT INTO {SF_MONITORING_SCHEMA}.PIPELINE_LOGS
                (PROCESS_ID, PROCESS_DATE, PIPELINE_VERSION, DATE_FOLDER, 
                 EXECUTION_START, EXECUTION_END, DURATION_SECONDS, STATUS,
                 TABLES_PROCESSED, TABLES_SUCCESS, TABLES_WARNING, TABLES_ERROR,
                 TOTAL_RECORDS, VALID_RECORDS, ERROR_RECORDS, WARNING_RECORDS,
                 STEPS_COMPLETED, ERROR_MESSAGE, PROCESSING_DETAILS)
            VALUES (
                %s, CURRENT_TIMESTAMP(), %s, %s,
                TO_TIMESTAMP_NTZ(%s), TO_TIMESTAMP_NTZ(%s), %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                PARSE_JSON($steps_json_var), %s, %s
            )
        """
        
        # Paramètres
        params = (
            process_id,
            PIPELINE_VERSION,
            date_folder,
            execution_start_str,
            execution_end_str,
            duration_seconds,
            status,
            tables_processed,
            tables_success,
            tables_warning,
            tables_error,
            total_records,
            valid_records, 
            error_records,
            warning_records,
            error_msg,
            details_json   # PROCESSING_DETAILS comme un VARCHAR normal
        )
        
        cs.execute(sql, params)
        ctx.commit()
        
        logger.info(f"Pipeline execution log inserted into Snowflake for date {date_folder}")
        logger.info(f"Detailed pipeline info saved to {details_file}")
        
        cs.close()
        ctx.close()
        return True
    except Exception as e:
        logger.error(f"Error logging pipeline execution: {e}")
        logger.error(traceback.format_exc())
        return False

def log_pipeline_execution(process_id, date_folder, execution_start, execution_end, 
                          status, tables_processed, tables_success, tables_warning, 
                          tables_error, error_message, processing_details):
    """
    Records pipeline execution in Snowflake monitoring table
    """
    try:
        # In mock mode, just log locally
        if MOCK_SNOWFLAKE:
            log_entry = {
                "PROCESS_ID": str(process_id),
                "PROCESS_DATE": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "PIPELINE_VERSION": PIPELINE_VERSION,
                "DATE_FOLDER": date_folder,
                "EXECUTION_START": execution_start.strftime('%Y-%m-%d %H:%M:%S'),
                "EXECUTION_END": execution_end.strftime('%Y-%m-%d %H:%M:%S'),
                "DURATION_SECONDS": (execution_end - execution_start).total_seconds(),
                "STATUS": status,
                "TABLES_PROCESSED": tables_processed,
                "TABLES_SUCCESS": tables_success,
                "TABLES_WARNING": tables_warning,
                "TABLES_ERROR": tables_error,
                "ERROR_MESSAGE": error_message,
                "PROCESSING_DETAILS": processing_details
            }
            
            # Save to local file
            filename = LOGS_DIR / f"pipeline_execution_{date_folder}_{process_id}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(log_entry, f, indent=2, cls=CustomJSONEncoder)
                
            logger.info(f"Pipeline execution log saved to {filename}")
            return True
            
        # Use Snowflake connector
        try:
            import snowflake.connector
        except ImportError:
            logger.error("Snowflake connector not installed, local log only")
            # Create a local log as fallback
            filename = LOGS_DIR / f"pipeline_execution_{date_folder}_{process_id}_fallback.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump({
                    "process_id": str(process_id),
                    "error": "Snowflake connector not installed",
                    "date_folder": date_folder,
                    "status": status
                }, f, indent=2)
            return False
            
        # Connect to Snowflake
        ctx = snowflake.connector.connect(
            user=SF_USER,
            password=SF_PWD,
            account=SF_ACCOUNT,
            warehouse=SF_WHS,
            database=SF_DB,
            schema=SF_MONITORING_SCHEMA
        )
        cs = ctx.cursor()
        
        # Save detailed processing information to a local file
        details_file = LOGS_DIR / f"pipeline_details_{date_folder}_{process_id}.json"
        with open(details_file, 'w', encoding='utf-8') as f:
            json.dump(processing_details, f, indent=2, cls=CustomJSONEncoder)
        
        # Format timestamps for SQL
        execution_start_str = execution_start.strftime("%Y-%m-%d %H:%M:%S")
        execution_end_str = execution_end.strftime("%Y-%m-%d %H:%M:%S")
        
        # Duration in seconds
        duration_seconds = int((execution_end - execution_start).total_seconds())
        
        # Extract steps completed for the STEPS_COMPLETED array column
        steps_completed = processing_details.get('steps_completed', [])
        steps_json = json.dumps(steps_completed)
        
        # Convert processing details to JSON string (for VARCHAR column)
        details_json = json.dumps(processing_details, cls=CustomJSONEncoder)
        error_msg = "" if not error_message else error_message
        
        # Calculate additional metrics if available
        total_records = processing_details.get('total_records', 0)
        valid_records = processing_details.get('valid_records', 0)
        error_records = processing_details.get('error_records', 0)
        warning_records = processing_details.get('warning_records', 0)
        
        # APPROCHE PAR ÉTAPES MULTIPLES:
        # 1. Insérer d'abord les données de base SANS les champs complexes
        sql_basic = f"""
            INSERT INTO {SF_MONITORING_SCHEMA}.PIPELINE_LOGS
                (PROCESS_ID, PROCESS_DATE, PIPELINE_VERSION, DATE_FOLDER, 
                 EXECUTION_START, EXECUTION_END, DURATION_SECONDS, STATUS,
                 TABLES_PROCESSED, TABLES_SUCCESS, TABLES_WARNING, TABLES_ERROR,
                 TOTAL_RECORDS, VALID_RECORDS, ERROR_RECORDS, WARNING_RECORDS,
                 ERROR_MESSAGE)
            VALUES (
                %s, CURRENT_TIMESTAMP(), %s, %s,
                TO_TIMESTAMP_NTZ(%s), TO_TIMESTAMP_NTZ(%s), %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s
            )
        """
        
        # Paramètres pour l'insertion de base
        params_basic = (
            process_id,
            PIPELINE_VERSION,
            date_folder,
            execution_start_str,
            execution_end_str,
            duration_seconds,
            status,
            tables_processed,
            tables_success,
            tables_warning,
            tables_error,
            total_records,
            valid_records, 
            error_records,
            warning_records,
            error_msg
        )
        
        # Exécuter l'insertion de base
        cs.execute(sql_basic, params_basic)
        
        # 2. Mettre à jour le champ STEPS_COMPLETED en utilisant une commande SQL
        # qui fonctionne avec le format ARRAY
        steps_sql = f"""
            UPDATE {SF_MONITORING_SCHEMA}.PIPELINE_LOGS
            SET STEPS_COMPLETED = ARRAY_CONSTRUCT({', '.join([f"'{step}'" for step in steps_completed])})
            WHERE PROCESS_ID = '{process_id}'
        """
        cs.execute(steps_sql)
        
        # 3. Mettre à jour le champ PROCESSING_DETAILS (VARCHAR) séparément
        # Utiliser un marqueur de paramètre pour éviter des problèmes d'échappement
        details_sql = f"""
            UPDATE {SF_MONITORING_SCHEMA}.PIPELINE_LOGS
            SET PROCESSING_DETAILS = %s
            WHERE PROCESS_ID = %s
        """
        cs.execute(details_sql, (details_json, process_id))
        
        # Valider les modifications
        ctx.commit()
        
        logger.info(f"Pipeline execution log inserted into Snowflake for date {date_folder}")
        logger.info(f"Detailed pipeline info saved to {details_file}")
        
        cs.close()
        ctx.close()
        return True
    except Exception as e:
        logger.error(f"Error logging pipeline execution: {e}")
        logger.error(traceback.format_exc())
        return False

def check_data_quality(date_str):
    """
    Checks data quality metrics for critical issues
    
    Args:
        date_str: Date in YYYYMMDD format
        
    Returns:
        tuple: (status, message, error_percentage)
    """
    try:
        # If in mock mode, don't check data quality
        if MOCK_SNOWFLAKE:
            logger.info("Mock mode: skipping data quality check")
            return "OK", "Data quality check skipped in mock mode", 0
        
        # Query Snowflake for quality metrics
        try:
            import snowflake.connector
        except ImportError:
            logger.error("Snowflake connector not installed, skipping data quality check")
            return "OK", "Data quality check skipped (no Snowflake connector)", 0
        
        # Connect to Snowflake
        ctx = snowflake.connector.connect(
            user=SF_USER,
            password=SF_PWD,
            account=SF_ACCOUNT,
            warehouse=SF_WHS,
            database=SF_DB,
            schema=SF_MONITORING_SCHEMA
        )
        cs = ctx.cursor()
        
        # Query for quality metrics from GOLD_LOGS - utilise des paramètres plutôt que des chaînes formatées
        sql = f"""
            SELECT
                TABLE_NAME,
                SUM(RECORDS_BEFORE_AGG) AS TOTAL_RECORDS,
                SUM(RECORDS_ERROR) AS ERROR_RECORDS,
                ROUND(100.0 * SUM(RECORDS_ERROR) / NULLIF(SUM(RECORDS_BEFORE_AGG), 0), 2) AS ERROR_PERCENT
            FROM
                {SF_MONITORING_SCHEMA}.GOLD_LOGS
            WHERE
                DATE_FOLDER = %s
            GROUP BY
                TABLE_NAME
        """
        
        cs.execute(sql, (date_str,))
        results = cs.fetchall()
        
        # If no results, data quality check can't be performed
        if not results:
            cs.close()
            ctx.close()
            logger.warning(f"No data quality metrics found for date {date_str}")
            return "OK", "No data quality metrics available", 0
        

        # Process the results
        total_records = 0
        total_errors = 0
        table_metrics = []
        
        for row in results:
            table_name, records, errors, error_pct = row
            total_records += records or 0
            total_errors += errors or 0
            
            # Store table-level metrics
            table_metrics.append({
                "table": table_name,
                "records": records,
                "errors": errors,
                "error_pct": error_pct
            })
        
        # Calculate overall error percentage
        error_pct = (total_errors / total_records * 100) if total_records > 0 else 0
        
        # Determine status based on thresholds
        if error_pct > ERROR_THRESHOLD:
            status = "ERROR"
            message = (
                f"DATA QUALITY ERROR: Overall error rate is {error_pct:.2f}%, "
                f"exceeding threshold of {ERROR_THRESHOLD}%"
            )
        elif error_pct > WARNING_THRESHOLD:
            status = "WARNING"
            message = (
                f"DATA QUALITY WARNING: Overall error rate is {error_pct:.2f}%, "
                f"exceeding threshold of {WARNING_THRESHOLD}%"
            )
        else:
            status = "OK"
            message = f"Data quality acceptable: Error rate {error_pct:.2f}%"
        
        # Close Snowflake connection
        cs.close()
        ctx.close()
        
        # Log details
        logger.info(f"Data quality for {date_str}: {message}")
        for tm in table_metrics:
            logger.info(f"  {tm['table']}: {tm['error_pct']}% errors ({tm['errors']}/{tm['records']})")
        
        return status, message, error_pct
        
    except Exception as e:
        logger.error(f"Error checking data quality: {e}")
        logger.error(traceback.format_exc())
        return "WARNING", f"Error during data quality check: {str(e)}", 0

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
    
    # Add mock flag if in mock mode
    if MOCK_SNOWFLAKE:
        cmd.append("--mock")
    
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
    # Generate a unique process ID for this pipeline run
    process_id = str(uuid.uuid4())
    
    # Record the start time
    execution_start = datetime.now()
    
    # Initialize metrics
    tables_success = 0
    tables_warning = 0
    tables_error = 0
    steps_completed = []
    tables_status = {}
    data_quality_results = {}
    
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
        raw_to_silver_success, raw_output = run_pipeline_step('raw_to_silver.py', date_str)
        
        if raw_to_silver_success:
            steps_completed.append("raw_to_silver")
            logger.info("RAW -> SILVER step completed successfully")
        else:
            logger.error("Failed at RAW -> SILVER step")
            mark_date_as_processed(date_str, status='failed')
            
            # Log pipeline execution
            execution_end = datetime.now()
            log_pipeline_execution(
                process_id=process_id,
                date_folder=date_str,
                execution_start=execution_start,
                execution_end=execution_end,
                status="ERROR",
                tables_processed=len(EXPECTED_TABLES),
                tables_success=0,
                tables_warning=0,
                tables_error=len(EXPECTED_TABLES),
                error_message="Failed at RAW -> SILVER step",
                processing_details={
                    "steps_completed": steps_completed,
                    "tables_status": tables_status,
                    "raw_output": raw_output
                }
            )
            
            # Send alert
            send_alert(
                f"Pipeline ERROR: Failed at RAW -> SILVER step for date {date_str}",
                "ERROR",
                ADMIN_EMAIL
            )
            
            return False
        
        # Step 2: silver_to_gold.py
        logger.info("Step 2: Processing SILVER -> GOLD")
        silver_to_gold_success, silver_output = run_pipeline_step('silver_to_gold.py', date_str)
        
        if silver_to_gold_success:
            steps_completed.append("silver_to_gold")
            logger.info("SILVER -> GOLD step completed successfully")
        else:
            logger.error("Failed at SILVER -> GOLD step")
            mark_date_as_processed(date_str, status='failed')
            
            # Log pipeline execution
            execution_end = datetime.now()
            log_pipeline_execution(
                process_id=process_id,
                date_folder=date_str,
                execution_start=execution_start,
                execution_end=execution_end,
                status="ERROR",
                tables_processed=len(EXPECTED_TABLES),
                tables_success=0,
                tables_warning=0,
                tables_error=len(EXPECTED_TABLES),
                error_message="Failed at SILVER -> GOLD step",
                processing_details={
                    "steps_completed": steps_completed,
                    "tables_status": tables_status,
                    "silver_output": silver_output
                }
            )
            
            # Send alert
            send_alert(
                f"Pipeline ERROR: Failed at SILVER -> GOLD step for date {date_str}",
                "ERROR",
                ADMIN_EMAIL
            )
            
            return False
        
        # Step 3: gold_to_snowflake.py
        logger.info("Step 3: Processing GOLD -> SNOWFLAKE")
        gold_to_snowflake_success, gold_output = run_pipeline_step('gold_to_snowflake.py', date_str)
        
        if gold_to_snowflake_success:
            steps_completed.append("gold_to_snowflake")
            logger.info("GOLD -> SNOWFLAKE step completed successfully")
        else:
            logger.error("Failed at GOLD -> SNOWFLAKE step")
            mark_date_as_processed(date_str, status='failed')
            
            # Log pipeline execution
            execution_end = datetime.now()
            log_pipeline_execution(
                process_id=process_id,
                date_folder=date_str,
                execution_start=execution_start,
                execution_end=execution_end,
                status="ERROR",
                tables_processed=len(EXPECTED_TABLES),
                tables_success=0,
                tables_warning=0,
                tables_error=len(EXPECTED_TABLES),
                error_message="Failed at GOLD -> SNOWFLAKE step",
                processing_details={
                    "steps_completed": steps_completed,
                    "tables_status": tables_status,
                    "gold_output": gold_output
                }
            )
            
            # Send alert
            send_alert(
                f"Pipeline ERROR: Failed at GOLD -> SNOWFLAKE step for date {date_str}",
                "ERROR",
                ADMIN_EMAIL
            )
            
            return False
        
        # If all steps completed successfully, check data quality
        logger.info("Checking data quality")
        quality_status, quality_message, error_percentage = check_data_quality(date_str)
        data_quality_results = {
            "status": quality_status,
            "message": quality_message,
            "error_percentage": error_percentage
        }
        
        # Set status based on data quality
        pipeline_status = "SUCCESS"
        error_message = None
        
        if quality_status == "ERROR":
            pipeline_status = "WARNING"
            error_message = quality_message
            tables_warning = len(EXPECTED_TABLES)
            
            # Send alert
            send_alert(
                f"Data Quality WARNING: {quality_message} for date {date_str}",
                "WARNING",
                ADMIN_EMAIL
            )
        elif quality_status == "WARNING":
            pipeline_status = "SUCCESS_WITH_WARNINGS"
            error_message = quality_message
            tables_warning = len(EXPECTED_TABLES) // 2  # Approximate
            tables_success = len(EXPECTED_TABLES) - tables_warning
        else:
            tables_success = len(EXPECTED_TABLES)
        
        # If everything went well, mark the date as processed
        logger.info(f"====== Processing successful for date: {date_str} ======")
        mark_date_as_processed(date_str, status='success')
        
        # Clean up silver files if configured
        if DELETE_SILVER_FILES_AFTER_LOAD:
            clean_silver_files_after_processing(date_str)
        
        # Log pipeline execution
        execution_end = datetime.now()
        log_pipeline_execution(
            process_id=process_id,
            date_folder=date_str,
            execution_start=execution_start,
            execution_end=execution_end,
            status=pipeline_status,
            tables_processed=len(EXPECTED_TABLES),
            tables_success=tables_success,
            tables_warning=tables_warning,
            tables_error=tables_error,
            error_message=error_message,
            processing_details={
                "steps_completed": steps_completed,
                "tables_status": tables_status,
                "data_quality": data_quality_results
            }
        )
        
        return True
        
    except Exception as e:
        logger.error(f"Error during pipeline execution: {e}")
        logger.error(traceback.format_exc())
        
        if date_str:
            mark_date_as_processed(date_str, status='failed')
        
        # Log pipeline execution
        execution_end = datetime.now()
        log_pipeline_execution(
            process_id=process_id,
            date_folder=date_str if date_str else "ALL",
            execution_start=execution_start,
            execution_end=execution_end,
            status="ERROR",
            tables_processed=len(EXPECTED_TABLES),
            tables_success=0,
            tables_warning=0,
            tables_error=len(EXPECTED_TABLES),
            error_message=f"Critical error: {str(e)}",
            processing_details={
                "steps_completed": steps_completed,
                "tables_status": tables_status,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
        )
        
        # Send alert
        send_alert(
            f"CRITICAL Pipeline Error: {str(e)}",
            "CRITICAL",
            ADMIN_EMAIL
        )
            
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
    parser.add_argument("--mock", action="store_true",
                       help="Run in mock mode without connecting to Snowflake")
    parser.add_argument("--no-mock", action="store_true",
                       help="Force live mode (overrides .env setting)")
    return parser.parse_args()

if __name__ == "__main__":
    try:
        # Display configuration information
        logger.info("====== STARTING PIPELINE ======")
        logger.info(f"Base directory: {BASE_DIR}")
        logger.info(f"Data directory: {DATA_DIR}")
        logger.info(f"Expected tables: {EXPECTED_TABLES}")
        logger.info(f"Pipeline version: {PIPELINE_VERSION}")
        
        # Parse arguments
        args = parse_args()
        
        # Déterminer le mode MOCK/LIVE avec une priorité claire
        # 1. Option --no-mock (priorité la plus élevée) - force LIVE mode
        if args.no_mock:
            MOCK_SNOWFLAKE = False
            logger.info("Running in LIVE mode (--no-mock flag takes priority)")
        # 2. Option --mock (seconde priorité) - force MOCK mode
        elif args.mock:
            MOCK_SNOWFLAKE = True
            logger.info("Running in MOCK mode (--mock flag)")
        # 3. Variable d'environnement (priorité la plus basse)
        else:
            env_mock = os.getenv('MOCK_SNOWFLAKE', 'false').lower() == 'true'
            MOCK_SNOWFLAKE = env_mock
            logger.info(f"Running in {'MOCK' if MOCK_SNOWFLAKE else 'LIVE'} mode (from environment)")
        
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
        logger.info(f"Mode        : {'MOCK' if MOCK_SNOWFLAKE else 'LIVE'}")
        logger.info("====== PIPELINE COMPLETED ======\n")
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    except Exception as e:
        logger.error(f"CRITICAL ERROR: {e}")
        logger.error(traceback.format_exc())
        
        # Send critical alert
        send_alert(
            f"CRITICAL Pipeline Error: {str(e)}",
            "CRITICAL",
            ADMIN_EMAIL
        )
        
        sys.exit(1)