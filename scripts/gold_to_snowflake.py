#!/usr/bin/env python3
import os
import io
import csv
import sys
import uuid
import json
import re
import logging
import traceback
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector
import psutil
from concurrent.futures import ThreadPoolExecutor, as_completed

# Ajouter le dossier parent au chemin pour pouvoir importer les utilitaires
sys.path.append(str(Path(__file__).parent))
from utils import file_utils, validation_utils

# Chargement des variables d'environnement du fichier .env
load_dotenv()

# ===== CONFIG =====
# Chemins de base
BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__))).parent

# Gestion des chemins relatifs vs absolus depuis .env
data_dir_env = os.getenv('DATA_DIR')
if data_dir_env:
    # Si le chemin commence par ./ ou est relatif sans slash, le considérer comme relatif à BASE_DIR
    if data_dir_env.startswith('./') or not ('/' in data_dir_env or '\\' in data_dir_env):
        DATA_DIR = BASE_DIR / data_dir_env.lstrip('./')
    # Sinon utiliser le chemin tel quel (absolu)
    else:
        DATA_DIR = Path(data_dir_env)
else:
    # Valeur par défaut si DATA_DIR n'est pas défini
    DATA_DIR = BASE_DIR / "data"

# Définition explicite de tous les autres chemins par rapport à DATA_DIR
GOLD_DIR = DATA_DIR / os.getenv('GOLD_SUBDIR', 'gold')
LOGS_DIR = DATA_DIR / os.getenv('LOGS_SUBDIR', 'logs')
CONFIG_DIR = BASE_DIR / "config"

# Conversion des chemins en chemins absolus pour le logging
GOLD_DIR = GOLD_DIR.resolve()
LOGS_DIR = LOGS_DIR.resolve()
CONFIG_DIR = CONFIG_DIR.resolve()

# Paramètres de traitement
EXPECTED_TABLES = os.getenv('EXPECTED_TABLES', "Device,EPG,Playback,User,Smartcard,VODCatalog,VODCatalogExtended").split(',')
MOCK_SNOWFLAKE = os.getenv('MOCK_SNOWFLAKE', 'true').lower() == 'true'
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
PARALLEL_PROCESSING = os.getenv('PARALLEL_PROCESSING', 'true').lower() == 'true'
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '4'))
LOADED_BY = os.getenv('LOADED_BY', 'ETL_GOLD_TO_SNOWFLAKE')  # Identifiant du processus de chargement

# Paramètres Snowflake
SF_USER = os.getenv('SNOWFLAKE_USER')
SF_PWD = os.getenv('SNOWFLAKE_PASSWORD')
SF_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SF_WHS = os.getenv('SNOWFLAKE_WAREHOUSE')
SF_DB = os.getenv('SNOWFLAKE_DATABASE')
SF_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'STG_SG')
SF_MONITORING_SCHEMA = os.getenv('SNOWFLAKE_MONITORING_SCHEMA', 'STG_SG_MONITORING')

# Assurez-vous que ces répertoires existent
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

# Classe pour gérer la sérialisation JSON personnalisée
class CustomJSONEncoder(json.JSONEncoder):
    """Encodeur JSON personnalisé pour gérer les types non-sérialisables"""
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

# Afficher la configuration au démarrage
def log_config():
    """Affiche la configuration actuelle dans les logs"""
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

# Chargement des fichiers de configuration
def load_config_files():
    """Charge les fichiers de configuration (brand_configs.json, table_schemas.json, validation_rules.json)"""
    configs = {}
    
    # Charger le fichier de mapping des marques
    brand_config_path = CONFIG_DIR / "brand_configs.json"
    try:
        with open(brand_config_path, 'r', encoding='utf-8') as f:
            configs['brands'] = json.load(f)
        logger.info(f"Configuration des marques chargée depuis {brand_config_path}")
    except Exception as e:
        logger.error(f"Erreur lors du chargement de la configuration des marques: {e}")
        configs['brands'] = {"brands": []}
    
    # Charger le schéma des tables
    table_schema_path = CONFIG_DIR / "table_schemas.json"
    try:
        with open(table_schema_path, 'r', encoding='utf-8') as f:
            configs['schemas'] = json.load(f)
        logger.info(f"Schémas des tables chargés depuis {table_schema_path}")
    except Exception as e:
        logger.error(f"Erreur lors du chargement des schémas de tables: {e}")
        configs['schemas'] = {}
    
    # Charger les règles de validation
    validation_rules_path = CONFIG_DIR / "validation_rules.json"
    try:
        with open(validation_rules_path, 'r', encoding='utf-8') as f:
            configs['validations'] = json.load(f)
        logger.info(f"Règles de validation chargées depuis {validation_rules_path}")
    except Exception as e:
        logger.error(f"Erreur lors du chargement des règles de validation: {e}")
        configs['validations'] = {}
    
    return configs

# Fonction pour extraire le nom de la table et la date depuis le nom de fichier
def parse_gold_filename(filename):
    """
    Extrait le nom de la table et la date depuis le nom de fichier Gold
    Exemple: Device_20250416.parquet -> ('Device', '20250416')
    """
    parts = filename.stem.split('_')
    if len(parts) >= 2 and parts[-1].isdigit() and len(parts[-1]) == 8:
        date = parts[-1]
        table_name = '_'.join(parts[:-1])
        return table_name, date
    
    return None, None

# Fonction pour enrichir les données avec les informations de marque
def enrich_brand_data(df, brand_configs):
    """
    Enrichit les données avec les informations complètes de marque basées sur le fichier de configuration
    """
    if 'BRAND' not in df.columns:
        logger.warning("Colonne BRAND manquante dans les données")
        return df
    
    # Créer une copie du DataFrame
    enriched_df = df.copy()
    
    # Récupérer la liste des marques depuis la configuration
    brand_mappings = brand_configs.get('brands', [])
    
    # Mapper les noms de marque avec les codes
    brand_map = {}
    for brand in brand_mappings:
        brand_map[brand.get('file_name', '')] = {
            'brand_code': brand.get('brand_code', '')
        }
    
    # Fonction pour appliquer le mapping à chaque ligne
    def map_brand(row):
        brand_name = row['BRAND']
        if brand_name in brand_map:
            row['BRAND_CODE'] = brand_map[brand_name]['brand_code']
        else:
            # Valeurs par défaut si aucune correspondance n'est trouvée
            row['BRAND_CODE'] = brand_name
        return row
    
    # Appliquer le mapping à chaque ligne
    enriched_df = enriched_df.apply(map_brand, axis=1)
    
    return enriched_df

# Fonctions de validation métier
def validate_user_data(df):
    """Valide les données utilisateur selon les règles métier"""
    errors = {}
    error_rows = []
    
    # Validation des types d'utilisateur
    if 'UserType' in df.columns:
        allowed_types = ["Trial", "Demo", "WebDemo", "Provisioned", "Anonymous"]
        invalid_types = df[~df['UserType'].isin(allowed_types) & ~df['UserType'].isna()]
        if len(invalid_types) > 0:
            errors['invalid_user_types'] = len(invalid_types)
            error_rows.extend(invalid_types.index.tolist())
    
    # Validation des champs requis
    required_fields = ["Name", "UserLogin"]
    for field in required_fields:
        if field in df.columns:
            missing_field = df[df[field].isna()]
            if len(missing_field) > 0:
                errors.setdefault('missing_required_fields', {})[field] = len(missing_field)
                error_rows.extend(missing_field.index.tolist())
    
    return errors, list(set(error_rows))

def validate_device_data(df):
    """Valide les données d'appareil selon les règles métier"""
    errors = {}
    error_rows = []
    
    # Validation des types d'appareil
    if 'device_type' in df.columns:
        allowed_types = ["STB", "Mobile", "Web", "Smart TV"]
        invalid_types = df[~df['device_type'].isin(allowed_types) & ~df['device_type'].isna()]
        if len(invalid_types) > 0:
            errors['invalid_device_types'] = len(invalid_types)
            error_rows.extend(invalid_types.index.tolist())
    
    return errors, list(set(error_rows))

def validate_smartcard_data(df):
    """Valide les données de carte à puce selon les règles métier"""
    errors = {}
    error_rows = []
    
    # Validation de l'unicité des ID de carte
    if 'SmartcardId' in df.columns:
        duplicate_cards = df[df.duplicated('SmartcardId', keep='first')]
        if len(duplicate_cards) > 0:
            errors['duplicate_smartcards'] = len(duplicate_cards)
            error_rows.extend(duplicate_cards.index.tolist())
    
    return errors, list(set(error_rows))

def validate_playback_data(df):
    """Valide les données de lecture selon les règles métier"""
    errors = {}
    error_rows = []
    
    # Validation de la durée de lecture
    if 'PlayDurationExPause' in df.columns and 'AssetDuration' in df.columns:
        # Convertir les colonnes en numériques
        df['PlayDurationExPause_num'] = pd.to_numeric(df['PlayDurationExPause'], errors='coerce')
        df['AssetDuration_num'] = pd.to_numeric(df['AssetDuration'], errors='coerce')
        
        # Validation: PlayDurationExPause <= AssetDuration OR AssetDuration == 0
        invalid_duration = df[
            (df['PlayDurationExPause_num'] > df['AssetDuration_num']) & 
            (df['AssetDuration_num'] != 0)
        ]
        
        if len(invalid_duration) > 0:
            errors['invalid_playback_duration'] = len(invalid_duration)
            error_rows.extend(invalid_duration.index.tolist())
    
    # Validation de la cohérence de la durée
    if 'PlayDuration' in df.columns and 'PlayDurationExPause' in df.columns:
        # Convertir les colonnes en numériques
        df['PlayDuration_num'] = pd.to_numeric(df['PlayDuration'], errors='coerce')
        df['PlayDurationExPause_num'] = pd.to_numeric(df['PlayDurationExPause'], errors='coerce')
        
        # Validation: PlayDuration >= PlayDurationExPause
        inconsistent_duration = df[df['PlayDuration_num'] < df['PlayDurationExPause_num']]
        
        if len(inconsistent_duration) > 0:
            errors['inconsistent_duration'] = len(inconsistent_duration)
            error_rows.extend(inconsistent_duration.index.tolist())
    
    # Validation de l'existence de l'utilisateur
    if 'UserID' in df.columns:
        missing_user = df[df['UserID'].isna()]
        if len(missing_user) > 0:
            errors['missing_user_id'] = len(missing_user)
            error_rows.extend(missing_user.index.tolist())
    
    return errors, list(set(error_rows))

def validate_epg_data(df):
    """Valide les données EPG selon les règles métier"""
    errors = {}
    error_rows = []
    
    # Validation de la validité des épisodes
    if 'season' in df.columns and 'episode' in df.columns:
        # Validation: NOT (season IS NULL AND episode IS NOT NULL)
        invalid_episodes = df[df['season'].isna() & ~df['episode'].isna()]
        
        if len(invalid_episodes) > 0:
            errors['invalid_episodes'] = len(invalid_episodes)
            error_rows.extend(invalid_episodes.index.tolist())
    
    return errors, list(set(error_rows))

def validate_vod_catalog_data(df):
    """Valide les données du catalogue VOD selon les règles métier"""
    errors = {}
    error_rows = []
    
    # Validation de la cohérence des épisodes
    if 'series_episode' in df.columns and 'series_episode_count' in df.columns:
        # Convertir les colonnes en numériques
        df['series_episode_num'] = pd.to_numeric(df['series_episode'], errors='coerce')
        df['series_episode_count_num'] = pd.to_numeric(df['series_episode_count'], errors='coerce')
        
        # Validation: series_episode <= series_episode_count OR series_episode IS NULL OR series_episode_count IS NULL OR series_episode_count == 0
        invalid_episodes = df[
            (df['series_episode_num'] > df['series_episode_count_num']) & 
            (~df['series_episode_num'].isna()) & 
            (~df['series_episode_count_num'].isna()) & 
            (df['series_episode_count_num'] != 0)
        ]
        
        if len(invalid_episodes) > 0:
            errors['invalid_episodes'] = len(invalid_episodes)
            error_rows.extend(invalid_episodes.index.tolist())
    
    return errors, list(set(error_rows))

def validate_vod_catalog_extended_data(df):
    """Valide les données étendues du catalogue VOD selon les règles métier"""
    # Pas de validation spécifique pour VODCatalogExtended
    return {}, []

# Sélectionner la fonction de validation appropriée pour chaque type de table
def get_validation_function(table_name):
    """Retourne la fonction de validation appropriée pour le type de table donné"""
    validation_functions = {
        'User': validate_user_data,
        'Device': validate_device_data,
        'Smartcard': validate_smartcard_data,
        'Playback': validate_playback_data,
        'EPG': validate_epg_data,
        'VODCatalog': validate_vod_catalog_data,
        'VODCatalogExtended': validate_vod_catalog_extended_data
    }
    
    return validation_functions.get(table_name, lambda df: ({}, []))

# Fonction pour insérer des logs dans Snowflake
def insert_gold_log(process_id, process_date, execution_start, execution_end, date_folder, 
                   table_name, brand, reseller, records_before_agg, records_after_agg, 
                   duplicates_removed, brands_aggregated, checks_total, checks_passed, 
                   checks_failed, checks_skipped, records_valid, records_warning, records_error, 
                   snowflake_rows_inserted, snowflake_rows_updated, snowflake_rows_rejected, 
                   processing_time_ms, memory_usage_mb, status, error_message, 
                   business_checks_details, aggregation_details, loading_details):
    """
    Insère un enregistrement de log dans la table GOLD_LOGS de Snowflake
    """
    try:
        # Convertir les détails en JSON sérialisable
        business_checks_json = json.dumps(business_checks_details, cls=CustomJSONEncoder)
        aggregation_details_json = json.dumps(aggregation_details, cls=CustomJSONEncoder)
        loading_details_json = json.dumps(loading_details, cls=CustomJSONEncoder)
        
        if MOCK_SNOWFLAKE:
            # Version locale pour les tests
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
            
            # Créer un nom de fichier unique pour le log
            filename = LOGS_DIR / f"gold_logs_{table_name}_{brand}_{date_folder}_{process_id}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(log_entry, f, indent=2, cls=CustomJSONEncoder)
            
            logger.info(f"Log local créé: {filename}")
            return True
        else:
            # Connexion à Snowflake
            ctx = snowflake.connector.connect(
                user=SF_USER,
                password=SF_PWD,
                account=SF_ACCOUNT,
                warehouse=SF_WHS,
                database=SF_DB,
                schema=SF_MONITORING_SCHEMA
            )
            cs = ctx.cursor()
            
            # Préparer la requête SQL
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
            logger.info(f"Log inséré dans Snowflake pour {table_name} {brand} {date_folder}")
            
            cs.close()
            ctx.close()
            return True
            
    except Exception as e:
        logger.error(f"Erreur lors de l'insertion du log: {e}")
        logger.error(traceback.format_exc())
        return False

# Fonction pour charger les données dans Snowflake
def load_to_snowflake(df, table_name):
    """
    Charge les données du DataFrame dans la table Snowflake correspondante
    Retourne un tuple (inserted, updated, rejected)
    """
    try:
        if MOCK_SNOWFLAKE:
            # Pour les tests, simuler le chargement
            rows = len(df)
            logger.info(f"[MOCK] Chargement simulé de {rows} lignes dans la table {table_name}")
            # Pour les tests, considérer que toutes les lignes sont insérées
            return rows, 0, 0
        
        # Nom de la table Snowflake cible
        sf_table_name = f"AGG_{table_name.upper()}"
        
        # Convertir le DataFrame en CSV pour le chargement
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_NONNUMERIC)
        csv_buffer.seek(0)
        
        # Connexion à Snowflake
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
            
            # Créer une étape temporaire pour le chargement
            stage_name = f"TEMP_STAGE_{uuid.uuid4().hex}"
            cursor.execute(f"CREATE TEMPORARY STAGE {stage_name}")
            
            # Déterminer les colonnes disponibles dans la table cible
            cursor.execute(f"DESCRIBE TABLE {SF_SCHEMA}.{sf_table_name}")
            table_columns = [row[0].upper() for row in cursor.fetchall()]
            
            # Filtrer le DataFrame pour n'inclure que les colonnes existantes dans la table
            columns_to_use = [col for col in df.columns if col.upper() in table_columns]
            df_filtered = df[columns_to_use]
            
            # Recréer le CSV avec uniquement les colonnes existantes
            csv_buffer = io.StringIO()
            df_filtered.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_NONNUMERIC)
            csv_buffer.seek(0)
            
            # Charger les données dans l'étape
            put_sql = f"PUT file://{csv_buffer} @{stage_name}"
            cursor.execute(put_sql)
            
            # Déterminer les colonnes cibles pour le COPY
            columns_str = ", ".join([f'"{col.upper()}"' for col in columns_to_use])
            
            # Charger les données dans la table
            copy_sql = f"""
            COPY INTO {SF_SCHEMA}.{sf_table_name} ({columns_str})
            FROM @{stage_name}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
            ON_ERROR = 'CONTINUE'
            """
            cursor.execute(copy_sql)
            
            # Récupérer les résultats du chargement
            copy_results = cursor.fetchall()
            rows_loaded = sum(int(row[3]) for row in copy_results) if copy_results else 0
            errors = sum(int(row[4]) for row in copy_results) if copy_results else 0
            
            # En mode COPY, toutes les lignes correctes sont insérées
            # (Snowflake fera une mise à jour si la clé primaire existe déjà)
            rows_inserted = rows_loaded
            rows_updated = 0  # Pas de distinction entre insertion et mise à jour en mode COPY
            
            logger.info(f"Chargement dans Snowflake réussi pour {sf_table_name}: {rows_inserted} lignes chargées, {errors} erreurs")
            
            return rows_inserted, rows_updated, errors
            
        finally:
            # Nettoyer la table d'étape
            try:
                cursor.execute(f"DROP STAGE IF EXISTS {stage_name}")
            except:
                pass
            
            # Fermer la connexion
            cursor.close()
            conn.close()
        
    except Exception as e:
        logger.error(f"Erreur lors du chargement dans Snowflake: {e}")
        logger.error(traceback.format_exc())
        return 0, 0, len(df)

# Fonction pour traiter un fichier Gold
def process_gold_file(file_path, configs):
    """
    Traite un fichier Gold individuel et charge les données dans Snowflake
    """
    try:
        start_time = datetime.now()
        process_id = str(uuid.uuid4())
        memory_start = psutil.Process().memory_info().rss / (1024 * 1024)  # En MB
        
        # Extraire le nom de la table et la date depuis le nom de fichier
        table_name, date_folder = parse_gold_filename(file_path)
        
        if not table_name or not date_folder:
            logger.error(f"Format de nom de fichier non valide: {file_path}")
            return False
        
        logger.info(f"Traitement du fichier {file_path}: table={table_name}, date={date_folder}")
        
        # Lire le fichier Parquet
        df = pd.read_parquet(file_path)
        records_before_agg = len(df)
        
        if records_before_agg == 0:
            logger.warning(f"Aucune donnée trouvée dans le fichier {file_path}")
            return False
        
        # Enrichir les données avec les informations de marque
        df = enrich_brand_data(df, configs.get('brands', {}))
        
        # Vérifier si les données contiennent la colonne BRAND
        if 'BRAND' not in df.columns:
            logger.error(f"Colonne BRAND manquante dans le fichier {file_path}")
            return False
        
        # Traitement par marque
        brands = df['BRAND'].unique()
        
        for brand in brands:
            brand_df = df[df['BRAND'] == brand]
            brand_records = len(brand_df)
            
            # Récupérer le code de marque
            brand_code = brand_df['BRAND_CODE'].iloc[0] if 'BRAND_CODE' in brand_df.columns else brand
            
            # Éliminer les colonnes temporaires
            if 'BRAND_CODE' in brand_df.columns:
                brand_df = brand_df.drop(columns=['BRAND_CODE'])
            
            # Déterminer les colonnes clés pour la déduplication
            key_columns = []
            if table_name == 'User':
                key_columns = ['Userid']
            elif table_name == 'Device':
                key_columns = ['Userid', 'Serial']
            elif table_name == 'Smartcard':
                key_columns = ['SmartcardId']
            elif table_name == 'Playback':
                key_columns = ['PlaySessionID']
            elif table_name == 'EPG':
                key_columns = ['broadcast_datetime', 'station_id']
            elif table_name == 'VODCatalog' or table_name == 'VODCatalogExtended':
                key_columns = ['external_id']
            
            # Dédupliquer les données si des colonnes clés sont définies
            duplicates_removed = 0
            if key_columns and all(col in brand_df.columns for col in key_columns):
                before_dedup = len(brand_df)
                brand_df = brand_df.drop_duplicates(subset=key_columns, keep='first')
                duplicates_removed = before_dedup - len(brand_df)
            
            records_after_agg = len(brand_df)
            
            # Effectuer les validations métier
            validation_function = get_validation_function(table_name)
            validation_errors, error_rows = validation_function(brand_df)
            
            # Calculer les statistiques de validation
            checks_total = sum(1 for _ in validation_errors)
            checks_failed = len(validation_errors)
            checks_passed = checks_total - checks_failed
            checks_skipped = 0
            
            records_valid = records_after_agg - len(error_rows)
            records_warning = 0
            records_error = len(error_rows)
            
            # Préparer les détails de validation pour le log
            business_checks_details = {
                "validation_errors": validation_errors,
                "error_count": len(error_rows)
            }
            
            # Préparer les détails d'agrégation pour le log
            aggregation_details = {
                "records_before": brand_records,
                "records_after": records_after_agg,
                "duplicates_removed": duplicates_removed,
                "key_columns": key_columns
            }
            
            # Déterminer le statut de la validation
            status = "OK" if checks_failed == 0 else "WARNING" if records_valid > 0 else "ERROR"
            
            # Charger les données dans Snowflake
            execution_start = start_time.strftime('%Y-%m-%d %H:%M:%S')
            
            # Normaliser les noms de colonnes pour Snowflake (si nécessaire)
            snowflake_df = brand_df.copy()
            
            # Ajouter les colonnes LOADDATE et LOADEDBY
            current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            snowflake_df['LOADDATE'] = current_timestamp
            snowflake_df['LOADEDBY'] = LOADED_BY  # Utiliser la variable globale
            
            # Résoudre les problèmes potentiels de typage pour Snowflake
            for col in snowflake_df.columns:
                if snowflake_df[col].dtype == 'object':
                    snowflake_df[col] = snowflake_df[col].astype(str)
                elif pd.api.types.is_datetime64_any_dtype(snowflake_df[col]):
                    snowflake_df[col] = snowflake_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Charger dans Snowflake
            rows_inserted, rows_updated, rows_rejected = load_to_snowflake(snowflake_df, table_name)
            
            # Préparer les détails de chargement pour le log
            loading_details = {
                "snowflake_rows_inserted": rows_inserted,
                "snowflake_rows_updated": rows_updated,
                "snowflake_rows_rejected": rows_rejected,
                "table_name": f"AGG_{table_name.upper()}"
            }
            
            # Calculer les métriques de performance
            end_time = datetime.now()
            execution_end = end_time.strftime('%Y-%m-%d %H:%M:%S')
            processing_time_ms = int((end_time - start_time).total_seconds() * 1000)
            memory_end = psutil.Process().memory_info().rss / (1024 * 1024)  # En MB
            memory_usage_mb = round(memory_end - memory_start, 2)
            
            # Insérer le log
            process_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            error_message = None if status == "OK" else f"{checks_failed} checks failed with {records_error} records in error"
            
            insert_gold_log(
                process_id=process_id,
                process_date=process_date,
                execution_start=execution_start,
                execution_end=execution_end,
                date_folder=date_folder,
                table_name=table_name,
                brand=brand_code,  # Utiliser le code de marque
                reseller="",       # Champ vide car plus utilisé
                records_before_agg=brand_records,
                records_after_agg=records_after_agg,
                duplicates_removed=duplicates_removed,
                brands_aggregated=1,  # Une seule marque par itération
                checks_total=checks_total,
                checks_passed=checks_passed,
                checks_failed=checks_failed,
                checks_skipped=checks_skipped,
                records_valid=records_valid,
                records_warning=records_warning,
                records_error=records_error,
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
            
            logger.info(f"Traitement terminé pour {table_name} / {brand} / {date_folder} avec statut {status}")
        
        return True
        
    except Exception as e:
        logger.error(f"Erreur lors du traitement du fichier {file_path}: {e}")
        logger.error(traceback.format_exc())
        return False

# Fonction principale de traitement
def process_gold_to_snowflake():
    """
    Traite les fichiers du Gold layer pour les charger dans Snowflake
    """
    logger.info("Début du traitement GOLD -> SNOWFLAKE")
    
    # 1. Charger les fichiers de configuration
    configs = load_config_files()
    
    # 2. Lister tous les fichiers Parquet dans le Gold layer
    try:
        gold_files = list(GOLD_DIR.glob('*.parquet'))
        if not gold_files:
            logger.warning(f"Aucun fichier Parquet trouvé dans {GOLD_DIR}")
            return False
        
        logger.info(f"Fichiers trouvés dans Gold: {len(gold_files)}")
        
        # 3. Traiter chaque fichier Gold
        success = True
        
        if PARALLEL_PROCESSING and len(gold_files) > 1:
            logger.info(f"Traitement parallèle de {len(gold_files)} fichiers avec {MAX_WORKERS} workers")
            futures = {}
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Soumettre les tâches
                for file_path in gold_files:
                    future = executor.submit(process_gold_file, file_path, configs)
                    futures[future] = file_path
                
                # Collecter les résultats
                for future in as_completed(futures):
                    file_path = futures[future]
                    try:
                        result = future.result()
                        if not result:
                            logger.warning(f"Échec du traitement pour {file_path}")
                            success = False
                    except Exception as e:
                        logger.error(f"Erreur lors du traitement parallèle de {file_path}: {e}")
                        logger.error(traceback.format_exc())
                        success = False
        else:
            logger.info(f"Traitement séquentiel de {len(gold_files)} fichiers")
            for file_path in gold_files:
                try:
                    result = process_gold_file(file_path, configs)
                    if not result:
                        logger.warning(f"Échec du traitement pour {file_path}")
                        success = False
                except Exception as e:
                    logger.error(f"Erreur lors du traitement de {file_path}: {e}")
                    logger.error(traceback.format_exc())
                    success = False
    
    except Exception as e:
        logger.error(f"Erreur lors du traitement GOLD -> SNOWFLAKE: {e}")
        logger.error(traceback.format_exc())
        return False
    
    return success

# Lambda handler
def lambda_handler(event, context):
    """
    Fonction handler pour AWS Lambda ou AWS Glue
    """
    logger.info("Lambda/Glue invocation started")
    try:
        # Affichage de la configuration
        log_config()
        
        # Exécution du traitement
        success = process_gold_to_snowflake()
        
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
        # Afficher les chemins pour le débogage
        print(f"BASE_DIR: {BASE_DIR}")
        print(f"DATA_DIR: {DATA_DIR}")
        print(f"GOLD_DIR: {GOLD_DIR}")
        print(f"LOGS_DIR: {LOGS_DIR}")
        print(f"CONFIG_DIR: {CONFIG_DIR}")
        
        logger.info("Starting GOLD to SNOWFLAKE processing")
        
        # Afficher la configuration
        log_config()
        
        # Enregistrer la date/heure de début
        start_time = datetime.now()
        logger.info(f"Process started at: {start_time}")
        
        # Exécuter le traitement principal
        success = process_gold_to_snowflake()
        
        # Enregistrer la date/heure de fin et la durée
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"Process completed at: {end_time}")
        logger.info(f"Total duration: {duration}")
        
        # Afficher un résumé sur la console
        print(f"\n=== RÉSUMÉ D'EXÉCUTION ===")
        print(f"Démarré à    : {start_time}")
        print(f"Terminé à    : {end_time}")
        print(f"Durée totale : {duration}")
        print(f"Statut       : {'SUCCÈS' if success else 'ÉCHEC'}")
        print(f"Journal d'exécution : {log_file}")
        print(f"===========================\n")
        
    except Exception as e:
        # Capture toutes les exceptions non traitées
        logger.error(f"ERREUR CRITIQUE: {e}")
        logger.error(traceback.format_exc())
        print(f"\n[ERREUR CRITIQUE]")
        print(f"Une erreur s'est produite pendant l'exécution:")
        print(f"{str(e)}")
        print(f"Consultez le journal pour plus de détails: {log_file}\n")