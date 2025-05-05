#!/usr/bin/env python3
import os
import json
import re
import logging
import traceback
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import pyarrow as pa
import pyarrow.parquet as pq
import snowflake.connector
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.validation_utils import load_validation_rules, validate_with_rules

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
SILVER_DIR = DATA_DIR / os.getenv('SILVER_SUBDIR', 'silver')
GOLD_DIR = DATA_DIR / os.getenv('GOLD_SUBDIR', 'gold')
LOGS_DIR = DATA_DIR / os.getenv('LOGS_SUBDIR', 'logs')

# Conversion des chemins en chemins absolus pour le logging
SILVER_DIR = SILVER_DIR.resolve()
GOLD_DIR = GOLD_DIR.resolve()
LOGS_DIR = LOGS_DIR.resolve()

# Paramètres de traitement
EXPECTED_TABLES = os.getenv('EXPECTED_TABLES', "Device,EPG,Playback,User,Smartcard,VODCatalog,VODCatalogExtended").split(',')
MOCK_SNOWFLAKE = os.getenv('MOCK_SNOWFLAKE', 'true').lower() == 'true'
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
PARALLEL_PROCESSING = os.getenv('PARALLEL_PROCESSING', 'true').lower() == 'true'
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '4'))

# Paramètres Snowflake
SF_USER = os.getenv('SNOWFLAKE_USER')
SF_PWD = os.getenv('SNOWFLAKE_PASSWORD')
SF_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SF_WHS = os.getenv('SNOWFLAKE_WAREHOUSE')
SF_DB = os.getenv('SNOWFLAKE_DATABASE')
SF_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'STG_SG_MONITORING')

# Assurez-vous que ces répertoires existent
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

# Charger les règles de validation
validation_rules = load_validation_rules()

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
        "SILVER_DIR": str(SILVER_DIR),
        "GOLD_DIR": str(GOLD_DIR),
        "LOGS_DIR": str(LOGS_DIR),
        "EXPECTED_TABLES": EXPECTED_TABLES,
        "MOCK_SNOWFLAKE": MOCK_SNOWFLAKE,
        "LOG_LEVEL": LOG_LEVEL,
        "PARALLEL_PROCESSING": PARALLEL_PROCESSING,
        "MAX_WORKERS": MAX_WORKERS
    }
    logger.info("Configuration:")
    for key, value in config.items():
        logger.info(f"  {key}: {value}")

# Utility: Extract brand and date from path
def extract_info_from_path(file_path):
    """
    Extrait la marque et la date à partir du chemin du fichier
    Exemple: silver/20250416/canal_digitaal/Device.parquet -> ('canal_digitaal', '20250416')
    """
    try:
        # Le dossier parent du fichier contient le nom de la marque
        brand_slug = file_path.parent.name
        # Convertir le slug en nom plus lisible
        brand = brand_slug.replace('_', ' ').title()
        
        # La date est le nom du dossier parent du parent (structure: silver/DATE/BRAND/FILE.parquet)
        file_date = file_path.parent.parent.name
        
        return brand, file_date
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction des informations depuis {file_path}: {e}")
        return "unknown_brand", "unknown_date"

# Fonction locale pour simuler l'insertion de logs dans Snowflake
def insert_log_local(proc_date, brand, extraction_date, table_name, rows_total, rows_ok, rows_ko, cols_error, status, details):
    """
    Version locale de la fonction insert_log qui écrit dans un fichier JSON
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
    
    # Créer un nom de fichier unique basé sur la date, la marque et la table
    filename = LOGS_DIR / f"silver_logs_{brand}_{table_name}_{extraction_date}.json"
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            # Utiliser l'encodeur personnalisé pour gérer les types non-sérialisables
            json.dump(log_entry, f, indent=2, cls=CustomJSONEncoder)
        
        logger.info(f"Log local créé: {filename}")
    except Exception as e:
        logger.error(f"Erreur lors de l'écriture du log local: {e}")
        logger.error(traceback.format_exc())

# Insertion des logs dans Snowflake
def insert_log_snowflake(proc_date, brand, extraction_date, table_name, rows_total, rows_ok, rows_ko, cols_error, status, details):
    """
    Insère un enregistrement de log dans Snowflake
    """
    try:
        # Convertir les détails en JSON sérialisable
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

# Fonction de validation basée sur les règles externes
def get_validation_function(table_name):
    """
    Retourne une fonction de validation qui applique les règles définies dans le fichier de configuration
    pour la table spécifiée
    """
    def validate_with_loaded_rules(df):
        """Fonction de validation qui utilise les règles chargées pour la table"""
        return validate_with_rules(df, table_name, validation_rules)
    
    return validate_with_loaded_rules

# Harmoniser les types de données dans les DataFrames
def harmonize_dataframe_types(dfs):
    """
    Harmonise les types de données à travers plusieurs DataFrames pour éviter les erreurs de conversion
    lors de la fusion et de l'écriture Parquet
    """
    if not dfs or len(dfs) == 0:
        return []
    
    # Identifier toutes les colonnes à travers tous les DataFrames
    all_columns = set()
    for df in dfs:
        all_columns.update(df.columns)
    
    harmonized_dfs = []
    
    for df in dfs:
        # Créer une copie pour ne pas modifier l'original
        df_copy = df.copy()
        
        # Pour chaque colonne dans ce DataFrame
        for col in df_copy.columns:
            # Si la colonne contient des objets Python, la convertir en chaîne
            if df_copy[col].dtype == 'object':
                # Convertir les NaN en None
                df_copy[col] = df_copy[col].where(pd.notna(df_copy[col]), None)
                
                # Convertir tous les éléments en chaînes de caractères pour éviter les types mixtes
                df_copy[col] = df_copy[col].apply(lambda x: str(x) if x is not None else None)
            
            # Gérer spécifiquement les colonnes problématiques connues
            if col == 'SSOID':
                # Convertir SSOID en chaîne pour éviter l'erreur "Expected bytes, got a 'float' object"
                df_copy[col] = df_copy[col].apply(lambda x: str(x) if x is not None and pd.notna(x) else None)
        
        harmonized_dfs.append(df_copy)
    
    return harmonized_dfs

# Déterminer le nom de base de la table depuis le nom du fichier
def get_table_name_from_file(file_path):
    """
    Extrait le nom de base de la table à partir du nom de fichier
    Exemples:
    - 'Canal Digitaal_Device_20250416.parquet' -> 'Device'
    - 'Canal Digitaal - Resellers_Playback_20250416.parquet' -> 'Playback'
    """
    filename = file_path.name
    # Enlever l'extension
    if filename.lower().endswith('.parquet'):
        filename = filename[:-8]  # Enlever '.parquet'
    
    # Trouver les parties du nom
    parts = filename.split('_')
    
    # Le nom de la table est généralement l'avant-dernière partie
    # (Marque_Table_Date ou Marque_Sous-Marque_Table_Date)
    if len(parts) >= 2:
        # Si la dernière partie est une date, la table est l'avant-dernière partie
        if parts[-1].isdigit() and len(parts[-1]) == 8:  # Format date YYYYMMDD
            return parts[-2]
    
    # Détecter des noms de table connus dans le nom de fichier
    for known_table in EXPECTED_TABLES:
        if known_table in filename:
            return known_table
    
    # Fallback - utiliser le nom du fichier sans extension
    return file_path.stem

# Traitement d'un fichier Parquet individuel
def process_single_parquet(file_path):
    """
    Traite un fichier Parquet individuel, ajoute les colonnes BRAND et FILEDATE et effectue les validations
    Retourne un DataFrame enrichi et des informations de validation
    """
    try:
        # Lecture du fichier Parquet
        df = pd.read_parquet(file_path)
        
        # Extraction des informations importantes
        brand, file_date = extract_info_from_path(file_path)
        
        # Déterminer le nom de la table depuis le nom du fichier
        table_name = get_table_name_from_file(file_path)
        
        # Formatage de la date d'extraction pour l'affichage
        extraction_date = file_date
        if len(file_date) == 8:  # Format YYYYMMDD
            extraction_date = f"{file_date[:4]}-{file_date[4:6]}-{file_date[6:8]}"
        
        logger.info(f"Traitement de {file_path}: brand={brand}, table={table_name}, date={file_date}")
        
        # Ajouter les colonnes BRAND et FILEDATE
        df['BRAND'] = brand
        df['FILEDATE'] = file_date
        
        # Effectuer les validations spécifiques à ce type de table
        validation_function = get_validation_function(table_name)
        validation_errors, error_rows = validation_function(df)
        
        # Calcul des statistiques
        rows_total = len(df)
        rows_ko = len(error_rows)
        rows_ok = rows_total - rows_ko
        
        # Création du rapport de validation
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
            'error_rows': error_rows[:100] if len(error_rows) > 100 else error_rows  # Limiter le nombre d'erreurs pour éviter des logs trop volumineux
        }
        
        return df, validation_report
    
    except Exception as e:
        logger.error(f"Erreur lors du traitement de {file_path}: {e}")
        logger.error(traceback.format_exc())
        # Retourner un DataFrame vide et un rapport d'erreur
        return pd.DataFrame(), {
            'file_path': str(file_path),
            'error': str(e),
            'traceback': traceback.format_exc()
        }

# Fonction principale de traitement
def process_silver_to_gold():
    """
    Traite les fichiers du Silver layer pour les convertir en Gold layer
    """
    logger.info("Début du traitement SILVER -> GOLD")
    
    # 1. Regrouper tous les fichiers Parquet par type de table
    parquet_files_by_table = {}
    table_dates = {}  # Pour stocker les dates uniques par table
    
    try:
        # Parcourir tous les fichiers Parquet dans le Silver layer
        for parquet_file in SILVER_DIR.glob('**/*.parquet'):
            # Extraire le nom de la table depuis le nom du fichier
            table_name = get_table_name_from_file(parquet_file)
            
            # Extraire la date du fichier
            _, file_date = extract_info_from_path(parquet_file)
            
            # Regrouper les fichiers par table
            if table_name not in parquet_files_by_table:
                parquet_files_by_table[table_name] = []
                table_dates[table_name] = set()
            
            parquet_files_by_table[table_name].append(parquet_file)
            table_dates[table_name].add(file_date)
            
    except Exception as e:
        logger.error(f"Erreur lors du scan des fichiers Parquet: {e}")
        logger.error(traceback.format_exc())
        return False
    
    # Si aucun fichier trouvé, terminer
    if not parquet_files_by_table:
        logger.warning("Aucun fichier Parquet trouvé dans le Silver layer")
        return False
    
    logger.info(f"Tables trouvées: {list(parquet_files_by_table.keys())}")
    
    # 2. Traiter chaque type de table
    all_validations = []
    success = True
    
    for table_name, files in parquet_files_by_table.items():
        logger.info(f"Traitement de la table {table_name} ({len(files)} fichiers)")
        
        # Créer le répertoire Gold s'il n'existe pas
        GOLD_DIR.mkdir(exist_ok=True, parents=True)
        
        # Pour chaque date unique de cette table
        for file_date in table_dates[table_name]:
            # Filtrer les fichiers pour cette date
            date_files = [f for f in files if extract_info_from_path(f)[1] == file_date]
            
            if not date_files:
                continue
                
            processed_dfs = []
            table_validations = []
            
            # Traitement parallèle ou séquentiel
            if PARALLEL_PROCESSING and len(date_files) > 1:
                futures = {}
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    # Soumettre les tâches
                    for file_path in date_files:
                        future = executor.submit(process_single_parquet, file_path)
                        futures[future] = file_path
                    
                    # Collecter les résultats
                    for future in as_completed(futures):
                        file_path = futures[future]
                        try:
                            df, validation = future.result()
                            if not df.empty:
                                processed_dfs.append(df)
                                table_validations.append(validation)
                        except Exception as e:
                            logger.error(f"Erreur lors du traitement parallèle de {file_path}: {e}")
                            logger.error(traceback.format_exc())
            else:
                # Traitement séquentiel
                for file_path in date_files:
                    df, validation = process_single_parquet(file_path)
                    if not df.empty:
                        processed_dfs.append(df)
                        table_validations.append(validation)
            
            # S'il n'y a pas de données valides, passer à la date suivante
            if not processed_dfs:
                logger.warning(f"Aucune donnée valide pour la table {table_name} à la date {file_date}")
                continue
            
            # 3. Fusionner tous les DataFrames pour cette table et cette date
            try:
                # Harmoniser les types de données avant la fusion
                harmonized_dfs = harmonize_dataframe_types(processed_dfs)
                
                # Fusionner les DataFrames harmonisés
                merged_df = pd.concat(harmonized_dfs, ignore_index=True)
                
                # Chemin du fichier Gold pour cette table et cette date
                gold_file_path = GOLD_DIR / f"{table_name}_{file_date}.parquet"
                
                # Écrire le fichier Gold avec gestion des erreurs
                try:
                    # Essayer d'écrire le fichier directement
                    merged_df.to_parquet(gold_file_path, index=False)
                except Exception as e:
                    logger.warning(f"Erreur lors de l'écriture directe de {gold_file_path}: {e}")
                    # Stratégie alternative: convertir en CSV puis charger de nouveau
                    csv_temp = LOGS_DIR / f"temp_{table_name}_{file_date}.csv"
                    logger.info(f"Essai de conversion à travers CSV: {csv_temp}")
                    
                    # Écrire en CSV
                    merged_df.to_csv(csv_temp, index=False)
                    
                    # Recharger le CSV et écrire en Parquet
                    temp_df = pd.read_csv(csv_temp)
                    # Résoudre les problèmes de type de données
                    for col in temp_df.columns:
                        if col == 'SSOID' or col.endswith('ID') or 'id' in col.lower():
                            temp_df[col] = temp_df[col].astype(str)
                    
                    temp_df.to_parquet(gold_file_path, index=False)
                    
                    # Nettoyer
                    if csv_temp.exists():
                        os.remove(csv_temp)
                
                logger.info(f"Fichier Gold créé: {gold_file_path} ({len(merged_df)} lignes)")
                
                # 4. Écrire les logs pour chaque brand/table
                brands = merged_df['BRAND'].unique()
                
                for brand in brands:
                    brand_df = merged_df[merged_df['BRAND'] == brand]
                    
                    # Trouver les validations correspondant à cette marque
                    brand_validations = [v for v in table_validations if v.get('brand') == brand]
                    
                    # Agréger les erreurs
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
                                    # Fusionner les informations d'erreur
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
                                        # Pour les compteurs numériques, les additionner
                                        if isinstance(details[error_type], (int, float)) and isinstance(error_info, (int, float)):
                                            details[error_type] += error_info
                                        else:
                                            details[error_type] = error_info
                                
                                # Ajouter le type d'erreur à la liste des colonnes en erreur
                                if error_type not in cols_error:
                                    cols_error.append(error_type)
                        
                        # Mettre à jour les compteurs
                        rows_ko += v.get('rows_ko', 0)
                    
                    rows_ok = rows_total - rows_ko
                    
                    # Déterminer le statut
                    status = "OK" if rows_ko == 0 else "KO"
                    
                    # Format de la date d'extraction (YYYY-MM-DD)
                    extraction_date = brand_validations[0].get('extraction_date') if brand_validations else f"{file_date[:4]}-{file_date[4:6]}-{file_date[6:8]}"
                    
                    # Insérer le log
                    proc_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    cols_error_str = ','.join(cols_error) if cols_error else ''
                    
                    if MOCK_SNOWFLAKE:
                        insert_log_local(proc_date, brand, extraction_date, table_name, rows_total, rows_ok, rows_ko, cols_error_str, status, details)
                    else:
                        insert_log_snowflake(proc_date, brand, extraction_date, table_name, rows_total, rows_ok, rows_ko, cols_error_str, status, details)
                
                # Ajouter les validations à la liste globale
                all_validations.extend(table_validations)
                    
            except Exception as e:
                logger.error(f"Erreur lors de la fusion des données pour {table_name} à la date {file_date}: {e}")
                logger.error(traceback.format_exc())
                success = False
    
    # Retourner True si au moins une table a été traitée avec succès
    return success and len(all_validations) > 0

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
        success = process_silver_to_gold()
        
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
        print(f"SILVER_DIR: {SILVER_DIR}")
        print(f"GOLD_DIR: {GOLD_DIR}")
        print(f"LOGS_DIR: {LOGS_DIR}")
        
        logger.info("Starting SILVER to GOLD processing")
        
        # Afficher la configuration
        log_config()
        
        # Enregistrer la date/heure de début
        start_time = datetime.now()
        logger.info(f"Process started at: {start_time}")
        
        # Exécuter le traitement principal
        success = process_silver_to_gold()
        
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