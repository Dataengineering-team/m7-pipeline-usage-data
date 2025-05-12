#!/usr/bin/env python3
"""
Utilitaires de configuration centralisée pour le projet
"""
import os
import json
import logging
import functools
from pathlib import Path
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Charger les variables d'environnement
load_dotenv()

class Config:
    """
    Classe de configuration centralisée pour le projet.
    Combine les variables d'environnement et les fichiers de configuration.
    """
    
    def __init__(self, config_dir=None):
        """Initialisation du gestionnaire de configuration"""
        # Chemins de base
        self.base_dir = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        
        if config_dir:
            self.config_dir = Path(config_dir)
        else:
            self.config_dir = self.base_dir / "config"
        
        # Charger les configurations
        self._load_configs()
        
        # Paramètres de chemin calculés
        data_dir_env = os.getenv('DATA_DIR')
        if data_dir_env:
            if data_dir_env.startswith('./') or not ('/' in data_dir_env or '\\' in data_dir_env):
                self.data_dir = self.base_dir / data_dir_env.lstrip('./')
            else:
                self.data_dir = Path(data_dir_env)
        else:
            self.data_dir = self.base_dir / "data"
        
        # Autres répertoires
        self.landing_dir = self.data_dir / os.getenv('LANDING_SUBDIR', 'landing')
        self.raw_dir = self.data_dir / os.getenv('RAW_SUBDIR', 'raw')
        self.silver_dir = self.data_dir / os.getenv('SILVER_SUBDIR', 'silver')
        self.gold_dir = self.data_dir / os.getenv('GOLD_SUBDIR', 'gold')
        self.logs_dir = self.data_dir / os.getenv('LOGS_SUBDIR', 'logs')
        
        # Paramètres Snowflake
        self.snowflake = {
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA', 'STG_SG'),
            'monitoring_schema': os.getenv('SNOWFLAKE_MONITORING_SCHEMA', 'STG_SG_MONITORING')
        }
        
        # Paramètres de traitement
        self.processing = {
            'expected_tables': os.getenv('EXPECTED_TABLES', "Device,EPG,Playback,User,Smartcard,VODCatalog,VODCatalogExtended").split(','),
            'max_days_back': int(os.getenv('MAX_DAYS_BACK', '3')),
            'delete_source_files': os.getenv('DELETE_SOURCE_FILES_AFTER_LOAD', 'false').lower() == 'true',
            'delete_silver_files': os.getenv('DELETE_SILVER_FILES_AFTER_LOAD', 'false').lower() == 'true',
            'gold_retention_days': int(os.getenv('GOLD_RETENTION_DAYS', '30')),
            'raw_retention_days': int(os.getenv('RAW_RETENTION_DAYS', '0')),
            'parallel_processing': os.getenv('PARALLEL_PROCESSING', 'true').lower() == 'true',
            'max_workers': int(os.getenv('MAX_WORKERS', '4')),
            'mock_snowflake': os.getenv('MOCK_SNOWFLAKE', 'false').lower() == 'true',
        }
        
        # Paramètres de journalisation
        self.logging = {
            'level': os.getenv('LOG_LEVEL', 'INFO'),
            'file': self.logs_dir / "pipeline.log",
            'max_size': int(os.getenv('LOG_MAX_SIZE', '10485760')),  # 10MB par défaut
            'backup_count': int(os.getenv('LOG_BACKUP_COUNT', '5'))
        }
    
    def _load_configs(self):
        """Charge les fichiers de configuration"""
        self.validation_rules = self._load_json_file('validation_rules.json', {})
        self.brand_configs = self._load_json_file('brand_configs.json', {'brands': []})
        self.table_schemas = self._load_json_file('table_schemas.json', {})
        
        # Configuration globale
        self.global_config = self._load_json_file('config.json', {})
    
    def _load_json_file(self, filename, default=None):
        """Charge un fichier JSON depuis le répertoire de configuration"""
        config_file = self.config_dir / filename
        try:
            if config_file.exists():
                with open(config_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                logger.warning(f"Fichier de configuration {filename} non trouvé")
                return default if default is not None else {}
        except Exception as e:
            logger.error(f"Erreur lors du chargement de {filename}: {e}")
            return default if default is not None else {}
    
    def get_table_mapping(self):
        """Retourne le mapping des noms de tables vers les tables Snowflake"""
        default_mapping = {
            "VODCatalog": "AGG_VOD_CATALOG",
            "VODCatalogExtended": "AGG_VOD_CATALOG_EXTENDED"
        }
        
        # Combiner avec la configuration personnalisée
        return {**default_mapping, **self.global_config.get('table_mapping', {})}
    
    def get_key_columns(self, table_name):
        """Retourne les colonnes clés pour une table spécifique"""
        key_map = {
            'User': ['UserId'],
            'Device': ['UserId', 'Serial'],
            'Smartcard': ['SmartcardId'],
            'Playback': ['PlaySessionID'],
            'EPG': ['broadcast_datetime', 'station_id'],
            'VODCatalog': ['external_id'],
            'VODCatalogExtended': ['external_id']
        }
        
        # Ajouter les configurations personnalisées
        custom_keys = self.global_config.get('key_columns', {})
        return custom_keys.get(table_name, key_map.get(table_name, []))
    
    def ensure_directories(self):
        """S'assure que tous les répertoires nécessaires existent"""
        dirs = [self.data_dir, self.landing_dir, self.raw_dir, 
                self.silver_dir, self.gold_dir, self.logs_dir]
        
        for directory in dirs:
            directory.mkdir(exist_ok=True, parents=True)
    
    def setup_logging(self):
        """Configure la journalisation en fonction des paramètres"""
        from logging.handlers import RotatingFileHandler
        
        # Créer le répertoire des logs s'il n'existe pas
        self.logs_dir.mkdir(exist_ok=True, parents=True)
        
        # Configuration
        handlers = []
        
        # Handler pour fichier rotatif
        file_handler = RotatingFileHandler(
            self.logging['file'],
            maxBytes=self.logging['max_size'],
            backupCount=self.logging['backup_count'],
            encoding='utf-8'
        )
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        handlers.append(file_handler)
        
        # Handler pour console
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        handlers.append(console_handler)
        
        # Configurer le logger racine
        logging.basicConfig(
            level=getattr(logging, self.logging['level']),
            handlers=handlers
        )
        
        logger.info(f"Journalisation configurée avec niveau {self.logging['level']}")
        return logging.getLogger()

# Instancier la configuration (singleton)
config = Config()

# Décorateur pour mettre en cache les fonctions de configuration
def cached_config(func):
    """Décore une fonction pour mettre en cache ses résultats"""
    @functools.lru_cache(maxsize=32)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper