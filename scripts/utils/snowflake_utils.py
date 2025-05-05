#!/usr/bin/env python3
"""
Utilitaires pour les opérations Snowflake
Ce module contient des fonctions utilitaires pour faciliter l'interaction avec Snowflake
"""
import io
import csv
import json
import logging
import pandas as pd
import snowflake.connector
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional, Union

logger = logging.getLogger(__name__)

class CustomJSONEncoder(json.JSONEncoder):
    """Encodeur JSON personnalisé pour gérer les types non-sérialisables"""
    def default(self, obj):
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        if isinstance(obj, (pd.Series, pd.DataFrame)):
            return obj.to_dict()
        try:
            return json.JSONEncoder.default(self, obj)
        except TypeError:
            return str(obj)

def create_snowflake_connection(user: str, password: str, account: str, 
                               warehouse: str, database: str, schema: str) -> snowflake.connector.SnowflakeConnection:
    """
    Crée une connexion à Snowflake
    
    Args:
        user: Nom d'utilisateur Snowflake
        password: Mot de passe
        account: Compte Snowflake
        warehouse: Entrepôt de données à utiliser
        database: Base de données
        schema: Schéma
        
    Returns:
        La connexion Snowflake établie
    """
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema
        )
        logger.info(f"Connexion établie à Snowflake: {database}.{schema}")
        return conn
    except Exception as e:
        logger.error(f"Erreur lors de la connexion à Snowflake: {e}")
        raise

def get_table_columns(conn: snowflake.connector.SnowflakeConnection, schema: str, table: str) -> List[str]:
    """
    Récupère la liste des colonnes d'une table Snowflake
    
    Args:
        conn: Connexion Snowflake
        schema: Schéma de la table
        table: Nom de la table
        
    Returns:
        Liste des noms de colonnes
    """
    try:
        cursor = conn.cursor()
        cursor.execute(f"DESCRIBE TABLE {schema}.{table}")
        columns = [row[0].upper() for row in cursor.fetchall()]
        cursor.close()
        return columns
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des colonnes de {schema}.{table}: {e}")
        raise

def get_table_primary_keys(conn: snowflake.connector.SnowflakeConnection, schema: str, table: str) -> List[str]:
    """
    Récupère les clés primaires d'une table Snowflake
    
    Args:
        conn: Connexion Snowflake
        schema: Schéma de la table
        table: Nom de la table
        
    Returns:
        Liste des noms de colonnes qui constituent la clé primaire
    """
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
          ON tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
          AND tc.TABLE_SCHEMA = '{schema}'
          AND tc.TABLE_NAME = '{table}'
        """)
        primary_keys = [row[0].upper() for row in cursor.fetchall()]
        cursor.close()
        return primary_keys
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des clés primaires de {schema}.{table}: {e}")
        return []

def load_dataframe_to_snowflake(conn: snowflake.connector.SnowflakeConnection, df: pd.DataFrame, 
                              schema: str, table: str) -> Tuple[int, int, int]:
    """
    Charge un DataFrame pandas dans une table Snowflake existante
    
    Args:
        conn: Connexion Snowflake
        df: DataFrame pandas à charger
        schema: Schéma de la table cible
        table: Nom de la table cible
        
    Returns:
        Tuple (lignes_insérées, lignes_mises_à_jour, lignes_rejetées)
    """
    try:
        cursor = conn.cursor()
        
        # Récupérer les colonnes de la table cible
        table_columns = get_table_columns(conn, schema, table)
        
        # Filtrer le DataFrame pour ne conserver que les colonnes existantes
        columns_to_use = [col for col in df.columns if col.upper() in table_columns]
        df_filtered = df[columns_to_use]
        
        # Convertir en CSV
        csv_buffer = io.StringIO()
        df_filtered.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_NONNUMERIC)
        csv_buffer.seek(0)
        
        # Créer une étape temporaire
        stage_name = f"TEMP_STAGE_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        cursor.execute(f"CREATE TEMPORARY STAGE {stage_name}")
        
        # Charger les données dans l'étape
        cursor.execute(f"PUT file://{csv_buffer} @{stage_name}")
        
        # Charger les données dans la table
        columns_str = ", ".join([f'"{col.upper()}"' for col in columns_to_use])
        copy_sql = f"""
        COPY INTO {schema}.{table} ({columns_str})
        FROM @{stage_name}
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
        ON_ERROR = 'CONTINUE'
        """
        cursor.execute(copy_sql)
        
        # Récupérer les résultats
        result = cursor.fetchall()
        rows_loaded = sum(int(row[3]) for row in result) if result else 0
        errors = sum(int(row[4]) for row in result) if result else 0
        
        # Nettoyer
        cursor.execute(f"DROP STAGE IF EXISTS {stage_name}")
        cursor.close()
        
        return rows_loaded, 0, errors  # rows_updated toujours à 0 car pas de distinction avec COPY
    
    except Exception as e:
        logger.error(f"Erreur lors du chargement des données dans {schema}.{table}: {e}")
        if 'cursor' in locals():
            cursor.close()
        return 0, 0, len(df)

def execute_query(conn: snowflake.connector.SnowflakeConnection, query: str, 
                 params: Optional[Dict[str, Any]] = None) -> List[Dict]:
    """
    Exécute une requête SQL et retourne les résultats
    
    Args:
        conn: Connexion Snowflake
        query: Requête SQL à exécuter
        params: Paramètres à remplacer dans la requête
        
    Returns:
        Liste de dictionnaires représentant les résultats
    """
    try:
        cursor = conn.cursor(snowflake.connector.DictCursor)
        cursor.execute(query, params if params else {})
        results = cursor.fetchall()
        cursor.close()
        return results
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution de la requête: {e}")
        raise

def insert_log_entry(conn: snowflake.connector.SnowflakeConnection, schema: str, table: str, log_data: Dict[str, Any]) -> bool:
    """
    Insère une entrée de log dans une table Snowflake
    
    Args:
        conn: Connexion Snowflake
        schema: Schéma de la table de logs
        table: Nom de la table de logs
        log_data: Dictionnaire de données à insérer
        
    Returns:
        True si l'insertion a réussi, False sinon
    """
    try:
        cursor = conn.cursor()
        
        # Construire les listes de colonnes et valeurs
        columns = []
        values = []
        placeholders = []
        
        for key, value in log_data.items():
            columns.append(key)
            
            if isinstance(value, dict):
                values.append(json.dumps(value, cls=CustomJSONEncoder))
                placeholders.append("PARSE_JSON(%s)")
            else:
                values.append(value)
                placeholders.append("%s")
        
        # Construire la requête
        columns_str = ", ".join(columns)
        placeholders_str = ", ".join(placeholders)
        
        query = f"INSERT INTO {schema}.{table} ({columns_str}) VALUES ({placeholders_str})"
        
        # Exécuter l'insertion
        cursor.execute(query, values)
        conn.commit()
        
        cursor.close()
        return True
        
    except Exception as e:
        logger.error(f"Erreur lors de l'insertion du log dans {schema}.{table}: {e}")
        if 'cursor' in locals():
            cursor.close()
        return False

def check_table_exists(conn: snowflake.connector.SnowflakeConnection, schema: str, table: str) -> bool:
    """
    Vérifie si une table existe dans Snowflake
    
    Args:
        conn: Connexion Snowflake
        schema: Schéma de la table
        table: Nom de la table
        
    Returns:
        True si la table existe, False sinon
    """
    try:
        cursor = conn.cursor()
        cursor.execute(f"SHOW TABLES LIKE '{table}' IN {schema}")
        result = cursor.fetchone()
        cursor.close()
        return result is not None
    except Exception as e:
        logger.error(f"Erreur lors de la vérification de l'existence de la table {schema}.{table}: {e}")
        return False

def get_table_row_count(conn: snowflake.connector.SnowflakeConnection, schema: str, table: str) -> int:
    """
    Récupère le nombre de lignes d'une table Snowflake
    
    Args:
        conn: Connexion Snowflake
        schema: Schéma de la table
        table: Nom de la table
        
    Returns:
        Nombre de lignes dans la table
    """
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    except Exception as e:
        logger.error(f"Erreur lors de la récupération du nombre de lignes de {schema}.{table}: {e}")
        return -1

def get_table_data(conn: snowflake.connector.SnowflakeConnection, schema: str, table: str, 
                  columns: Optional[List[str]] = None, limit: int = 1000) -> pd.DataFrame:
    """
    Récupère les données d'une table Snowflake dans un DataFrame
    
    Args:
        conn: Connexion Snowflake
        schema: Schéma de la table
        table: Nom de la table
        columns: Liste des colonnes à récupérer (toutes si None)
        limit: Nombre maximum de lignes à récupérer
        
    Returns:
        DataFrame pandas contenant les données
    """
    try:
        cols_str = "*" if columns is None else ", ".join(columns)
        query = f"SELECT {cols_str} FROM {schema}.{table} LIMIT {limit}"
        
        # Utiliser pandas pour récupérer directement un DataFrame
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des données de {schema}.{table}: {e}")
        return pd.DataFrame()

def create_temp_table(conn: snowflake.connector.SnowflakeConnection, temp_table: str, 
                     schema: str, source_table: str) -> bool:
    """
    Crée une table temporaire basée sur une table existante
    
    Args:
        conn: Connexion Snowflake
        temp_table: Nom de la table temporaire à créer
        schema: Schéma de la table source
        source_table: Nom de la table source
        
    Returns:
        True si la création a réussi, False sinon
    """
    try:
        cursor = conn.cursor()
        cursor.execute(f"CREATE TEMPORARY TABLE {temp_table} LIKE {schema}.{source_table}")
        cursor.close()
        return True
    except Exception as e:
        logger.error(f"Erreur lors de la création de la table temporaire {temp_table}: {e}")
        return False

def snowflake_batch_update(conn: snowflake.connector.SnowflakeConnection, schema: str, 
                          table: str, updates: List[Dict[str, Any]], 
                          key_columns: List[str]) -> Tuple[int, int]:
    """
    Effectue une mise à jour par lots dans une table Snowflake
    
    Args:
        conn: Connexion Snowflake
        schema: Schéma de la table
        table: Nom de la table
        updates: Liste de dictionnaires contenant les valeurs à mettre à jour
        key_columns: Liste des colonnes constituant la clé pour l'identification des lignes
        
    Returns:
        Tuple (lignes_mises_à_jour, erreurs)
    """
    if not updates:
        return 0, 0
    
    try:
        # Créer un DataFrame à partir des mises à jour
        df = pd.DataFrame(updates)
        
        # Créer une table temporaire pour charger les mises à jour
        temp_table = f"TEMP_UPDATES_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        create_temp_table(conn, temp_table, schema, table)
        
        # Charger les mises à jour dans la table temporaire
        cursor = conn.cursor()
        
        # Convertir en CSV
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_NONNUMERIC)
        csv_buffer.seek(0)
        
        # Créer une étape temporaire
        stage_name = f"TEMP_STAGE_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        cursor.execute(f"CREATE TEMPORARY STAGE {stage_name}")
        
        # Charger les données dans l'étape
        cursor.execute(f"PUT file://{csv_buffer} @{stage_name}")
        
        # Charger les données dans la table temporaire
        columns_str = ", ".join([f'"{col.upper()}"' for col in df.columns])
        copy_sql = f"""
        COPY INTO {temp_table} ({columns_str})
        FROM @{stage_name}
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
        ON_ERROR = 'CONTINUE'
        """
        cursor.execute(copy_sql)
        
        # Construire la condition de jointure pour le MERGE
        join_condition = " AND ".join([f"t.{key} = s.{key}" for key in key_columns])
        
        # Construire la clause SET pour la mise à jour
        update_columns = [col for col in df.columns if col not in key_columns]
        set_clause = ", ".join([f"t.{col} = s.{col}" for col in update_columns])
        
        # Exécuter le MERGE
        merge_sql = f"""
        MERGE INTO {schema}.{table} t
        USING {temp_table} s
        ON {join_condition}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        """
        cursor.execute(merge_sql)
        
        # Récupérer le nombre de lignes mises à jour
        result = cursor.fetchone()
        rows_updated = result[0] if result else 0
        
        # Nettoyer les ressources temporaires
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
        cursor.execute(f"DROP STAGE IF EXISTS {stage_name}")
        
        cursor.close()
        return rows_updated, 0
        
    except Exception as e:
        logger.error(f"Erreur lors de la mise à jour par lots dans {schema}.{table}: {e}")
        if 'cursor' in locals():
            cursor.close()
        return 0, len(updates)

def snowflake_batch_insert(conn: snowflake.connector.SnowflakeConnection, schema: str, 
                          table: str, records: List[Dict[str, Any]]) -> Tuple[int, int]:
    """
    Effectue une insertion par lots dans une table Snowflake
    
    Args:
        conn: Connexion Snowflake
        schema: Schéma de la table
        table: Nom de la table
        records: Liste de dictionnaires contenant les valeurs à insérer
        
    Returns:
        Tuple (lignes_insérées, erreurs)
    """
    if not records:
        return 0, 0
    
    try:
        # Créer un DataFrame à partir des enregistrements
        df = pd.DataFrame(records)
        
        # Charger les données directement dans la table
        return load_dataframe_to_snowflake(conn, df, schema, table)
        
    except Exception as e:
        logger.error(f"Erreur lors de l'insertion par lots dans {schema}.{table}: {e}")
        return 0, len(records)

def snowflake_upsert(conn: snowflake.connector.SnowflakeConnection, schema: str, 
                    table: str, records: List[Dict[str, Any]], 
                    key_columns: List[str]) -> Tuple[int, int, int]:
    """
    Effectue une opération UPSERT (INSERT ou UPDATE) par lots dans une table Snowflake
    
    Args:
        conn: Connexion Snowflake
        schema: Schéma de la table
        table: Nom de la table
        records: Liste de dictionnaires contenant les valeurs à insérer ou mettre à jour
        key_columns: Liste des colonnes constituant la clé pour l'identification des lignes
        
    Returns:
        Tuple (lignes_insérées, lignes_mises_à_jour, erreurs)
    """
    if not records:
        return 0, 0, 0
    
    try:
        # Créer un DataFrame à partir des enregistrements
        df = pd.DataFrame(records)
        
        # Créer une table temporaire pour charger les enregistrements
        temp_table = f"TEMP_UPSERT_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        create_temp_table(conn, temp_table, schema, table)
        
        cursor = conn.cursor()
        
        # Convertir en CSV
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_NONNUMERIC)
        csv_buffer.seek(0)
        
        # Créer une étape temporaire
        stage_name = f"TEMP_STAGE_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        cursor.execute(f"CREATE TEMPORARY STAGE {stage_name}")
        
        # Charger les données dans l'étape
        cursor.execute(f"PUT file://{csv_buffer} @{stage_name}")
        
        # Charger les données dans la table temporaire
        columns_str = ", ".join([f'"{col.upper()}"' for col in df.columns])
        copy_sql = f"""
        COPY INTO {temp_table} ({columns_str})
        FROM @{stage_name}
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
        ON_ERROR = 'CONTINUE'
        """
        cursor.execute(copy_sql)
        
        # Construire la condition de jointure pour le MERGE
        join_condition = " AND ".join([f"t.{key} = s.{key}" for key in key_columns])
        
        # Construire la clause SET pour la mise à jour
        update_columns = [col for col in df.columns if col not in key_columns]
        set_clause = ", ".join([f"t.{col} = s.{col}" for col in update_columns])
        
        # Construire la clause INSERT
        columns = ", ".join([f"{col}" for col in df.columns])
        values = ", ".join([f"s.{col}" for col in df.columns])
        
        # Exécuter le MERGE
        merge_sql = f"""
        MERGE INTO {schema}.{table} t
        USING {temp_table} s
        ON {join_condition}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})
        """
        cursor.execute(merge_sql)
        
        # Récupérer les résultats du MERGE
        result = cursor.fetchone()
        rows_inserted = result[0] if result else 0
        rows_updated = result[1] if result and len(result) > 1 else 0
        
        # Nettoyer les ressources temporaires
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
        cursor.execute(f"DROP STAGE IF EXISTS {stage_name}")
        
        cursor.close()
        return rows_inserted, rows_updated, 0
        
    except Exception as e:
        logger.error(f"Erreur lors de l'UPSERT par lots dans {schema}.{table}: {e}")
        if 'cursor' in locals():
            cursor.close()
        return 0, 0, len(records)

def execute_stored_procedure(conn: snowflake.connector.SnowflakeConnection, 
                            procedure_name: str, params: List[Any] = None) -> Any:
    """
    Exécute une procédure stockée dans Snowflake
    
    Args:
        conn: Connexion Snowflake
        procedure_name: Nom complet de la procédure stockée (schéma.procédure)
        params: Liste des paramètres à passer à la procédure
        
    Returns:
        Résultat de la procédure stockée
    """
    try:
        cursor = conn.cursor()
        
        # Construire la chaîne de paramètres
        param_str = ""
        if params:
            param_values = []
            for param in params:
                if isinstance(param, str):
                    param_values.append(f"'{param}'")
                elif param is None:
                    param_values.append("NULL")
                else:
                    param_values.append(str(param))
            param_str = ", ".join(param_values)
        
        # Exécuter la procédure stockée
        sql = f"CALL {procedure_name}({param_str})"
        cursor.execute(sql)
        
        # Récupérer le résultat
        result = cursor.fetchone()
        cursor.close()
        
        return result[0] if result else None
        
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution de la procédure stockée {procedure_name}: {e}")
        if 'cursor' in locals():
            cursor.close()
        raise

def execute_multiple_statements(conn: snowflake.connector.SnowflakeConnection, 
                               statements: List[str]) -> List[bool]:
    """
    Exécute plusieurs instructions SQL dans une transaction
    
    Args:
        conn: Connexion Snowflake
        statements: Liste des instructions SQL à exécuter
        
    Returns:
        Liste de booléens indiquant si chaque instruction a réussi
    """
    results = []
    cursor = conn.cursor()
    
    try:
        # Commencer une transaction
        cursor.execute("BEGIN TRANSACTION")
        
        # Exécuter chaque instruction
        for stmt in statements:
            try:
                cursor.execute(stmt)
                results.append(True)
            except Exception as e:
                logger.error(f"Erreur lors de l'exécution de l'instruction SQL: {e}")
                logger.error(f"Instruction: {stmt}")
                results.append(False)
                # Continuer malgré l'erreur
        
        # Valider la transaction si toutes les instructions ont réussi
        if all(results):
            cursor.execute("COMMIT")
        else:
            cursor.execute("ROLLBACK")
            logger.warning("Transaction annulée en raison d'erreurs")
        
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution des instructions SQL: {e}")
        # Annuler la transaction en cas d'erreur globale
        try:
            cursor.execute("ROLLBACK")
        except:
            pass
        
        # Marquer toutes les instructions comme échouées
        results = [False] * len(statements)
    
    finally:
        cursor.close()
    
    return results