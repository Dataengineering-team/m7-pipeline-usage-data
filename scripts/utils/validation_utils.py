"""
Utilitaires pour la validation des données selon des règles externes
"""
import json
import os
import logging
import pandas as pd
import numpy as np
from pathlib import Path

logger = logging.getLogger(__name__)

def load_validation_rules(config_path=None):
    """
    Charge les règles de validation depuis un fichier JSON
    
    Args:
        config_path: Chemin vers le fichier de règles de validation
                     Si None, utilise le chemin par défaut
    
    Returns:
        dict: Règles de validation par table
    """
    try:
        # Chemin par défaut si non spécifié
        if config_path is None:
            # Obtenir le chemin du répertoire du script
            base_dir = Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
            config_path = base_dir / "config" / "validation_rules.json"
        
        # Charger le fichier JSON
        with open(config_path, 'r', encoding='utf-8') as f:
            rules = json.load(f)
        
        logger.info(f"Règles de validation chargées depuis {config_path}")
        return rules
    except Exception as e:
        logger.error(f"Erreur lors du chargement des règles de validation: {e}")
        # Retourner un dictionnaire vide en cas d'erreur
        return {}

def apply_condition(df, condition_str):
    """
    Applique une condition exprimée en chaîne de caractères à un DataFrame
    
    Args:
        df: DataFrame pandas
        condition_str: Chaîne de caractères représentant une condition
                      Ex: "column1 > column2" ou "column == value"
    
    Returns:
        Series: Masque booléen indiquant si chaque ligne satisfait la condition
    """
    try:
        # Remplacer les opérateurs textuels par leurs équivalents Python
        condition_map = {
            "AND": "&",
            "OR": "|",
            "NOT": "~",
            "IS NULL": ".isna()",
            "IS NOT NULL": ".notna()"
        }
        
        for text, op in condition_map.items():
            condition_str = condition_str.replace(text, op)
        
        # Évaluer la condition
        return df.eval(condition_str)
    except Exception as e:
        logger.error(f"Erreur lors de l'application de la condition '{condition_str}': {e}")
        # En cas d'erreur, retourner un masque qui ne sélectionne aucune ligne
        return pd.Series([False] * len(df))

def validate_with_rules(df, table_name, rules):
    """
    Valide un DataFrame selon les règles définies pour une table spécifique
    
    Args:
        df: DataFrame pandas à valider
        table_name: Nom de la table (pour sélectionner les règles appropriées)
        rules: Dictionnaire des règles de validation
    
    Returns:
        tuple: (errors, error_rows)
               - errors: Dictionnaire des erreurs détectées
               - error_rows: Liste des indices des lignes en erreur
    """
    errors = {}
    error_rows = []
    
    # Si aucune règle pour cette table, ou pas de règles du tout, retourner vide
    if not rules or table_name not in rules:
        return errors, error_rows
    
    table_rules = rules[table_name]
    
    # Parcourir chaque règle pour cette table
    for rule_type, rule_config in table_rules.items():
        error_type = rule_config.get('error_type', rule_type)
        
        try:
            # Traitement selon le type de règle
            if rule_type == 'valid_types' and 'column' in rule_config and 'allowed_values' in rule_config:
                # Validation des valeurs autorisées
                column = rule_config['column']
                allowed_values = rule_config['allowed_values']
                
                if column in df.columns:
                    invalid = df[~df[column].isin(allowed_values) & ~df[column].isna()]
                    if not invalid.empty:
                        errors[error_type] = invalid[column].unique().tolist()
                        error_rows.extend(invalid.index.tolist())
            
            elif rule_type == 'required_fields' and 'columns' in rule_config:
                # Validation des champs requis
                required_columns = rule_config['columns']
                missing_fields = {}
                
                for column in required_columns:
                    if column in df.columns:
                        missing = df[df[column].isna() | (df[column] == '')]
                        if not missing.empty:
                            missing_fields[column] = len(missing)
                            error_rows.extend(missing.index.tolist())
                
                if missing_fields:
                    errors[error_type] = missing_fields
            
            elif rule_type == 'uniqueness' and 'columns' in rule_config:
                # Validation de l'unicité
                uniqueness_columns = rule_config['columns']
                # Vérifier que toutes les colonnes existent
                if all(col in df.columns for col in uniqueness_columns):
                    duplicates = df[df.duplicated(subset=uniqueness_columns, keep=False)]
                    if not duplicates.empty:
                        # Collecter les valeurs dupliquées
                        duplicate_values = []
                        for _, row in duplicates.drop_duplicates(subset=uniqueness_columns).iterrows():
                            values = [row[col] for col in uniqueness_columns]
                            duplicate_values.append("-".join(str(v) for v in values))
                        
                        errors[error_type] = duplicate_values
                        error_rows.extend(duplicates.index.tolist())
            
            elif 'condition' in rule_config:
                # Validation basée sur une condition
                condition = rule_config['condition']
                # Appliquer la condition
                result = apply_condition(df, condition)
                invalid = df[~result]
                
                if not invalid.empty:
                    errors[error_type] = len(invalid)
                    error_rows.extend(invalid.index.tolist())
            
            elif 'column' in rule_config and 'not_empty' in rule_config and rule_config['not_empty']:
                # Validation qu'une colonne n'est pas vide
                column = rule_config['column']
                if column in df.columns:
                    empty = df[df[column].isna() | (df[column].astype(str) == '')]
                    if not empty.empty:
                        errors[error_type] = len(empty)
                        error_rows.extend(empty.index.tolist())
        
        except Exception as e:
            logger.error(f"Erreur lors de l'application de la règle '{rule_type}' pour la table '{table_name}': {e}")
    
    # Retourner des résultats uniques et triés
    error_rows = sorted(list(set(error_rows)))
    return errors, error_rows