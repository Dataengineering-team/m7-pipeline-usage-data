#!/usr/bin/env python3
"""
Utilitaires pour la manipulation des fichiers et des chemins
"""
import os
import json
import logging
import shutil
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

def ensure_dir_exists(directory_path):
    """
    S'assure qu'un répertoire existe, le crée si nécessaire
    
    Args:
        directory_path: Chemin du répertoire à vérifier/créer
    
    Returns:
        Path: Chemin absolu du répertoire
    """
    dir_path = Path(directory_path)
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path.absolute()

def get_files_by_extension(directory_path, extension, recursive=False):
    """
    Récupère tous les fichiers avec une extension spécifique dans un répertoire
    
    Args:
        directory_path: Chemin du répertoire à scanner
        extension: Extension à rechercher (ex: '.csv', '.parquet')
        recursive: Si True, cherche également dans les sous-répertoires
    
    Returns:
        list: Liste des chemins de fichiers trouvés
    """
    dir_path = Path(directory_path)
    if not dir_path.exists():
        logger.warning(f"Le répertoire {directory_path} n'existe pas")
        return []
    
    pattern = f"**/*{extension}" if recursive else f"*{extension}"
    return list(dir_path.glob(pattern))

def extract_date_from_filename(filename, pattern="*_YYYYMMDD.*"):
    """
    Extrait une date d'un nom de fichier selon un pattern
    
    Args:
        filename: Nom du fichier
        pattern: Pattern avec YYYYMMDD qui indique où se trouve la date
    
    Returns:
        str: Date au format YYYYMMDD ou None si non trouvée
    """
    try:
        filename = str(filename)
        parts = pattern.split("YYYYMMDD")
        
        if len(parts) != 2:
            return None
        
        prefix, suffix = parts
        prefix = prefix.replace("*", "")
        suffix = suffix.replace("*", "")
        
        if prefix and not filename.startswith(prefix):
            return None
        
        if suffix and not filename.endswith(suffix):
            return None
        
        # Extraire la partie entre prefix et suffix
        start = len(prefix) if prefix else 0
        end = -len(suffix) if suffix else None
        
        middle = filename[start:end]
        
        # Chercher une sous-chaîne de 8 chiffres
        for i in range(len(middle) - 7):
            candidate = middle[i:i+8]
            if candidate.isdigit():
                return candidate
        
        return None
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction de la date du fichier {filename}: {e}")
        return None

def safe_copy(source, destination, overwrite=False):
    """
    Copie un fichier de manière sécurisée
    
    Args:
        source: Chemin du fichier source
        destination: Chemin de destination
        overwrite: Si True, écrase le fichier de destination s'il existe
    
    Returns:
        bool: True si succès, False sinon
    """
    try:
        src_path = Path(source)
        dst_path = Path(destination)
        
        # Vérifier que la source existe
        if not src_path.exists():
            logger.error(f"Le fichier source {source} n'existe pas")
            return False
        
        # Créer le répertoire de destination si nécessaire
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Vérifier si la destination existe déjà
        if dst_path.exists() and not overwrite:
            logger.warning(f"La destination {destination} existe déjà et overwrite=False")
            return False
        
        # Copier le fichier
        shutil.copy2(src_path, dst_path)
        logger.info(f"Fichier copié avec succès de {source} vers {destination}")
        return True
    except Exception as e:
        logger.error(f"Erreur lors de la copie de {source} vers {destination}: {e}")
        return False

def save_json(data, file_path, pretty=True):
    """
    Sauvegarde des données au format JSON
    
    Args:
        data: Données à sauvegarder
        file_path: Chemin du fichier de destination
        pretty: Si True, formate le JSON pour une meilleure lisibilité
    
    Returns:
        bool: True si succès, False sinon
    """
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            if pretty:
                json.dump(data, f, indent=2, ensure_ascii=False)
            else:
                json.dump(data, f, ensure_ascii=False)
        logger.info(f"Données JSON sauvegardées avec succès dans {file_path}")
        return True
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde JSON dans {file_path}: {e}")
        return False

def load_json(file_path, default=None):
    """
    Charge des données depuis un fichier JSON
    
    Args:
        file_path: Chemin du fichier JSON
        default: Valeur par défaut si le chargement échoue
    
    Returns:
        Données chargées ou valeur par défaut en cas d'échec
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Erreur lors du chargement JSON depuis {file_path}: {e}")
        return default if default is not None else {}

def get_subdirectories(directory_path):
    """
    Récupère la liste des sous-répertoires d'un répertoire
    
    Args:
        directory_path: Chemin du répertoire parent
    
    Returns:
        list: Liste des chemins des sous-répertoires
    """
    dir_path = Path(directory_path)
    if not dir_path.exists():
        logger.warning(f"Le répertoire {directory_path} n'existe pas")
        return []
    
    return [d for d in dir_path.iterdir() if d.is_dir()]

def delete_old_files(directory_path, days_to_keep, extension=None):
    """
    Supprime les fichiers plus anciens qu'un certain nombre de jours
    
    Args:
        directory_path: Répertoire contenant les fichiers
        days_to_keep: Nombre de jours à conserver
        extension: Si spécifié, ne supprime que les fichiers avec cette extension
    
    Returns:
        int: Nombre de fichiers supprimés
    """
    try:
        dir_path = Path(directory_path)
        if not dir_path.exists():
            logger.warning(f"Le répertoire {directory_path} n'existe pas")
            return 0
        
        now = datetime.now()
        count = 0
        
        # Filtrer par extension si spécifiée
        pattern = f"**/*{extension}" if extension else "**/*"
        
        for file_path in dir_path.glob(pattern):
            if file_path.is_file():
                file_age = (now - datetime.fromtimestamp(file_path.stat().st_mtime)).days
                if file_age > days_to_keep:
                    file_path.unlink()
                    count += 1
                    logger.debug(f"Supprimé ancien fichier: {file_path}")
        
        logger.info(f"Supprimé {count} fichiers anciens dans {directory_path}")
        return count
    except Exception as e:
        logger.error(f"Erreur lors de la suppression de fichiers anciens dans {directory_path}: {e}")
        return 0