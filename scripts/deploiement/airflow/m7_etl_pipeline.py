"""
M7 ETL Pipeline DAG
Ce DAG orchestre le processus ETL complet pour le traitement des données M7:
1. RAW -> SILVER (Lambda)
2. SILVER -> GOLD (Lambda)
3. GOLD -> SNOWFLAKE (Glue Job)
4. Maintenance (nettoyage des anciens fichiers)
"""

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import AwsLambdaInvokeOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable, DagRun
from airflow.utils.dates import days_ago

# Configuration par défaut pour les tâches du DAG
default_args = {
    'owner': 'M7-Data-Team',
    'depends_on_past': False,
    'email': ['data-alerts@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Environnement (récupéré des variables Airflow ou valeur par défaut)
ENVIRONMENT = Variable.get('environment', default='test')

# Noms des fonctions Lambda et du job Glue
RAW_TO_SILVER_LAMBDA = f"m7-raw-to-silver-{ENVIRONMENT}"
SILVER_TO_GOLD_LAMBDA = f"m7-silver-to-gold-{ENVIRONMENT}"
GOLD_TO_SNOWFLAKE_JOB = f"m7-gold-to-snowflake-{ENVIRONMENT}"
MAINTENANCE_LAMBDA = f"m7-etl-maintenance-{ENVIRONMENT}"

# Paramètres de configuration
MAX_DAYS_BACK = int(Variable.get('max_days_back', default='3'))

def get_date_param(**context):
    """
    Récupère le paramètre de date depuis les paramètres de configuration du DAG
    ou utilise la date d'exécution par défaut au format YYYYMMDD
    """
    try:
        # Vérifier si une date est fournie dans la configuration
        dag_run = context['dag_run']
        if dag_run and dag_run.conf and 'date' in dag_run.conf:
            return dag_run.conf['date']
        
        # Sinon, utiliser la date d'exécution
        execution_date = context['execution_date']
        return execution_date.strftime('%Y%m%d')
    except Exception as e:
        logging.error(f"Erreur lors de la récupération du paramètre de date: {str(e)}")
        # Fallback: date d'aujourd'hui
        return datetime.now().strftime('%Y%m%d')

def check_data_quality(**context):
    """
    Vérifie la qualité des données après traitement
    et décide si des alertes doivent être déclenchées
    """
    ti = context['ti']
    
    # Récupérer les résultats de l'étape GOLD -> SNOWFLAKE
    gold_to_snowflake_result = ti.xcom_pull(task_ids='gold_to_snowflake')
    
    # Vérifier les métriques de qualité des données
    if gold_to_snowflake_result and 'metrics' in gold_to_snowflake_result:
        metrics = gold_to_snowflake_result['metrics']
        
        # Seuils d'alerte pour les erreurs (à configurer via des variables Airflow)
        error_threshold = float(Variable.get('error_threshold', default='5.0'))
        warning_threshold = float(Variable.get('warning_threshold', default='10.0'))
        
        # Calcul du pourcentage d'erreur global
        total_records = metrics.get('total_records', 0)
        error_records = metrics.get('error_records', 0)
        
        if total_records > 0:
            error_percentage = (error_records / total_records) * 100
            
            # Journaliser les métriques de qualité
            logging.info(f"Métriques de qualité des données: {metrics}")
            logging.info(f"Pourcentage d'erreur: {error_percentage:.2f}%")
            
            # Vérifier si les seuils sont dépassés
            if error_percentage > error_threshold:
                logging.error(f"ALERTE QUALITÉ: Le taux d'erreur ({error_percentage:.2f}%) dépasse le seuil critique ({error_threshold}%)")
                return False
            elif error_percentage > warning_threshold:
                logging.warning(f"AVERTISSEMENT QUALITÉ: Le taux d'erreur ({error_percentage:.2f}%) dépasse le seuil d'avertissement ({warning_threshold}%)")
    
    return True

# Création du DAG
with DAG(
    'm7_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL M7 pour le traitement des données (RAW → SILVER → GOLD → SNOWFLAKE)',
    schedule_interval='0 1 * * *',  # Tous les jours à 1h du matin
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'm7', 'data-processing'],
    max_active_runs=1,
) as dag:
    
    # Tâche 1: RAW → SILVER
    raw_to_silver = AwsLambdaInvokeOperator(
        task_id='raw_to_silver',
        function_name=RAW_TO_SILVER_LAMBDA,
        payload=json.dumps({
            'date': '{{ ti.xcom_push(key="processing_date", value=task_instance.get_template_context()["execution_date"].strftime("%Y%m%d")) }}{{ task_instance.xcom_pull(key="processing_date") }}'
        }),
        aws_conn_id='aws_default',
        invocation_type='RequestResponse',
        log_type='Tail',
    )
    
    # Tâche 2: SILVER → GOLD
    silver_to_gold = AwsLambdaInvokeOperator(
        task_id='silver_to_gold',
        function_name=SILVER_TO_GOLD_LAMBDA,
        payload=json.dumps({
            'date': '{{ task_instance.xcom_pull(key="processing_date") }}'
        }),
        aws_conn_id='aws_default',
        invocation_type='RequestResponse',
        log_type='Tail',
    )
    
    # Tâche 3: GOLD → SNOWFLAKE
    gold_to_snowflake = GlueJobOperator(
        task_id='gold_to_snowflake',
        job_name=GOLD_TO_SNOWFLAKE_JOB,
        script_args={
            '--date': '{{ task_instance.xcom_pull(key="processing_date") }}'
        },
        aws_conn_id='aws_default',
        wait_for_completion=True,
        verbose=True,
    )
    
    # Tâche 4: Vérification de la qualité des données
    data_quality_check = PythonOperator(
        task_id='data_quality_check',
        python_callable=check_data_quality,
        provide_context=True,
    )
    
    # Tâche 5: Maintenance (nettoyage des anciens fichiers)
    maintenance = AwsLambdaInvokeOperator(
        task_id='maintenance',
        function_name=MAINTENANCE_LAMBDA,
        aws_conn_id='aws_default',
        invocation_type='RequestResponse',
    )
    
    # Définition des dépendances entre les tâches
    raw_to_silver >> silver_to_gold >> gold_to_snowflake >> data_quality_check >> maintenance

# DAG pour le traitement par date spécifique (déclenché par des événements S3)
with DAG(
    'm7_etl_pipeline_specific_date',
    default_args=default_args,
    description='Pipeline ETL M7 pour le traitement d\'une date spécifique (déclenché par S3)',
    schedule_interval=None,  # Triggered externally
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'm7', 'event-triggered'],
    max_active_runs=5,  # Permettre plusieurs exécutions simultanées pour différentes dates
) as specific_date_dag:
    
    # Extraction de la date à partir de la configuration
    extract_date = PythonOperator(
        task_id='extract_date',
        python_callable=get_date_param,
        provide_context=True,
    )
    
    # Tâche 1: RAW → SILVER pour une date spécifique
    raw_to_silver_specific = AwsLambdaInvokeOperator(
        task_id='raw_to_silver',
        function_name=RAW_TO_SILVER_LAMBDA,
        payload=json.dumps({
            'date': '{{ task_instance.xcom_pull(task_ids="extract_date") }}'
        }),
        aws_conn_id='aws_default',
        invocation_type='RequestResponse',
        log_type='Tail',
    )
    
    # Tâche 2: SILVER → GOLD pour une date spécifique
    silver_to_gold_specific = AwsLambdaInvokeOperator(
        task_id='silver_to_gold',
        function_name=SILVER_TO_GOLD_LAMBDA,
        payload=json.dumps({
            'date': '{{ task_instance.xcom_pull(task_ids="extract_date") }}'
        }),
        aws_conn_id='aws_default',
        invocation_type='RequestResponse',
        log_type='Tail',
    )
    
    # Tâche 3: GOLD → SNOWFLAKE pour une date spécifique
    gold_to_snowflake_specific = GlueJobOperator(
        task_id='gold_to_snowflake',
        job_name=GOLD_TO_SNOWFLAKE_JOB,
        script_args={
            '--date': '{{ task_instance.xcom_pull(task_ids="extract_date") }}'
        },
        aws_conn_id='aws_default',
        wait_for_completion=True,
        verbose=True,
    )
    
    # Tâche 4: Vérification de la qualité des données
    data_quality_check_specific = PythonOperator(
        task_id='data_quality_check',
        python_callable=check_data_quality,
        provide_context=True,
    )
    
    # Définition des dépendances entre les tâches
    extract_date >> raw_to_silver_specific >> silver_to_gold_specific >> gold_to_snowflake_specific >> data_quality_check_specific