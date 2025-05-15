# Guide d'implémentation ETL Pipeline avec MWAA

Ce document fournit les étapes détaillées pour déployer votre pipeline ETL en utilisant AWS Managed Workflows for Apache Airflow (MWAA) pour l'orchestration.

## Prérequis

1. Un AWS Account avec les permissions nécessaires pour créer/gérer les services suivants :
   - Amazon S3
   - AWS Lambda
   - AWS Glue
   - MWAA (Managed Workflows for Apache Airflow)
   - Amazon CloudWatch
   - AWS IAM

2. AWS CLI configuré ou AWS Console accessible
3. Les buckets S3 existants :
   - `m7-etl-data-solocco-test` (données ETL)
   - `m7-solocco-code-repository-test` (code et artefacts)

4. Un VPC correctement configuré avec au moins deux sous-réseaux privés pour MWAA
   - Les sous-réseaux doivent avoir accès à Internet via un NAT Gateway

## Étape 1 : Préparation des fichiers de code et configuration

1. **Préparer les fichiers Lambda**

   Créez les archives ZIP des fonctions Lambda suivantes dans votre environnement local :
   
   - `raw_to_silver.zip` - Lambda qui transforme les données RAW en SILVER
   - `silver_to_gold.zip` - Lambda qui transforme les données SILVER en GOLD
   
   Chaque archive doit contenir le code source Python et toutes les dépendances locales.

2. **Préparer le script Glue**

   Créez le script `gold_to_snowflake.py` pour le job Glue qui chargera les données GOLD dans Snowflake.

3. **Préparer le layer Lambda**
   
   Créez une archive `etl-dependencies.zip` contenant les bibliothèques Python requises par vos fonctions Lambda.

4. **Préparer les fichiers Airflow**

   Créez un répertoire `dags` contenant votre DAG Airflow (`m7_etl_pipeline.py`) et un fichier `requirements.txt` avec les dépendances.

## Étape 2 : Télécharger les fichiers dans S3

Téléchargez tous les fichiers nécessaires dans les buckets S3 appropriés :

```bash
# Téléchargement des archives Lambda
aws s3 cp raw_to_silver.zip s3://m7-solocco-code-repository-test/lambda/
aws s3 cp silver_to_gold.zip s3://m7-solocco-code-repository-test/lambda/

# Téléchargement du script Glue
aws s3 cp gold_to_snowflake.py s3://m7-solocco-code-repository-test/glue/

# Téléchargement du layer de dépendances
aws s3 cp etl-dependencies.zip s3://m7-solocco-code-repository-test/layers/

# Téléchargement des fichiers Airflow
aws s3 cp m7_etl_pipeline.py s3://m7-solocco-code-repository-test/dags/
aws s3 cp requirements.txt s3://m7-solocco-code-repository-test/requirements/
```

## Étape 3 : Configuration des ressources d'infrastructure

Pour déployer l'infrastructure en utilisant CloudFormation, suivez ces étapes :

1. **Préparer les informations du VPC**

   Assurez-vous d'avoir les valeurs suivantes disponibles :
   - ID du VPC
   - IDs des sous-réseaux privés (au moins deux)
   
   Ces valeurs doivent être exportées par une stack CloudFormation existante avec les noms :
   - `VPC` pour l'ID du VPC
   - `PrivateSubnet1` et `PrivateSubnet2` pour les sous-réseaux

   Si ces exports n'existent pas, vous devrez modifier le template CloudFormation.

2. **Déployer la stack CloudFormation**

   Utilisez AWS Console ou AWS CLI pour déployer la stack CloudFormation :

   ```bash
   aws cloudformation create-stack \
     --stack-name m7-etl-pipeline-mwaa \
     --template-body file://etl-pipeline-mwaa.yaml \
     --capabilities CAPABILITY_IAM \
     --parameters \
       ParameterKey=SnowflakePassword,ParameterValue=VotreMotDePasse
   ```

   Note : Remplacez `VotreMotDePasse` par le mot de passe Snowflake réel.

## Étape 4 : Configuration d'Airflow et validation

Une fois la stack CloudFormation déployée avec succès, suivez ces étapes :

1. **Accéder à l'interface web d'Airflow**

   L'URL de l'interface web est disponible dans la section Outputs de la stack CloudFormation.

2. **Configurer les variables Airflow**

   Dans l'interface web d'Airflow, allez dans Admin > Variables et définissez les variables suivantes :
   
   - `environment` : `test` (ou votre environnement)
   - `max_days_back` : `3` (ou votre valeur préférée)
   - `error_threshold` : `5.0`
   - `warning_threshold` : `10.0`

3. **Configurer les connexions AWS**

   Dans Admin > Connections, vérifiez que la connexion `aws_default` est correctement configurée.

4. **Tester le pipeline**

   Déclenchez manuellement le DAG pour vérifier que tout fonctionne correctement :
   
   ```bash
   # Utiliser la CLI MWAA pour déclencher le DAG
   aws mwaa create-cli-token --name m7-etl-mwaa-test
   
   # Utiliser le token pour exécuter la commande
   aws mwaa execute-cli-command \
     --name m7-etl-mwaa-test \
     --cli-token <TOKEN> \
     --cli-command "dags trigger m7_etl_pipeline"
   ```

## Étape 5 : Configuration des déclencheurs S3

Pour que les nouveaux fichiers déposés dans le bucket S3 déclenchent automatiquement le pipeline :

1. **Configurer les notifications d'événements S3**

   ```bash
   aws s3api put-bucket-notification-configuration \
     --bucket m7-etl-data-solocco-test \
     --notification-configuration '{
       "LambdaFunctionConfigurations": [
         {
           "LambdaFunctionArn": "<ARN_FONCTION_LAMBDA>",
           "Events": ["s3:ObjectCreated:*"],
           "Filter": {
             "Key": {
               "FilterRules": [
                 {
                   "Name": "suffix",
                   "Value": ".csv"
                 }
               ]
             }
           }
         }
       ]
     }'
   ```

   Remplacez `<ARN_FONCTION_LAMBDA>` par l'ARN de la fonction Lambda S3EventLambdaFunction.

## Étape 6 : Surveillance et maintenance

1. **Tableau de bord CloudWatch**

   Un tableau de bord CloudWatch a été créé pour surveiller les métriques clés du pipeline. L'URL est disponible dans les sorties de la stack CloudFormation.

2. **Journaux**

   Les journaux sont disponibles dans CloudWatch Logs, avec des groupes de journaux séparés pour :
   - Fonctions Lambda
   - Job Glue
   - Environnement MWAA

3. **Maintenance**

   Un job de maintenance est automatiquement exécuté quotidiennement pour nettoyer les anciens fichiers selon la politique de rétention configurée.

## Dépannage

### Problèmes courants et solutions

1. **Échec de déploiement CloudFormation**
   - Vérifiez que les exports du VPC existent
   - Vérifiez les permissions IAM

2. **Échec de l'exécution des tâches Lambda**
   - Vérifiez les journaux CloudWatch
   - Assurez-vous que les fichiers S3 sont accessibles
   - Vérifiez les permissions IAM

3. **Problèmes de connexion Snowflake**
   - Vérifiez les identifiants Snowflake
   - Assurez-vous que le VPC et les groupes de sécurité permettent la connectivité sortante

4. **Problèmes MWAA**
   - Vérifiez les paramètres réseau
   - Vérifiez que requirements.txt est correctement formaté
   - Examinez les journaux du planificateur et du travailleur Airflow

## Annexe : Structure du pipeline de données

```
S3 (m7-etl-data-solocco-test)
│
├── landing/         # Fichiers CSV entrants
│
├── raw/             # Données brutes organisées par date (YYYYMMDD)
│   ├── 20250501/
│   └── 20250502/
│
├── silver/          # Données nettoyées et validées
│   ├── 20250501/
│   └── 20250502/
│
├── gold/            # Données agrégées prêtes pour analyse
│   ├── Device_20250501.parquet
│   ├── EPG_20250501.parquet
│   └── ...
│
└── logs/            # Journaux et informations de suivi
    ├── pipeline_execution_20250501.json
    └── ...
```
s3://m7-solocco-code-repository-test/
├── lambda/
│   ├── raw_to_silver.zip
│   └── silver_to_gold.zip
├── glue/
│   └── gold_to_snowflake.py
├── layers/
│   └── etl-dependencies.zip
├── dags/
│   └── m7_etl_pipeline.py
└── requirements/
    └── requirements.txt

## Prochaines étapes

1. **Test de charge** - Validez le pipeline avec un volume de données représentatif
2. **Documentation des procédures opérationnelles** - Créez des runbooks pour les opérations quotidiennes
3. **Mise en place d'alertes** - Configurez des alertes pour les événements critiques
4. **Tests de reprise après sinistre** - Validez les procédures de reprise