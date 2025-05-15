Compress-Archive -Path "C:\Users\abidine.mouliom\Documents\GitHub\m7-pipeline-usage-local\scripts\deploiement\raw_to_silver.py" `
                 -DestinationPath "C:\Users\abidine.mouliom\Documents\GitHub\m7-pipeline-usage-local\scripts\deploiement\build\raw_to_silver.zip" `
                 -Force




Compress-Archive -Path "scripts/deploiement/silver_to_gold.py" `
                 -DestinationPath "scripts/deploiement/build/silver_to_gold.zip" `
                 -Force





# Uploader les packages Lambda vers S3
aws s3 cp scripts/deploiement/build/raw_to_silver.zip s3://m7-solocco-code-repository-test/lambda/
aws s3 cp scripts/deploiement/build/silver_to_gold.zip s3://m7-solocco-code-repository-test/lambda/

# Uploader le script Glue vers S3
aws s3 cp scripts/deploiement/gold_to_snowflake.py s3://m7-solocco-code-repository-test/glue/

# Uploader la couche Lambda vers S3
aws s3 cp scripts/deploiement/build/etl-dependencies.zip s3://m7-solocco-code-repository-test/layers/

# Uploader les fichiers de configuration vers S3
aws s3 cp config/brand_configs.json s3://m7-solocco-code-repository-test/config/
aws s3 cp config/table_schemas.json s3://m7-solocco-code-repository-test/config/
aws s3 cp config/validation_rules.json s3://m7-solocco-code-repository-test/config/




# Déployer le stack CloudFormation
aws cloudformation deploy `
  --template-file scripts/deploiement/step_function/cloudformation_deploi.yaml `
  --stack-name m7-etl-solocco-test `
  --parameter-overrides `
    Environment=test `
    CodeBucketName=m7-solocco-code-repository-test `
    DataBucketName=m7-etl-data-solocco-test `
    SnowflakeUser=AWS_SNOWFLAKE `
    SnowflakePassword=Qazwsxedcrfv@007 `
    SnowflakeAccount=eb15765.eu-west-1 `
    SnowflakeWarehouse=DATA_ANALYTICS_WAREHOUSE `
    SnowflakeDatabase=M7_DB_STG_DEV `
    SnowflakeSchema=STG_SG `
    SnowflakeMonitoringSchema=STG_SG_MONITORING `
  --capabilities CAPABILITY_IAM

