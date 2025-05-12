# ETL Pipeline Deployment Instructions on AWS

This guide will help you quickly deploy your existing ETL pipeline on AWS using CloudFormation. The entire process takes approximately 2-4 hours, depending on your familiarity with AWS.

## Prerequisites

1. An AWS account with the necessary permissions to create the following resources:
   - S3
   - Lambda
   - IAM Roles & Policies
   - Glue
   - Step Functions
   - CloudWatch
   - EventBridge

2. AWS CLI installed and configured on your machine

3. The source code of your current ETL pipeline

## Step 1: Preparing Deployment Packages (45-60 minutes)

### Create Lambda Packages

1. **Create the raw_to_silver package**:

```bash
mkdir -p build/raw_to_silver
cp scripts/raw_to_silver.py build/raw_to_silver/
cp -r scripts/utils build/raw_to_silver/
cd build/raw_to_silver
pip install -t . pandas pyarrow dotenv
zip -r ../raw_to_silver.zip .
cd ../..
```

2. **Create the silver_to_gold package**:

```bash
mkdir -p build/silver_to_gold
cp scripts/silver_to_gold.py build/silver_to_gold/
cp -r scripts/utils build/silver_to_gold/
cd build/silver_to_gold
pip install -t . pandas pyarrow dotenv
zip -r ../silver_to_gold.zip .
cd ../..
```

3. **Create the Lambda layer package**:

```bash
mkdir -p build/layer/python
cd build/layer/python
pip install pandas pyarrow snowflake-connector-python dotenv -t .
cd ..
zip -r ../etl-dependencies.zip .
cd ../..
```

4. **Prepare the Glue script**:

```bash
cp scripts/gold_to_snowflake.py build/
```

5. **Copy the configuration files**:

```bash
mkdir -p build/config
cp config/brand_configs.json build/config/
cp config/table_schemas.json build/config/
cp config/validation_rules.json build/config/
cd build/config
zip -r ../config.zip .
cd ../..
```

## Step 2: Create Initial S3 Buckets (15 minutes)

Manually create the S3 buckets that will store the code and data:

```bash
# Replace "your-account-number" with your AWS account number
aws s3 mb s3://m7-etl-code-repository-dev
aws s3 mb s3://m7-etl-data-bucket-dev
```

## Step 3: Upload Packages to the Code Bucket (10 minutes)

```bash
aws s3 cp build/raw_to_silver.zip s3://m7-etl-code-repository-dev/lambda/
aws s3 cp build/silver_to_gold.zip s3://m7-etl-code-repository-dev/lambda/
aws s3 cp build/etl-dependencies.zip s3://m7-etl-code-repository-dev/layers/
aws s3 cp build/gold_to_snowflake.py s3://m7-etl-code-repository-dev/glue/
aws s3 cp build/config.zip s3://m7-etl-code-repository-dev/config/
```

## Step 4: Deploy Infrastructure via CloudFormation (30-45 minutes)

1. **Download the CloudFormation template**

2. **Deploy the CloudFormation stack via the AWS console**:
   - Log in to the AWS console
   - Go to the CloudFormation service
   - Click on "Create stack" > "With new resources (standard)"
   - Select "Upload a template file" and upload the template
   - Click "Next"
   - Give your stack a name (e.g., "m7-etl-pipeline-stack")
   - Fill in the parameters with the appropriate values:
     - Environment: dev (or other according to your needs)
     - The S3 bucket names you created
     - Snowflake connection information
   - Click "Next" twice
   - Check the box acknowledging that CloudFormation may create IAM resources
   - Click "Create stack"

3. **Wait for deployment to complete**:
   - Deployment will take 10-15 minutes
   - You can track progress in the "Events" tab
   - Once the status shows "CREATE_COMPLETE", your infrastructure is ready

## Step 5: Configure S3 Triggers (15 minutes)

To configure S3 notifications that will trigger the pipeline when new files arrive:

```bash
aws s3api put-bucket-notification-configuration \
  --bucket m7-etl-data-bucket-dev \
  --notification-configuration '{
    "LambdaFunctionConfigurations": [
      {
        "Id": "TriggerETLPipeline",
        "LambdaFunctionArn": "ARN_OF_S3_EVENT_TRIGGER_LAMBDA",
        "Events": ["s3:ObjectCreated:*"],
        "Filter": {
          "Key": {
            "FilterRules": [
              {
                "Name": "suffix",
                "Value": ".csv"
              },
              {
                "Name": "prefix",
                "Value": "landing/"
              }
            ]
          }
        }
      }
    ]
  }'
```

Replace `ARN_OF_S3_EVENT_TRIGGER_LAMBDA` with the ARN of the Lambda function created by CloudFormation (available in the stack outputs).

## Step 6: Upload Initial Data for Testing (15 minutes)

Create the folder structure in the S3 bucket:

```bash
mkdir -p temp_upload/landing
```

Copy some test files to the temporary folder:

```bash
cp data/landing/*.csv temp_upload/landing/
```

Upload these files to S3:

```bash
aws s3 sync temp_upload/ s3://m7-etl-data-bucket-dev/
```

This action will automatically trigger the ETL pipeline.

## Step 7: Verification and Monitoring (15-30 minutes)

1. **Check Step Functions execution**:
   - Go to the AWS console > Step Functions
   - You should see your state machine running or completed
   - Examine the execution graph to see progress

2. **Check CloudWatch logs**:
   - Go to CloudWatch > Log Groups
   - Examine the logs of Lambda functions and the Glue job
   - Look for any errors

3. **Check the CloudWatch dashboard**:
   - Go to CloudWatch > Dashboards
   - Open the dashboard created by CloudFormation
   - You'll see pipeline performance metrics

4. **Check data in S3**:
   - Navigate to your data S3 bucket
   - Verify that files have been processed and are in the corresponding folders (raw, silver, gold)

5. **Check data in Snowflake**:
   - Log in to Snowflake
   - Verify that data has been loaded into the appropriate tables

## Step 8: Production Configuration (30 minutes)

After successful testing, you can prepare the production environment:

1. **Create a new CloudFormation stack for production**:
   - Use the same template but with different parameters
   - Choose "prod" as the environment
   - Use different bucket names

2. **Configure additional alerts**:
   - In CloudWatch, create alarms for errors and execution delays
   - Configure email/SMS notifications in case of problems

3. **Document the deployed solution**:
   - Create a technical document describing the deployed architecture
   - Include standard operating procedures (SOPs) for maintenance

## Step 9: Clean Up Temporary Resources (5 minutes)

Clean up temporary files created for deployment:

```bash
rm -rf build/
rm -rf temp_upload/
```

## Troubleshooting Common Issues

### Lambda functions fail with timeout errors
- Increase the timeout value in the CloudFormation template
- Check if the data size requires more resources

### Snowflake connection errors
- Verify that Snowflake credentials are correct
- Ensure Lambda function IP addresses are allowed to access Snowflake

### CSV files don't trigger the pipeline
- Check the S3 notification configuration
- Make sure files are uploaded to the correct folder (landing/)

## Conclusion

Your ETL pipeline is now deployed on AWS and ready to use. This solution is highly scalable and can handle increasing data volumes without major architecture changes.

To optimize further:
- Consider using AWS Secrets Manager for Snowflake credentials
- Configure AWS Backup for critical S3 buckets
- Implement a CI/CD pipeline for code updates