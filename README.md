# M7 ETL Pipeline

## Overview

This project implements a complete ETL (Extract, Transform, Load) pipeline for processing data through multiple refinement layers (landing → raw → silver → gold → Snowflake). The pipeline is designed to be robust, configurable, and easily deployable on AWS.


## Key Features

- **Layered Architecture**: Organization of data in distinct layers with clear responsibilities
- **Data Validation**: Business validation rules configurable via JSON
- **Parameterized Processing**: Flexible and adaptable to different data types
- **Comprehensive Logging**: Detailed tracking of all processing steps
- **Integrated Alerts**: Notification of data quality issues or execution problems
- **Automated Deployment**: Deployment options via AWS Step Functions or Apache Airflow
- **Monitoring**: Dashboards and metrics to monitor the pipeline

## Project Structure

```
M7-PIPELINE-USAGE-LOCAL/
│
├── config/                       # Pipeline configuration
│   ├── brand_configs.json        # Brand mapping
│   ├── table_schemas.json        # Table schemas
│   └── validation_rules.json     # Business validation rules
│
├── data/                         # Data folders (local)
│   ├── archive/                  # Archives
│   ├── gold/                     # Final consolidated data (parquet)
│   ├── landing/                  # Raw data arrival zone
│   ├── logs/                     # Execution logs
│   ├── raw/                      # Organized raw data
│   └── silver/                   # Transformed data (parquet)
│
├── scripts/                      # Pipeline scripts
│   ├── deploiement/              # Deployment scripts
│   │   ├── airflow/              # Apache Airflow deployment
│   │   │   ├── deploy_airflow.yaml
│   │   │   ├── m7_etl_pipeline.py
│   │   │   ├── requirements.txt
│   │   │   └── vpc-mwaa.yaml
│   │   ├── build/                # Deployment files
│   │   │   ├── etl-dependencies.zip
│   │   │   ├── raw_to_silver.zip
│   │   │   └── silver_to_gold.zip
│   │   ├── etl_layer/            # Lambda layer for ETL dependencies
│   │   └── step_function/        # AWS Step Functions deployment
│   │       ├── Architecture.png
│   │       ├── cloudformation_deploy.yaml
│   │       └── instruction.md
│   ├── debug.py                  # Debugging utility
│   ├── gold_to_snowflake.py      # Gold → Snowflake loading
│   ├── raw_to_silver.py          # Raw → Silver transformation
│   ├── run_pipeline.py           # Main pipeline orchestrator
│   ├── silver_to_gold.py         # Silver → Gold transformation
│   ├── tests/                    # Unit and integration tests
│   └── utils/                    # Shared utilities
│
├── .env                          # Environment variables (not committed)
├── .gitattributes
├── .gitignore
├── README.md                     # Project documentation
├── requirements.txt              # Python dependencies
└── setup.py                      # Installation script
```

## Data Processing Flow

1. **Landing → Raw** (`raw_to_silver.py`):
   - Detection of CSV files in landing folder
   - CSV format validation and automatic delimiter detection
   - Organization in folders by date and brand
   - Initial conversion to Parquet format

2. **Raw → Silver → Gold** (`silver_to_gold.py`):
   - Consolidation of Parquet files by table type
   - Application of transformation business rules
   - Generation of validation reports
   - Creation of consolidated Parquet files by table and date

3. **Gold → Snowflake** (`gold_to_snowflake.py`):
   - Data enrichment (brand mapping)
   - Application of business validation rules
   - Final technical validation
   - Optimized loading into Snowflake

4. **Orchestration** (`run_pipeline.py`):
   - Coordination of script execution
   - Management of dates to process
   - Alert and logging system
   - Data maintenance

## Deployment Options

The pipeline can be deployed in two different ways on AWS:

### 1. AWS Step Functions (Recommended for simplicity)

Deployment via AWS Step Functions offers serverless orchestration for the pipeline:

1. Navigate to the `scripts/deploiement/step_function/` folder
2. Follow the instructions in `instruction.md`
3. Use the CloudFormation template `cloudformation_deploy.yaml` to deploy the infrastructure

This method is ideal for simple workflows and environments that require minimal maintenance.

### 2. Apache Airflow (MWAA)

For more advanced orchestration with Apache Airflow:

1. Navigate to the `scripts/deploiement/airflow/` folder
2. Follow the instructions in the deployment guide
3. Use the YAML templates to configure the MWAA environment

This method offers more flexibility and orchestration possibilities but requires more resources.

## Configuration

### Environment Variables

Create an `.env` file at the root of the project with the following variables:

```
# Data paths
DATA_DIR=./data
LANDING_SUBDIR=landing
RAW_SUBDIR=raw
SILVER_SUBDIR=silver
GOLD_SUBDIR=gold
LOGS_SUBDIR=logs

# Processing configuration
EXPECTED_TABLES=Device,EPG,Playback,User,Smartcard,VODCatalog,VODCatalogExtended
MAX_DAYS_BACK=3
MOCK_SNOWFLAKE=true
DELETE_SILVER_FILES_AFTER_LOAD=false
GOLD_RETENTION_DAYS=30
RAW_RETENTION_DAYS=0
PARALLEL_PROCESSING=true
MAX_WORKERS=4

# Snowflake configuration
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=STG_SG
SNOWFLAKE_MONITORING_SCHEMA=STG_SG_MONITORING

# Alert configuration
ENABLE_EMAIL_ALERTS=false
SMTP_SERVER=smtp.example.com
SMTP_PORT=587
SMTP_USER=alerts@example.com
SMTP_PASSWORD=your_smtp_password
ADMIN_EMAIL=admin@example.com
ERROR_THRESHOLD=5.0
WARNING_THRESHOLD=10.0
```

### JSON Configuration Files

1. **brand_configs.json**: Mapping of brand names to codes.
2. **table_schemas.json**: Definition of table schemas and type conversions.
3. **validation_rules.json**: Business rules for data validation.

## Local Installation and Execution

### Prerequisites

- Python 3.9+
- pip
- Access to Snowflake (optional for MOCK mode)

### Installation

```bash
# Clone the repository
git clone https://github.com/your-organization/M7-PIPELINE-USAGE-LOCAL.git
cd M7-PIPELINE-USAGE-LOCAL

# Create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Modify .env with your values
```

### Local Execution

```bash
# Run the complete orchestrator
python scripts/run_pipeline.py

# Run a specific step with a specific date
python scripts/raw_to_silver.py --date 20250416
python scripts/silver_to_gold.py --date 20250416
python scripts/gold_to_snowflake.py --date 20250416 --mock

# Run in maintenance mode (cleaning old files)
python scripts/run_pipeline.py --maintenance
```

## Alert System

The pipeline integrates an alert system to report data quality issues or execution errors. To activate it, configure the SMTP parameters in the `.env` file and set `ENABLE_EMAIL_ALERTS=true`.

Alerts are triggered in the following cases:
- Failure of a pipeline step
- Data error rate exceeding the configured threshold
- Critical errors during execution

## Monitoring

The CloudFormation deployment automatically creates a CloudWatch dashboard to monitor:
- Lambda function invocations and durations
- Glue job metrics
- MWAA task status
- Errors and warnings

## Tests

```bash
# Run all tests
python -m pytest

# Run tests for a specific step
python -m pytest scripts/tests/test_raw_to_silver.py
```

## Production Considerations

1. **Security**:
   - Store Snowflake credentials in AWS Secrets Manager
   - Use IAM roles with least privileges
   - Enable encryption of data at rest and in transit

2. **Scalability**:
   - Increase Lambda/Glue resources for high volumes
   - Enable parallel processing with `PARALLEL_PROCESSING=true`
   - Adjust `MAX_WORKERS` according to data volume

3. **Reliability**:
   - Configure alerts for quick intervention
   - Implement a recovery system for failed processes
   - Regularly back up configuration files

## Contributing

1. Fork the project
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Need Help?

For any questions or assistance regarding this ETL pipeline, please contact Abidine.Mouliom@canal-plus.com or open an issue in the repository.

## License

This project is licensed under M7 Group.

## Acknowledgments

This ETL pipeline was developed as part of an internship. It incorporates professional data engineering practices and offers a complete and robust solution for data processing.