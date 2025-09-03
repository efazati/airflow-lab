# Dataset ETL Pipeline - Complex Multi-Stage Data Processing

## Overview

This is a comprehensive **Dataset ETL Pipeline** that demonstrates advanced data processing patterns in Apache Airflow. The pipeline processes a simulated customer transaction dataset through multiple stages, showcasing real-world ETL best practices.

## Pipeline Architecture

The pipeline consists of **9 distinct stages** with **18 tasks** organized in a sophisticated dependency graph:

### 🔄 Stage 1: Data Extraction
- **extract_raw_data**: Generates 10,000 sample customer transaction records
- **validate_raw_data**: Validates schema and basic data structure

### 🔍 Stage 2: Data Quality Checks (Parallel)
- **check_data_completeness**: Ensures 95% data completeness threshold
- **check_data_duplicates**: Identifies duplicates with 5% threshold
- **check_data_outliers**: Detects outliers using IQR method

### 🧹 Stage 3: Data Cleaning
- **clean_missing_values**: Handles missing values with forward-fill strategy
- **remove_duplicates**: Removes duplicates based on customer+date+amount

### ⚙️ Stage 4: Feature Engineering (Parallel)
- **calculate_customer_metrics**: Aggregates customer-level statistics
- **create_time_features**: Extracts temporal features (year, month, day-of-week, etc.)
- **calculate_rolling_averages**: Computes 7d, 30d, 90d rolling metrics

### 📊 Stage 5: Data Analysis (Parallel)
- **perform_cohort_analysis**: Customer retention cohort analysis
- **generate_transaction_insights**: Pattern analysis by time and type
- **calculate_trend_analysis**: Growth rates and volatility metrics

### 🔗 Stage 6: Data Consolidation
- **merge_analysis_results**: Combines all analysis outputs into final dataset

### 📤 Stage 7: Data Export (Parallel)
- **export_to_csv**: Exports to CSV format
- **export_to_parquet**: Exports to Parquet format
- **export_summary_json**: Exports structured metadata

### ✅ Stage 8: Quality Assurance
- **validate_final_dataset**: Performs final validation checks

### 📋 Stage 9: Reporting
- **generate_data_quality_report**: Creates HTML quality report
- **send_completion_notification**: Sends completion notification

## Key Features

### 🏗️ **Complex Dependencies**
- Mix of sequential and parallel processing
- Proper dependency management across stages
- Fan-out and fan-in patterns

### 📈 **Realistic Data Processing**
- Customer transaction simulation with realistic patterns
- Multiple data quality dimensions
- Advanced analytics (cohort analysis, trend analysis)

### 🔧 **Production-Ready Patterns**
- Comprehensive error handling and retries
- Data validation at multiple stages
- Configurable thresholds and parameters
- Detailed logging and monitoring

### 📊 **Multiple Output Formats**
- CSV for flat file compatibility
- Parquet for efficient columnar storage
- JSON for structured metadata
- HTML for human-readable reports

## Generated Dataset Schema

The pipeline processes customer transaction data with the following schema:

```
transaction_id      : Unique transaction identifier
customer_id         : Customer identifier (CUST_######)
transaction_date    : Date of transaction (YYYY-MM-DD)
transaction_amount  : Transaction amount (float)
transaction_type    : Type (purchase/refund/subscription)
merchant           : Merchant name (with some missing values)
created_at         : Record creation timestamp
```

## Output Artifacts

The pipeline generates several output artifacts in `/tmp/airflow/`:

- **raw_data/**: Original extracted dataset
- **cleaned_data/**: Data after missing value handling
- **deduped_data/**: Deduplicated clean dataset
- **customer_metrics/**: Customer-level aggregations
- **time_features/**: Dataset with temporal features
- **rolling_metrics/**: Rolling average calculations
- **cohort_analysis/**: Customer cohort retention analysis
- **transaction_insights/**: Pattern analysis results
- **trend_analysis/**: Growth and volatility metrics
- **final_dataset/**: Consolidated analysis results
- **exports/**: Final data exports (CSV, Parquet, JSON)
- **reports/**: HTML quality report

## Configuration

The DAG is configured via `dataset_etl_pipeline.yaml`:

- **Schedule**: Daily (`@daily`)
- **Owner**: data-team
- **Retries**: 2 with 5-minute delay
- **Max Active Runs**: 1 (prevents overlapping runs)
- **Catchup**: Disabled

## Usage

1. **Start Airflow**: Use the lab-2 startup scripts
2. **Enable DAG**: The DAG will appear as `dataset_etl_pipeline` in the Airflow UI
3. **Trigger Run**: Can be triggered manually or runs daily
4. **Monitor Progress**: Track progress through the Airflow UI
5. **View Results**: Check the HTML report in `/tmp/airflow/reports/`

## Dependencies

The pipeline requires these additional Python packages (already added to `requirements.txt`):

- `pandas>=1.5.0`: Data manipulation and analysis
- `numpy>=1.24.0`: Numerical computing
- `sqlalchemy>=1.4.0`: Database connectivity (future use)

## Customization

You can customize the pipeline by modifying:

- **Dataset size**: Change `num_records` in the YAML config
- **Quality thresholds**: Adjust `completeness_threshold`, `duplicate_threshold`
- **Rolling windows**: Modify `window_days` parameter
- **Output paths**: Change file paths in the YAML config
- **Processing logic**: Modify functions in `dataset_functions.py`

## Monitoring & Troubleshooting

- **Task Logs**: Check individual task logs in Airflow UI
- **XCom Values**: Intermediate results stored in XCom for debugging
- **Quality Report**: Comprehensive HTML report shows pipeline health
- **Validation Checks**: Built-in validation at multiple stages

This pipeline serves as a comprehensive example of production-ready data processing workflows in Airflow, demonstrating complex dependencies, parallel processing, data quality management, and comprehensive reporting.
