"""
Dataset Processing Functions for ETL Pipeline
Contains all the data processing logic for the complex dataset ETL DAG
"""

import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ensure_dir(path):
    """Ensure directory exists for given path (can be file or directory path)"""
    # If path appears to be a file (has extension), get parent directory
    # Otherwise, treat as directory path
    if '.' in os.path.basename(path) and not path.endswith('/'):
        # Likely a file path, create parent directory
        Path(os.path.dirname(path)).mkdir(parents=True, exist_ok=True)
    else:
        # Directory path, create the directory itself
        Path(path).mkdir(parents=True, exist_ok=True)

def extract_sample_dataset(dataset_name, output_path, num_records, **context):
    """Generate a sample customer transaction dataset"""
    logger.info(f"Extracting {num_records} records for {dataset_name}")

    ensure_dir(output_path)

    # Generate realistic sample data
    np.random.seed(42)  # For reproducible results

    # Generate customer IDs
    customer_ids = [f"CUST_{i:06d}" for i in range(1, min(1000, num_records//10) + 1)]

    # Generate transactions
    data = []
    start_date = datetime(2023, 1, 1)

    for i in range(num_records):
        transaction_date = start_date + timedelta(days=np.random.randint(0, 365))
        customer_id = np.random.choice(customer_ids)

        # Generate realistic transaction amounts with some patterns
        if np.random.random() < 0.1:  # 10% large transactions
            amount = np.random.lognormal(8, 1)  # Large amounts
        else:
            amount = np.random.lognormal(4, 1)  # Regular amounts

        transaction_type = np.random.choice(['purchase', 'refund', 'subscription'], p=[0.7, 0.2, 0.1])

        # Add some missing values intentionally (5% missing)
        merchant = np.random.choice(['Amazon', 'Walmart', 'Target', 'Best Buy', None], p=[0.3, 0.25, 0.2, 0.2, 0.05])

        data.append({
            'transaction_id': f"TXN_{i:08d}",
            'customer_id': customer_id,
            'transaction_date': transaction_date.strftime('%Y-%m-%d'),
            'transaction_amount': round(amount, 2),
            'transaction_type': transaction_type,
            'merchant': merchant,
            'created_at': datetime.now().isoformat()
        })

    # Create DataFrame and save
    df = pd.DataFrame(data)

    # Add some duplicates intentionally (2% duplicates)
    num_duplicates = int(len(df) * 0.02)
    duplicate_indices = np.random.choice(len(df), num_duplicates, replace=False)
    duplicates = df.iloc[duplicate_indices].copy()
    df = pd.concat([df, duplicates], ignore_index=True)

    df.to_csv(f"{output_path}/customer_transactions.csv", index=False)
    logger.info(f"Generated {len(df)} records and saved to {output_path}")

    return f"Extracted {len(df)} records"

def validate_raw_data(input_path, **context):
    """Validate the structure and basic quality of raw data"""
    logger.info(f"Validating raw data from {input_path}")

    df = pd.read_csv(f"{input_path}/customer_transactions.csv")

    # Basic validation checks
    required_columns = ['transaction_id', 'customer_id', 'transaction_date', 'transaction_amount']
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # Check for completely empty dataset
    if len(df) == 0:
        raise ValueError("Dataset is empty")

    logger.info(f"Validation passed: {len(df)} records with {len(df.columns)} columns")
    return f"Validation successful: {len(df)} records"

def check_data_completeness(input_path, completeness_threshold, **context):
    """Check data completeness and report missing values"""
    logger.info(f"Checking data completeness with threshold {completeness_threshold}")

    df = pd.read_csv(f"{input_path}/customer_transactions.csv")

    # Calculate completeness for each column
    completeness = {}
    for col in df.columns:
        completeness[col] = (df[col].notna().sum() / len(df))

    # Check if any column falls below threshold
    failed_columns = [col for col, comp in completeness.items() if comp < completeness_threshold]

    if failed_columns:
        logger.warning(f"Columns below completeness threshold: {failed_columns}")
    else:
        logger.info("All columns meet completeness requirements")

    # Store results for later use
    context['task_instance'].xcom_push(key='completeness_report', value=completeness)

    return f"Completeness check: {len(failed_columns)} columns below threshold"

def check_duplicates(input_path, duplicate_threshold, **context):
    """Check for duplicate records"""
    logger.info(f"Checking for duplicates with threshold {duplicate_threshold}")

    df = pd.read_csv(f"{input_path}/customer_transactions.csv")

    # Check for duplicates
    duplicates = df.duplicated()
    duplicate_rate = duplicates.sum() / len(df)

    logger.info(f"Found {duplicates.sum()} duplicates ({duplicate_rate:.2%})")

    if duplicate_rate > duplicate_threshold:
        logger.warning(f"Duplicate rate {duplicate_rate:.2%} exceeds threshold {duplicate_threshold:.2%}")

    # Store results
    context['task_instance'].xcom_push(key='duplicate_count', value=int(duplicates.sum()))
    context['task_instance'].xcom_push(key='duplicate_rate', value=duplicate_rate)

    return f"Duplicate check: {duplicates.sum()} duplicates found"

def detect_outliers(input_path, outlier_method, **context):
    """Detect outliers in transaction amounts"""
    logger.info(f"Detecting outliers using {outlier_method} method")

    df = pd.read_csv(f"{input_path}/customer_transactions.csv")

    amounts = df['transaction_amount'].dropna()

    if outlier_method == 'iqr':
        Q1 = amounts.quantile(0.25)
        Q3 = amounts.quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        outliers = (amounts < lower_bound) | (amounts > upper_bound)
    else:
        # Z-score method
        z_scores = np.abs((amounts - amounts.mean()) / amounts.std())
        outliers = z_scores > 3

    outlier_count = outliers.sum()
    outlier_rate = outlier_count / len(amounts)

    logger.info(f"Found {outlier_count} outliers ({outlier_rate:.2%})")

    # Store results
    context['task_instance'].xcom_push(key='outlier_count', value=int(outlier_count))
    context['task_instance'].xcom_push(key='outlier_indices', value=outliers[outliers].index.tolist())

    return f"Outlier detection: {outlier_count} outliers found"

def handle_missing_values(input_path, output_path, strategy, **context):
    """Handle missing values in the dataset"""
    logger.info(f"Handling missing values using {strategy} strategy")

    ensure_dir(output_path)
    df = pd.read_csv(f"{input_path}/customer_transactions.csv")

    # Apply different strategies based on column type
    if strategy == 'forward_fill':
        # Forward fill for categorical columns
        df['merchant'] = df['merchant'].fillna(method='ffill')
        # Fill remaining with 'Unknown'
        df['merchant'] = df['merchant'].fillna('Unknown')
    elif strategy == 'mean_fill':
        # Fill numeric columns with mean
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].mean())

    # Save cleaned data
    df.to_csv(f"{output_path}/customer_transactions_clean.csv", index=False)

    logger.info(f"Cleaned dataset saved with {len(df)} records")
    return f"Missing values handled: {len(df)} records cleaned"

def remove_duplicates(input_path, output_path, subset_columns, **context):
    """Remove duplicate records from the dataset"""
    logger.info(f"Removing duplicates based on columns: {subset_columns}")

    ensure_dir(output_path)
    df = pd.read_csv(f"{input_path}/customer_transactions_clean.csv")

    initial_count = len(df)
    df_deduped = df.drop_duplicates(subset=subset_columns, keep='first')
    final_count = len(df_deduped)

    removed_count = initial_count - final_count

    # Save deduplicated data
    df_deduped.to_csv(f"{output_path}/customer_transactions_deduped.csv", index=False)

    logger.info(f"Removed {removed_count} duplicates, {final_count} records remaining")
    return f"Duplicates removed: {removed_count} duplicates, {final_count} records remaining"

def calculate_customer_metrics(input_path, output_path, **context):
    """Calculate customer-level metrics"""
    logger.info("Calculating customer metrics")

    ensure_dir(output_path)
    df = pd.read_csv(f"{input_path}/customer_transactions_deduped.csv")
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])

    # Calculate customer metrics
    customer_metrics = df.groupby('customer_id').agg({
        'transaction_amount': ['sum', 'mean', 'count', 'std'],
        'transaction_date': ['min', 'max'],
        'transaction_type': lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else 'purchase'
    }).round(2)

    # Flatten column names
    customer_metrics.columns = ['_'.join(col).strip() for col in customer_metrics.columns]

    # Calculate additional metrics
    customer_metrics['days_active'] = (
        pd.to_datetime(customer_metrics['transaction_date_max']) -
        pd.to_datetime(customer_metrics['transaction_date_min'])
    ).dt.days

    customer_metrics['avg_transaction_per_day'] = (
        customer_metrics['transaction_amount_count'] /
        customer_metrics['days_active'].replace(0, 1)
    ).round(2)

    # Reset index to include customer_id as column
    customer_metrics.reset_index(inplace=True)

    # Save metrics
    customer_metrics.to_csv(f"{output_path}/customer_metrics.csv", index=False)

    logger.info(f"Calculated metrics for {len(customer_metrics)} customers")
    return f"Customer metrics calculated for {len(customer_metrics)} customers"

def create_time_features(input_path, output_path, **context):
    """Create time-based features from transaction data"""
    logger.info("Creating time-based features")

    ensure_dir(output_path)
    df = pd.read_csv(f"{input_path}/customer_transactions_deduped.csv")
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])

    # Create time features
    df['year'] = df['transaction_date'].dt.year
    df['month'] = df['transaction_date'].dt.month
    df['day_of_week'] = df['transaction_date'].dt.dayofweek
    df['day_of_month'] = df['transaction_date'].dt.day
    df['week_of_year'] = df['transaction_date'].dt.isocalendar().week
    df['is_weekend'] = df['day_of_week'].isin([5, 6])
    df['quarter'] = df['transaction_date'].dt.quarter

    # Create time-based aggregations
    monthly_summary = df.groupby(['year', 'month']).agg({
        'transaction_amount': ['sum', 'mean', 'count'],
        'customer_id': 'nunique'
    }).round(2)

    monthly_summary.columns = ['_'.join(col).strip() for col in monthly_summary.columns]
    monthly_summary.reset_index(inplace=True)

    # Save both detailed and summary data
    df.to_csv(f"{output_path}/transactions_with_time_features.csv", index=False)
    monthly_summary.to_csv(f"{output_path}/monthly_summary.csv", index=False)

    logger.info(f"Created time features for {len(df)} transactions")
    return f"Time features created for {len(df)} transactions"

def calculate_rolling_averages(input_path, output_path, window_days, **context):
    """Calculate rolling averages for different time windows"""
    logger.info(f"Calculating rolling averages for windows: {window_days}")

    ensure_dir(output_path)
    df = pd.read_csv(f"{input_path}/customer_transactions_deduped.csv")
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])

    # Sort by date for rolling calculations
    df = df.sort_values('transaction_date')

    # Calculate daily aggregates first
    daily_agg = df.groupby('transaction_date').agg({
        'transaction_amount': ['sum', 'mean', 'count'],
        'customer_id': 'nunique'
    }).round(2)

    daily_agg.columns = ['_'.join(col).strip() for col in daily_agg.columns]

    # Calculate rolling averages for each window
    for window in window_days:
        daily_agg[f'rolling_sum_{window}d'] = daily_agg['transaction_amount_sum'].rolling(window=window, min_periods=1).mean().round(2)
        daily_agg[f'rolling_avg_amount_{window}d'] = daily_agg['transaction_amount_mean'].rolling(window=window, min_periods=1).mean().round(2)
        daily_agg[f'rolling_customer_count_{window}d'] = daily_agg['customer_id_nunique'].rolling(window=window, min_periods=1).mean().round(2)

    daily_agg.reset_index(inplace=True)

    # Save rolling metrics
    daily_agg.to_csv(f"{output_path}/rolling_metrics.csv", index=False)

    logger.info(f"Calculated rolling averages for {len(daily_agg)} days")
    return f"Rolling averages calculated for {len(daily_agg)} days"

def perform_cohort_analysis(input_path, customer_metrics_path, output_path, **context):
    """Perform customer cohort analysis"""
    logger.info("Performing cohort analysis")

    ensure_dir(output_path)
    df = pd.read_csv(f"{input_path}/customer_transactions_deduped.csv")

    df['transaction_date'] = pd.to_datetime(df['transaction_date'])

    # Define cohorts based on first transaction month
    customer_first_purchase = df.groupby('customer_id')['transaction_date'].min().reset_index()
    customer_first_purchase['cohort_month'] = customer_first_purchase['transaction_date'].dt.to_period('M')

    # Merge with transactions
    df = df.merge(customer_first_purchase[['customer_id', 'cohort_month']], on='customer_id')
    df['period_number'] = (df['transaction_date'].dt.to_period('M') - df['cohort_month']).apply(lambda x: x.n)

    # Calculate cohort table
    cohort_data = df.groupby(['cohort_month', 'period_number'])['customer_id'].nunique().reset_index()
    cohort_table = cohort_data.pivot(index='cohort_month', columns='period_number', values='customer_id')

    # Calculate retention rates
    cohort_sizes = customer_first_purchase.groupby('cohort_month')['customer_id'].nunique()
    retention_table = cohort_table.divide(cohort_sizes, axis=0)

    # Save cohort analysis
    cohort_table.to_csv(f"{output_path}/cohort_table.csv")
    retention_table.to_csv(f"{output_path}/retention_rates.csv")

    logger.info(f"Cohort analysis completed for {len(cohort_sizes)} cohorts")
    return f"Cohort analysis completed for {len(cohort_sizes)} cohorts"

def generate_transaction_insights(input_path, time_features_path, output_path, **context):
    """Generate insights from transaction patterns"""
    logger.info("Generating transaction insights")

    ensure_dir(output_path)
    df = pd.read_csv(f"{time_features_path}/transactions_with_time_features.csv")

    insights = {}

    # Transaction patterns by day of week
    dow_analysis = df.groupby('day_of_week')['transaction_amount'].agg(['sum', 'mean', 'count']).round(2)
    insights['day_of_week_patterns'] = dow_analysis.to_dict()

    # Weekend vs weekday analysis
    weekend_analysis = df.groupby('is_weekend')['transaction_amount'].agg(['sum', 'mean', 'count']).round(2)
    insights['weekend_analysis'] = weekend_analysis.to_dict()

    # Monthly trends
    monthly_trends = df.groupby(['year', 'month'])['transaction_amount'].agg(['sum', 'mean', 'count']).round(2)
    insights['monthly_trends'] = monthly_trends.to_dict()

    # Transaction type analysis
    type_analysis = df.groupby('transaction_type')['transaction_amount'].agg(['sum', 'mean', 'count']).round(2)
    insights['transaction_type_analysis'] = type_analysis.to_dict()

    # Save insights
    with open(f"{output_path}/transaction_insights.json", 'w') as f:
        json.dump(insights, f, indent=2, default=str)

    logger.info("Transaction insights generated")
    return "Transaction insights generated successfully"

def calculate_trend_analysis(input_path, output_path, **context):
    """Calculate trend analysis from rolling metrics"""
    logger.info("Calculating trend analysis")

    ensure_dir(output_path)
    df = pd.read_csv(f"{input_path}/rolling_metrics.csv")
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])

    trends = {}

    # Calculate growth rates for different metrics
    for col in df.columns:
        if col.startswith('rolling_'):
            # Calculate month-over-month growth
            df[f'{col}_growth'] = df[col].pct_change() * 100

            # Calculate trend direction
            recent_trend = df[f'{col}_growth'].tail(30).mean()  # Last 30 days average
            trends[col] = {
                'recent_growth_rate': round(recent_trend, 2),
                'trend_direction': 'increasing' if recent_trend > 0 else 'decreasing',
                'volatility': round(df[f'{col}_growth'].std(), 2)
            }

    # Save trend analysis
    df.to_csv(f"{output_path}/trends_with_growth.csv", index=False)

    with open(f"{output_path}/trend_summary.json", 'w') as f:
        json.dump(trends, f, indent=2)

    logger.info("Trend analysis completed")
    return "Trend analysis completed successfully"

def merge_analysis_results(cohort_path, insights_path, trends_path, output_path, **context):
    """Merge all analysis results into final dataset"""
    logger.info("Merging all analysis results")

    ensure_dir(output_path)

    # Load all analysis results
    cohort_retention = pd.read_csv(f"{cohort_path}/retention_rates.csv")

    with open(f"{insights_path}/transaction_insights.json", 'r') as f:
        insights = json.load(f)

    with open(f"{trends_path}/trend_summary.json", 'r') as f:
        trends = json.load(f)

    # Create final summary
    final_summary = {
        'processing_date': datetime.now().isoformat(),
        'cohort_analysis': {
            'total_cohorts': len(cohort_retention),
            'avg_retention_month_1': cohort_retention.iloc[:, 1].mean() if len(cohort_retention.columns) > 1 else 0
        },
        'transaction_insights': insights,
        'trend_analysis': trends,
        'pipeline_metadata': {
            'total_processing_stages': 9,
            'data_quality_checks': 3,
            'feature_engineering_steps': 3,
            'analysis_components': 3
        }
    }

    # Save final results
    with open(f"{output_path}/final_analysis_summary.json", 'w') as f:
        json.dump(final_summary, f, indent=2, default=str)

    logger.info("Analysis results merged successfully")
    return "All analysis results merged into final dataset"

def export_to_csv(input_path, output_path, **context):
    """Export final dataset to CSV format"""
    logger.info("Exporting to CSV format")

    ensure_dir(output_path)

    # Load and export final summary
    with open(f"{input_path}/final_analysis_summary.json", 'r') as f:
        data = json.load(f)

    # Create a flattened CSV for the summary
    summary_rows = []
    for key, value in data.items():
        if isinstance(value, dict):
            for subkey, subvalue in value.items():
                summary_rows.append({
                    'category': key,
                    'metric': subkey,
                    'value': str(subvalue)
                })
        else:
            summary_rows.append({
                'category': 'general',
                'metric': key,
                'value': str(value)
            })

    pd.DataFrame(summary_rows).to_csv(output_path, index=False)

    logger.info(f"Exported to CSV: {output_path}")
    return f"Successfully exported to CSV: {output_path}"

def export_to_parquet(input_path, output_path, **context):
    """Export final dataset to Parquet format"""
    logger.info("Exporting to Parquet format")

    ensure_dir(output_path)

    # For parquet, we'll export the summary as a more structured format
    with open(f"{input_path}/final_analysis_summary.json", 'r') as f:
        data = json.load(f)

    # Convert to DataFrame and save as parquet
    df = pd.json_normalize(data, sep='_')
    df.to_parquet(output_path, index=False)

    logger.info(f"Exported to Parquet: {output_path}")
    return f"Successfully exported to Parquet: {output_path}"

def export_summary_statistics(input_path, output_path, **context):
    """Export summary statistics as JSON"""
    logger.info("Exporting summary statistics")

    ensure_dir(output_path)

    # Copy the final summary with additional metadata
    with open(f"{input_path}/final_analysis_summary.json", 'r') as f:
        data = json.load(f)

    # Add export metadata
    data['export_metadata'] = {
        'exported_at': datetime.now().isoformat(),
        'export_format': 'json',
        'airflow_dag_id': context.get('dag').dag_id if context.get('dag') else 'dataset_etl_pipeline',
        'airflow_run_id': context.get('run_id', 'unknown')
    }

    # Save enhanced summary
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2, default=str)

    logger.info(f"Summary statistics exported: {output_path}")
    return f"Summary statistics exported: {output_path}"

def validate_final_dataset(dataset_path, min_records, required_columns, **context):
    """Validate the final processed dataset"""
    logger.info("Validating final dataset")

    # Load final summary for validation
    with open(f"{dataset_path}/final_analysis_summary.json", 'r') as f:
        data = json.load(f)

    # Perform validation checks
    validation_results = {
        'validation_passed': True,
        'checks': []
    }

    # Check if we have the required structure
    required_sections = ['cohort_analysis', 'transaction_insights', 'trend_analysis']
    for section in required_sections:
        if section in data:
            validation_results['checks'].append({
                'check': f'{section}_exists',
                'status': 'PASS',
                'message': f'{section} data found'
            })
        else:
            validation_results['checks'].append({
                'check': f'{section}_exists',
                'status': 'FAIL',
                'message': f'{section} data missing'
            })
            validation_results['validation_passed'] = False

    # Store validation results
    context['task_instance'].xcom_push(key='validation_results', value=validation_results)

    if validation_results['validation_passed']:
        logger.info("Final dataset validation passed")
        return "Final dataset validation: PASSED"
    else:
        logger.error("Final dataset validation failed")
        raise ValueError("Final dataset validation failed")

def generate_quality_report(dataset_path, output_path, **context):
    """Generate an HTML data quality report"""
    logger.info("Generating data quality report")

    ensure_dir(output_path)

    # Load final data and validation results
    with open(f"{dataset_path}/final_analysis_summary.json", 'r') as f:
        data = json.load(f)

    validation_results = context['task_instance'].xcom_pull(key='validation_results')

    # Generate HTML report
    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <title>Dataset ETL Pipeline - Quality Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background-color: #f0f8ff; padding: 20px; border-radius: 5px; }}
        .section {{ margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }}
        .pass {{ color: green; }}
        .fail {{ color: red; }}
        .metric {{ margin: 5px 0; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>📊 Dataset ETL Pipeline Quality Report</h1>
        <p>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p>Pipeline Status: <span class="{'pass' if validation_results['validation_passed'] else 'fail'}">
            {'✅ PASSED' if validation_results['validation_passed'] else '❌ FAILED'}
        </span></p>
    </div>

    <div class="section">
        <h2>Validation Checks</h2>
        <table>
            <tr><th>Check</th><th>Status</th><th>Message</th></tr>"""

    for check in validation_results['checks']:
        status_class = 'pass' if check['status'] == 'PASS' else 'fail'
        html_content += f"""
            <tr>
                <td>{check['check']}</td>
                <td class="{status_class}">{check['status']}</td>
                <td>{check['message']}</td>
            </tr>"""

    html_content += """
        </table>
    </div>

    <div class="section">
        <h2>Pipeline Summary</h2>"""

    if 'pipeline_metadata' in data:
        for key, value in data['pipeline_metadata'].items():
            html_content += f'<div class="metric"><strong>{key.replace("_", " ").title()}:</strong> {value}</div>'

    html_content += """
    </div>

    <div class="section">
        <h2>Processing Completed</h2>
        <p>✅ Data extraction and validation</p>
        <p>✅ Quality checks (completeness, duplicates, outliers)</p>
        <p>✅ Data cleaning and deduplication</p>
        <p>✅ Feature engineering (customer metrics, time features, rolling averages)</p>
        <p>✅ Advanced analysis (cohort analysis, transaction insights, trend analysis)</p>
        <p>✅ Data export (CSV, Parquet, JSON formats)</p>
        <p>✅ Quality assurance and reporting</p>
    </div>
</body>
</html>"""

    # Save HTML report
    with open(output_path, 'w') as f:
        f.write(html_content)

    logger.info(f"Quality report generated: {output_path}")
    return f"Quality report generated: {output_path}"
