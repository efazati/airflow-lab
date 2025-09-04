"""
Alternative functions using dlt's filesystem source for more flexibility
This version can handle local files, S3, GCS, HTTP URLs, etc.
"""
import dlt
from dlt.sources.filesystem import filesystem
import duckdb
import os
from typing import List, Dict, Any
import pandas as pd

# Configuration
DATASETS_PATH = '/opt/airflow/datasets'  # Use original datasets folder
DUCKDB_PATH = '/tmp/streaming_platforms.duckdb'
PIPELINE_NAME = 'streaming_platforms'
DATASET_NAME = 'streaming_data'

def load_csv_with_filesystem_source(platform_name: str, csv_filename: str, datasets_path: str = None) -> None:
    """
    Load CSV using dlt's filesystem source - supports local, S3, GCS, HTTP

    Args:
        platform_name: Name of the streaming platform (netflix, disney, hulu, prime)
        csv_filename: Name of the CSV file to load
        datasets_path: Path or URL to datasets (local path, s3://, gs://, http://)
    """
    print(f"Loading {platform_name} data from {csv_filename} using dlt filesystem source")

    # Use provided path or default
    data_path = datasets_path or DATASETS_PATH

    # Create filesystem resource
    if data_path.startswith(('s3://', 'gs://', 'http://', 'https://')):
        # Remote source
        resource = filesystem(
            bucket_url=data_path,
            file_glob=csv_filename
        )
        print(f"Using remote source: {data_path}")
    else:
        # Local filesystem
        resource = filesystem(
            bucket_url=f"file://{data_path}",
            file_glob=csv_filename
        )
        print(f"Using local source: {data_path}")

    # Create dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name=f"{PIPELINE_NAME}_{platform_name}",
        destination="duckdb",
        dataset_name=DATASET_NAME,
        destination_config={"database": DUCKDB_PATH}
    )

    # Transform the data to add platform indicator
    @dlt.transformer
    def add_platform_indicator(items):
        """Add platform indicator to each record"""
        for item in items:
            # If it's file content, parse CSV
            if 'file_content' in item:
                # Parse CSV content
                import io
                df = pd.read_csv(io.StringIO(item['file_content']))

                # Add platform indicator
                df[f'in_{platform_name}'] = True

                # Ensure required columns exist
                required_columns = ['show_id', 'type', 'title', 'director', 'cast', 'country', 'release_year', 'duration', 'listed_in']
                for col in required_columns:
                    if col not in df.columns:
                        df[col] = ''

                # Clean data
                df = df.fillna('')
                df['release_year'] = pd.to_numeric(df['release_year'], errors='coerce').fillna(0).astype(int)

                # Select only needed columns
                columns_to_keep = required_columns + [f'in_{platform_name}']
                df_filtered = df[columns_to_keep]

                # Yield each row
                for _, row in df_filtered.iterrows():
                    yield row.to_dict()
            else:
                # Direct data item
                item[f'in_{platform_name}'] = True
                yield item

    # Run the pipeline with transformation
    load_info = pipeline.run(resource | add_platform_indicator, table_name=f"{platform_name}_titles")
    print(f"Successfully loaded {platform_name} data")
    print(f"Load info: {load_info}")

def load_csv_from_url(platform_name: str, csv_url: str) -> None:
    """
    Load CSV directly from HTTP URL using dlt

    Args:
        platform_name: Name of the streaming platform
        csv_url: Direct URL to the CSV file
    """
    print(f"Loading {platform_name} data from URL: {csv_url}")

    # Use dlt's HTTP source capabilities
    import requests
    import io

    # Download CSV
    response = requests.get(csv_url)
    response.raise_for_status()

    # Parse CSV
    df = pd.read_csv(io.StringIO(response.text))
    print(f"Downloaded and parsed {len(df)} records from {csv_url}")

    # Add platform indicator
    df[f'in_{platform_name}'] = True

    # Ensure required columns exist
    required_columns = ['show_id', 'type', 'title', 'director', 'cast', 'country', 'release_year', 'duration', 'listed_in']
    for col in required_columns:
        if col not in df.columns:
            df[col] = ''

    # Clean data
    df = df.fillna('')
    df['release_year'] = pd.to_numeric(df['release_year'], errors='coerce').fillna(0).astype(int)

    # Select only needed columns
    columns_to_keep = required_columns + [f'in_{platform_name}']
    df_filtered = df[columns_to_keep]

    # Create dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name=f"{PIPELINE_NAME}_{platform_name}",
        destination="duckdb",
        dataset_name=DATASET_NAME,
        destination_config={"database": DUCKDB_PATH}
    )

    # Convert to records and load
    data = df_filtered.to_dict('records')
    load_info = pipeline.run(data, table_name=f"{platform_name}_titles")
    print(f"Successfully loaded {len(data)} records for {platform_name}")
    print(f"Load info: {load_info}")

# Example usage configurations:
EXAMPLE_CONFIGS = {
    "local_files": {
        "datasets_path": "/opt/airflow/datasets",
        "description": "Load from local filesystem"
    },
    "s3_bucket": {
        "datasets_path": "s3://your-bucket/datasets/",
        "description": "Load from S3 bucket"
    },
    "gcs_bucket": {
        "datasets_path": "gs://your-bucket/datasets/",
        "description": "Load from Google Cloud Storage"
    },
    "http_source": {
        "datasets_path": "https://example.com/datasets/",
        "description": "Load from HTTP source"
    }
}
