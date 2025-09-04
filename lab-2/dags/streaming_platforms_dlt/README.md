# Streaming Platforms Pure ORM Data Pipeline

This DAG processes streaming platform datasets (Netflix, Disney+, Hulu, Amazon Prime) using a **pure SQLAlchemy ORM approach** with DuckDB as the backend database.

## Overview

The pipeline performs the following operations:
1. **Parallel CSV Import**: Loads 4 CSV files in parallel using pure ORM operations
2. **Data Merging**: Merges and deduplicates titles across platforms using ORM logic
3. **Data Validation**: Comprehensive statistics and quality checks with ORM queries
4. **Cross-Platform Analysis**: Advanced analytics using SQLAlchemy ORM features
5. **ORM Demonstrations**: Showcases advanced ORM capabilities and patterns

## Schema

The final unified table (`unified_streaming_platforms`) contains:

| Column | Type | Description |
|--------|------|-------------|
| show_id | STRING | Unique identifier for the show |
| type | STRING | Content type (Movie/TV Show) |
| title | STRING | Title of the content |
| director | STRING | Director(s) |
| cast | STRING | Cast members |
| country | STRING | Country of origin |
| release_year | INTEGER | Year of release |
| duration | STRING | Duration (e.g., "90 min", "2 Seasons") |
| listed_in | STRING | Genres/categories |
| in_netflix | BOOLEAN | Available on Netflix |
| in_disney | BOOLEAN | Available on Disney+ |
| in_hulu | BOOLEAN | Available on Hulu |
| in_prime | BOOLEAN | Available on Amazon Prime |

## Files Structure

```
streaming_platforms_dlt/
├── streaming_platforms_dlt.py      # Main DAG definition
├── streaming_platforms_dlt.yaml    # DAG Factory configuration
├── dag_loader.py                   # DAG loader for Factory pattern
├── functions.py                    # Core processing functions
├── datasets -> ../../datasets      # Symbolic link to original datasets
└── README.md                       # This file
```

## Data Source Configuration

The DAG references the original datasets from `/opt/airflow/datasets` (which maps to `lab-2/datasets/` in your local setup). This approach:

✅ **No data duplication** - Uses original files directly
✅ **Single source of truth** - All DAGs can reference the same datasets
✅ **Easy maintenance** - Update datasets in one place
✅ **Flexible paths** - Can be configured via environment variables or DAG parameters

### Alternative Data Source Options

You can easily modify the DAG to use different data sources:

#### 1. **Environment Variables**
```python
DATASETS_PATH = os.environ.get('DATASETS_PATH', '/opt/airflow/datasets')
```

#### 2. **Remote URLs** (for dlt's HTTP source)
```python
# In functions.py, replace file reading with:
import dlt
from dlt.sources.filesystem import filesystem

# For S3, GCS, or HTTP sources
resource = filesystem(
    bucket_url="s3://your-bucket/datasets/",
    file_glob="*.csv"
)
```

#### 3. **Database Sources**
```python
# Connect directly to databases
from dlt.sources.sql_database import sql_database

source = sql_database(
    credentials="postgresql://user:pass@host/db",
    table_names=["streaming_data"]
)
```

## Features

### Parallel Processing
- All 4 CSV files are loaded simultaneously for optimal performance
- Each platform gets its own DuckDB table initially

### Data Quality
- Handles missing values and data inconsistencies
- Standardizes column names and data types
- Provides comprehensive validation and statistics

### Platform Indicators
- Boolean flags for each streaming platform
- Identifies content available on multiple platforms
- Enables cross-platform analysis

## Usage

### Running the DAG
1. Ensure dlt and DuckDB are installed (see requirements.txt)
2. Place CSV files in the `datasets/` directory
3. The DAG runs daily by default, or trigger manually

### Accessing Results
The unified data is stored in DuckDB at `/tmp/streaming_platforms.duckdb`:

```sql
-- View all content
SELECT * FROM streaming_data.unified_streaming_platforms;

-- Find content on multiple platforms
SELECT title, release_year, in_netflix, in_disney, in_hulu, in_prime
FROM streaming_data.unified_streaming_platforms
WHERE (in_netflix::int + in_disney::int + in_hulu::int + in_prime::int) > 1;

-- Platform-specific content counts
SELECT
    SUM(in_netflix::int) as netflix_count,
    SUM(in_disney::int) as disney_count,
    SUM(in_hulu::int) as hulu_count,
    SUM(in_prime::int) as prime_count
FROM streaming_data.unified_streaming_platforms;
```

## Dependencies

- `dlt[duckdb]>=0.4.0`
- `duckdb>=0.9.0`
- `pandas>=1.5.0`
- `dag-factory==1.0.0`

## Configuration

The DAG can be customized via the YAML configuration:
- Schedule interval
- Retry policies
- Resource allocation
- Output paths

## Monitoring

The DAG includes validation steps that output:
- Total record counts
- Platform-specific statistics
- Content type breakdowns
- Top genres analysis
- Data quality metrics
