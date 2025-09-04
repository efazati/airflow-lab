"""
Functions for streaming platforms data pipeline using dlt
"""
import pandas as pd
import dlt
import duckdb
import os
from typing import List, Dict, Any

# Configuration
DATASETS_PATH = '/opt/airflow/datasets'  # Use original datasets folder
DUCKDB_PATH = '/tmp/streaming_platforms.duckdb'
PIPELINE_NAME = 'streaming_platforms'
DATASET_NAME = 'streaming_data'

def load_csv_to_dlt(platform_name: str, csv_filename: str, datasets_path: str = None) -> None:
    """
    Load a single CSV file to DuckDB using dlt

    Args:
        platform_name: Name of the streaming platform (netflix, disney, hulu, prime)
        csv_filename: Name of the CSV file to load
        datasets_path: Optional override for datasets path
    """
    print(f"Loading {platform_name} data from {csv_filename}")

    # Use provided path or default
    data_path = datasets_path or DATASETS_PATH
    csv_path = os.path.join(data_path, csv_filename)
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} records from {csv_filename}")

    # Add platform indicator column
    df[f'in_{platform_name}'] = True

    # Ensure all required columns exist and handle missing values
    required_columns = ['show_id', 'type', 'title', 'director', 'cast', 'country', 'release_year', 'duration', 'listed_in']
    for col in required_columns:
        if col not in df.columns:
            df[col] = ''

    # Clean and prepare data
    df = df.fillna('')  # Replace NaN with empty strings

    # Ensure release_year is numeric
    df['release_year'] = pd.to_numeric(df['release_year'], errors='coerce').fillna(0).astype(int)

    # Use separate database file for each platform to avoid locking conflicts
    platform_db_path = f"/tmp/streaming_platforms_{platform_name}.duckdb"
    os.environ['DESTINATION__DUCKDB__CREDENTIALS'] = platform_db_path

    # Create dlt pipeline with DuckDB destination
    pipeline = dlt.pipeline(
        pipeline_name=f"{PIPELINE_NAME}_{platform_name}",
        destination="duckdb",
        dataset_name=DATASET_NAME
    )

    # Select only the columns we need for the final schema plus the platform indicator
    columns_to_keep = required_columns + [f'in_{platform_name}']
    df_filtered = df[columns_to_keep]

    # Convert DataFrame to records for dlt
    data = df_filtered.to_dict('records')

    # Run the pipeline
    load_info = pipeline.run(data, table_name=f"{platform_name}_titles")
    print(f"Successfully loaded {len(data)} records for {platform_name}")
    print(f"Load info: {load_info}")

def create_unified_table() -> None:
    """
    Create a unified table combining all streaming platform data
    with the specified schema and platform indicators
    """
    print("Creating unified streaming platforms table")

    # Connect to the main unified DuckDB database
    conn = duckdb.connect(DUCKDB_PATH)

    # Platform database paths
    platform_dbs = {
        'netflix': f"/tmp/streaming_platforms_netflix.duckdb",
        'disney': f"/tmp/streaming_platforms_disney.duckdb",
        'hulu': f"/tmp/streaming_platforms_hulu.duckdb",
        'prime': f"/tmp/streaming_platforms_prime.duckdb"
    }

    try:
        # Attach all platform databases to access their tables
        for platform, db_path in platform_dbs.items():
            if os.path.exists(db_path):
                conn.execute(f"ATTACH '{db_path}' AS {platform}_db")
                print(f"Attached {platform} database: {db_path}")
            else:
                print(f"Warning: {platform} database not found: {db_path}")

        # Create the unified table with proper schema
        # Merge titles by title name, combining platform availability
        create_table_sql = f"""
        CREATE OR REPLACE TABLE main.unified_streaming_platforms AS
        WITH all_titles AS (
            -- Netflix titles
            SELECT
                show_id,
                type,
                title,
                director,
                "cast",
                country,
                release_year,
                duration,
                listed_in,
                true as in_netflix,
                false as in_disney,
                false as in_hulu,
                false as in_prime
            FROM netflix_db.{DATASET_NAME}.netflix_titles

            UNION ALL

            -- Disney titles
            SELECT
                show_id,
                type,
                title,
                director,
                "cast",
                country,
                release_year,
                duration,
                listed_in,
                false as in_netflix,
                true as in_disney,
                false as in_hulu,
                false as in_prime
            FROM disney_db.{DATASET_NAME}.disney_titles

            UNION ALL

            -- Hulu titles
            SELECT
                show_id,
                type,
                title,
                director,
                "cast",
                country,
                release_year,
                duration,
                listed_in,
                false as in_netflix,
                false as in_disney,
                true as in_hulu,
                false as in_prime
            FROM hulu_db.{DATASET_NAME}.hulu_titles

            UNION ALL

            -- Prime titles
            SELECT
                show_id,
                type,
                title,
                director,
                "cast",
                country,
                release_year,
                duration,
                listed_in,
                false as in_netflix,
                false as in_disney,
                false as in_hulu,
                true as in_prime
            FROM prime_db.{DATASET_NAME}.prime_titles
        )
        SELECT
            -- Use the first non-empty show_id for merged titles
            FIRST_VALUE(CASE WHEN show_id != '' THEN show_id ELSE NULL END IGNORE NULLS)
                OVER (PARTITION BY LOWER(TRIM(title)) ORDER BY show_id) as show_id,
            -- Use the first non-empty value for other fields, prioritizing more complete records
            FIRST_VALUE(type) OVER (PARTITION BY LOWER(TRIM(title)) ORDER BY
                CASE WHEN type != '' THEN 0 ELSE 1 END, show_id) as type,
            -- Keep the original title (not lowercased)
            FIRST_VALUE(title) OVER (PARTITION BY LOWER(TRIM(title)) ORDER BY
                LENGTH(title) DESC, show_id) as title,
            -- Merge director info (take the most complete one)
            FIRST_VALUE(director) OVER (PARTITION BY LOWER(TRIM(title)) ORDER BY
                LENGTH(director) DESC, show_id) as director,
            -- Merge cast info (take the most complete one)
            FIRST_VALUE("cast") OVER (PARTITION BY LOWER(TRIM(title)) ORDER BY
                LENGTH("cast") DESC, show_id) as "cast",
            -- Merge country info (take the most complete one)
            FIRST_VALUE(country) OVER (PARTITION BY LOWER(TRIM(title)) ORDER BY
                LENGTH(country) DESC, show_id) as country,
            -- Use the most recent/reliable release year
            FIRST_VALUE(release_year) OVER (PARTITION BY LOWER(TRIM(title)) ORDER BY
                CASE WHEN release_year > 0 THEN 0 ELSE 1 END, release_year DESC, show_id) as release_year,
            -- Merge duration info (take the most complete one)
            FIRST_VALUE(duration) OVER (PARTITION BY LOWER(TRIM(title)) ORDER BY
                LENGTH(duration) DESC, show_id) as duration,
            -- Merge genre info (take the most complete one)
            FIRST_VALUE(listed_in) OVER (PARTITION BY LOWER(TRIM(title)) ORDER BY
                LENGTH(listed_in) DESC, show_id) as listed_in,
            -- Aggregate platform availability - if ANY record has the platform, set to true
            MAX(in_netflix::int) OVER (PARTITION BY LOWER(TRIM(title)))::boolean as in_netflix,
            MAX(in_disney::int) OVER (PARTITION BY LOWER(TRIM(title)))::boolean as in_disney,
            MAX(in_hulu::int) OVER (PARTITION BY LOWER(TRIM(title)))::boolean as in_hulu,
            MAX(in_prime::int) OVER (PARTITION BY LOWER(TRIM(title)))::boolean as in_prime,
            -- Add row number to deduplicate
            ROW_NUMBER() OVER (PARTITION BY LOWER(TRIM(title)) ORDER BY
                LENGTH(director) + LENGTH("cast") + LENGTH(country) + LENGTH(listed_in) DESC,
                show_id) as rn
        FROM all_titles
        WHERE title IS NOT NULL AND TRIM(title) != ''
        QUALIFY rn = 1  -- Keep only the best record for each title
        """

        conn.execute(create_table_sql)

        # Get count of unified records
        result = conn.execute("SELECT COUNT(*) FROM main.unified_streaming_platforms").fetchone()
        print(f"Created unified table with {result[0]} records")

        # Show sample of the data with multiple platforms
        sample_data = conn.execute("""
            SELECT title, release_year, in_netflix, in_disney, in_hulu, in_prime
            FROM main.unified_streaming_platforms
            WHERE (in_netflix::int + in_disney::int + in_hulu::int + in_prime::int) > 1
            LIMIT 5
        """).fetchall()

        if sample_data:
            print("Sample records available on multiple platforms:")
            for row in sample_data:
                platforms = []
                if row[2]: platforms.append("Netflix")
                if row[3]: platforms.append("Disney+")
                if row[4]: platforms.append("Hulu")
                if row[5]: platforms.append("Prime")
                print(f"  {row[0]} ({row[1]}) - Available on: {', '.join(platforms)}")
        else:
            print("No titles found on multiple platforms")

    except Exception as e:
        print(f"Error creating unified table: {str(e)}")
        raise
    finally:
        conn.close()

def validate_data() -> None:
    """
    Validate the loaded data and print summary statistics
    """
    print("Validating loaded data...")

    conn = duckdb.connect(DUCKDB_PATH)

    try:
        # Get summary statistics
        summary_sql = """
        SELECT
            'Total Records' as metric, COUNT(*) as value
        FROM main.unified_streaming_platforms
        UNION ALL
        SELECT 'Netflix Only', COUNT(*)
        FROM main.unified_streaming_platforms
        WHERE in_netflix AND NOT in_disney AND NOT in_hulu AND NOT in_prime
        UNION ALL
        SELECT 'Disney Only', COUNT(*)
        FROM main.unified_streaming_platforms
        WHERE in_disney AND NOT in_netflix AND NOT in_hulu AND NOT in_prime
        UNION ALL
        SELECT 'Hulu Only', COUNT(*)
        FROM main.unified_streaming_platforms
        WHERE in_hulu AND NOT in_netflix AND NOT in_disney AND NOT in_prime
        UNION ALL
        SELECT 'Prime Only', COUNT(*)
        FROM main.unified_streaming_platforms
        WHERE in_prime AND NOT in_netflix AND NOT in_disney AND NOT in_hulu
        UNION ALL
        SELECT 'Multiple Platforms', COUNT(*)
        FROM main.unified_streaming_platforms
        WHERE (in_netflix::int + in_disney::int + in_hulu::int + in_prime::int) > 1
        """

        results = conn.execute(summary_sql).fetchall()

        print("\n=== Data Summary ===")
        for metric, value in results:
            print(f"{metric}: {value}")

        # Show content type breakdown
        content_sql = """
        SELECT type, COUNT(*) as count
        FROM main.unified_streaming_platforms
        GROUP BY type
        ORDER BY count DESC
        """

        content_results = conn.execute(content_sql).fetchall()
        print("\n=== Content Type Breakdown ===")
        for content_type, count in content_results:
            print(f"{content_type}: {count}")

        # Show top genres
        genres_sql = """
        WITH genres_unnested AS (
            SELECT TRIM(genre) as genre
            FROM main.unified_streaming_platforms,
            UNNEST(string_split(listed_in, ',')) as t(genre)
            WHERE listed_in != ''
        )
        SELECT genre, COUNT(*) as count
        FROM genres_unnested
        WHERE genre != ''
        GROUP BY genre
        ORDER BY count DESC
        LIMIT 10
        """

        genre_results = conn.execute(genres_sql).fetchall()
        print("\n=== Top 10 Genres ===")
        for genre, count in genre_results:
            print(f"{genre}: {count}")

    except Exception as e:
        print(f"Error during validation: {str(e)}")
        raise
    finally:
        conn.close()

def analyze_cross_platform_rankings() -> None:
    """
    Analyze cross-platform content and show rankings for each network
    and highlight the best movies available on all platforms
    """
    print("Analyzing cross-platform rankings...")

    conn = duckdb.connect(DUCKDB_PATH)

    try:
        # Show platform-specific rankings (top 10 movies by release year for each platform)
        print("\n=== Top 10 Recent Movies by Platform ===")

        platforms = [
            ('Netflix', 'in_netflix'),
            ('Disney+', 'in_disney'),
            ('Hulu', 'in_hulu'),
            ('Prime Video', 'in_prime')
        ]

        for platform_name, platform_col in platforms:
            platform_sql = f"""
            SELECT title, release_year, director, listed_in
            FROM main.unified_streaming_platforms
            WHERE {platform_col} = true
                AND type = 'Movie'
                AND release_year > 2015
                AND title != ''
            ORDER BY release_year DESC, title
            LIMIT 10
            """

            results = conn.execute(platform_sql).fetchall()
            print(f"\n🎬 {platform_name}:")
            for i, (title, year, director, genres) in enumerate(results, 1):
                director_info = f" - {director}" if director and director != '' else ""
                print(f"  {i:2d}. {title} ({year}){director_info}")

        # Find movies available on ALL platforms
        print("\n" + "="*60)
        print("🌟 TOP 5 MOVIES AVAILABLE ON ALL PLATFORMS 🌟")
        print("="*60)

        all_platforms_sql = """
        SELECT
            title,
            release_year,
            director,
            "cast",
            listed_in,
            -- Calculate a popularity score based on multiple factors
            (release_year * 0.1) +
            (LENGTH(listed_in) * 0.5) +
            (CASE WHEN director != '' THEN 10 ELSE 0 END) +
            (CASE WHEN "cast" != '' THEN 5 ELSE 0 END) as popularity_score
        FROM main.unified_streaming_platforms
        WHERE in_netflix = true
            AND in_disney = true
            AND in_hulu = true
            AND in_prime = true
            AND type = 'Movie'
            AND title != ''
        ORDER BY popularity_score DESC, release_year DESC, title
        LIMIT 5
        """

        all_platform_results = conn.execute(all_platforms_sql).fetchall()

        if all_platform_results:
            for i, (title, year, director, cast, genres, score) in enumerate(all_platform_results, 1):
                print(f"\n🏆 #{i}: {title} ({year})")
                if director and director != '':
                    print(f"    Director: {director}")
                if cast and cast != '':
                    # Show first few cast members
                    cast_list = [c.strip() for c in cast.split(',')[:3]]
                    print(f"    Starring: {', '.join(cast_list)}")
                if genres and genres != '':
                    genre_list = [g.strip() for g in genres.split(',')[:3]]
                    print(f"    Genres: {', '.join(genre_list)}")
                print(f"    ✅ Available on: Netflix, Disney+, Hulu, Prime Video")
        else:
            print("❌ No movies found that are available on ALL four platforms")

            # Show movies available on 3 platforms as alternative
            print("\n🎯 TOP 5 MOVIES AVAILABLE ON 3+ PLATFORMS:")
            three_platforms_sql = """
            SELECT
                title,
                release_year,
                director,
                listed_in,
                in_netflix,
                in_disney,
                in_hulu,
                in_prime,
                (in_netflix::int + in_disney::int + in_hulu::int + in_prime::int) as platform_count,
                (release_year * 0.1) + (LENGTH(listed_in) * 0.5) +
                (CASE WHEN director != '' THEN 10 ELSE 0 END) as popularity_score
            FROM main.unified_streaming_platforms
            WHERE (in_netflix::int + in_disney::int + in_hulu::int + in_prime::int) >= 3
                AND type = 'Movie'
                AND title != ''
            ORDER BY platform_count DESC, popularity_score DESC, release_year DESC
            LIMIT 5
            """

            three_platform_results = conn.execute(three_platforms_sql).fetchall()

            for i, (title, year, director, genres, netflix, disney, hulu, prime, count, score) in enumerate(three_platform_results, 1):
                platforms_available = []
                if netflix: platforms_available.append("Netflix")
                if disney: platforms_available.append("Disney+")
                if hulu: platforms_available.append("Hulu")
                if prime: platforms_available.append("Prime Video")

                print(f"\n🥉 #{i}: {title} ({year}) - On {count} platforms")
                if director and director != '':
                    print(f"    Director: {director}")
                if genres and genres != '':
                    genre_list = [g.strip() for g in genres.split(',')[:3]]
                    print(f"    Genres: {', '.join(genre_list)}")
                print(f"    📺 Available on: {', '.join(platforms_available)}")

        # Show overall statistics
        print("\n" + "="*50)
        print("📊 CROSS-PLATFORM STATISTICS")
        print("="*50)

        stats_sql = """
        SELECT
            'All 4 Platforms' as category,
            COUNT(*) as movie_count
        FROM main.unified_streaming_platforms
        WHERE in_netflix AND in_disney AND in_hulu AND in_prime AND type = 'Movie'

        UNION ALL

        SELECT
            '3 Platforms' as category,
            COUNT(*) as movie_count
        FROM main.unified_streaming_platforms
        WHERE (in_netflix::int + in_disney::int + in_hulu::int + in_prime::int) = 3 AND type = 'Movie'

        UNION ALL

        SELECT
            '2 Platforms' as category,
            COUNT(*) as movie_count
        FROM main.unified_streaming_platforms
        WHERE (in_netflix::int + in_disney::int + in_hulu::int + in_prime::int) = 2 AND type = 'Movie'

        UNION ALL

        SELECT
            'Single Platform Only' as category,
            COUNT(*) as movie_count
        FROM main.unified_streaming_platforms
        WHERE (in_netflix::int + in_disney::int + in_hulu::int + in_prime::int) = 1 AND type = 'Movie'
        """

        stats_results = conn.execute(stats_sql).fetchall()

        for category, count in stats_results:
            print(f"{category:20s}: {count:,} movies")

    except Exception as e:
        print(f"Error during cross-platform analysis: {str(e)}")
        raise
    finally:
        conn.close()
