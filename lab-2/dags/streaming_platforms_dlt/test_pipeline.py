#!/usr/bin/env python3
"""
Test script for the streaming platforms dlt pipeline
Run this to test the pipeline functions outside of Airflow

Usage:
    python test_pipeline.py              # Run full pipeline test
    python test_pipeline.py --quick      # Run quick validation only
    python test_pipeline.py --clean      # Clean test database and exit
"""
import sys
import os
import argparse

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from functions import load_csv_to_dlt, create_unified_table, validate_data, analyze_cross_platform_rankings
import duckdb

def test_pipeline():
    """Test the complete pipeline"""
    print("🚀 Testing Streaming Platforms dlt Pipeline")
    print("=" * 50)

    # Override the database path for testing
    import functions
    functions.DUCKDB_PATH = '/tmp/test_streaming_platforms.duckdb'
    # Use the original datasets location
    functions.DATASETS_PATH = '/home/efazati/projects/airflow-lab/lab-2/datasets'

    # Set environment variable for DuckDB path
    os.environ['DESTINATION__DUCKDB__CREDENTIALS'] = functions.DUCKDB_PATH

    try:
        # Test 1: Load individual CSV files
        print("\n📊 Step 1: Loading CSV files to DuckDB using dlt")
        platforms = [
            ('netflix', 'netflix_titles.csv'),
            ('disney', 'disney_plus_titles.csv'),
            ('hulu', 'hulu_titles.csv'),
            ('prime', 'amazon_prime_titles.csv')
        ]

        for platform_name, csv_file in platforms:
            print(f"  Loading {platform_name} data...")
            load_csv_to_dlt(platform_name, csv_file)
            print(f"  ✅ {platform_name} data loaded successfully")

        # Test 2: Create unified table
        print("\n🔗 Step 2: Creating unified table")
        create_unified_table()
        print("  ✅ Unified table created successfully")

        # Test 3: Validate data
        print("\n✅ Step 3: Validating data")
        validate_data()

        # Test 4: Cross-platform rankings
        print("\n🏆 Step 4: Cross-platform rankings")
        analyze_cross_platform_rankings()

        # Test 5: Run some sample queries
        print("\n🔍 Step 5: Sample queries")
        conn = duckdb.connect(functions.DUCKDB_PATH)

        # Query 1: Content on multiple platforms
        multi_platform_query = """
        SELECT title, release_year,
               in_netflix, in_disney, in_hulu, in_prime,
               (in_netflix::int + in_disney::int + in_hulu::int + in_prime::int) as platform_count
        FROM main.unified_streaming_platforms
        WHERE (in_netflix::int + in_disney::int + in_hulu::int + in_prime::int) > 1
        ORDER BY platform_count DESC, title
        LIMIT 10
        """

        results = conn.execute(multi_platform_query).fetchall()
        if results:
            print("\n🎬 Top content available on multiple platforms:")
            for row in results:
                platforms_list = []
                if row[2]: platforms_list.append("Netflix")
                if row[3]: platforms_list.append("Disney+")
                if row[4]: platforms_list.append("Hulu")
                if row[5]: platforms_list.append("Prime")
                print(f"  • {row[0]} ({row[1]}) - {len(platforms_list)} platforms: {', '.join(platforms_list)}")

        # Query 2: Platform comparison
        platform_stats_query = """
        SELECT
            SUM(in_netflix::int) as netflix_titles,
            SUM(in_disney::int) as disney_titles,
            SUM(in_hulu::int) as hulu_titles,
            SUM(in_prime::int) as prime_titles,
            COUNT(*) as total_unique_titles
        FROM main.unified_streaming_platforms
        """

        stats = conn.execute(platform_stats_query).fetchone()
        print(f"\n📈 Platform Statistics:")
        print(f"  • Netflix: {stats[0]:,} titles")
        print(f"  • Disney+: {stats[1]:,} titles")
        print(f"  • Hulu: {stats[2]:,} titles")
        print(f"  • Amazon Prime: {stats[3]:,} titles")
        print(f"  • Total unique titles: {stats[4]:,}")

        conn.close()

        print("\n🎉 Pipeline test completed successfully!")
        print(f"📁 Test database saved at: {functions.DUCKDB_PATH}")

    except Exception as e:
        print(f"\n❌ Pipeline test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

    return True

def clean_test_database():
    """Clean up test database"""
    import functions
    test_db = '/tmp/test_streaming_platforms.duckdb'
    platform_dbs = [
        '/tmp/streaming_platforms_netflix.duckdb',
        '/tmp/streaming_platforms_disney.duckdb',
        '/tmp/streaming_platforms_hulu.duckdb',
        '/tmp/streaming_platforms_prime.duckdb'
    ]

    for db_path in [test_db] + platform_dbs:
        if os.path.exists(db_path):
            os.remove(db_path)
            print(f"🗑️  Removed {db_path}")

    print("✅ Test databases cleaned")

def quick_validate():
    """Quick validation of existing data"""
    import functions
    functions.DUCKDB_PATH = '/tmp/test_streaming_platforms.duckdb'

    if not os.path.exists(functions.DUCKDB_PATH):
        print("❌ No test database found. Run full test first.")
        return False

    print("🔍 Quick validation of existing data")
    try:
        validate_data()
        analyze_cross_platform_rankings()
        return True
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Test streaming platforms pipeline')
    parser.add_argument('--quick', action='store_true', help='Run quick validation only')
    parser.add_argument('--clean', action='store_true', help='Clean test databases and exit')

    args = parser.parse_args()

    if args.clean:
        clean_test_database()
        return

    if args.quick:
        success = quick_validate()
    else:
        success = test_pipeline()

    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
