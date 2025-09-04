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

from functions import load_csv_to_orm_direct, merge_titles_using_orm, validate_data_pure_orm, analyze_with_pure_orm, demonstrate_orm_power
import duckdb

def test_pipeline():
    """Test the complete pure ORM pipeline"""
    print("🚀 Testing Streaming Platforms Pure ORM Pipeline")
    print("=" * 60)

    # Override the database path for testing
    import functions
    functions.DUCKDB_PATH = '/tmp/test_streaming_platforms_pure_orm.duckdb'
    # Use the original datasets location
    functions.DATASETS_PATH = '/home/efazati/projects/airflow-lab/lab-2/datasets'

    try:
        # Test 1: Load individual CSV files using pure ORM
        print("\n📊 Step 1: Loading CSV files using Pure ORM")
        platforms = [
            ('netflix', 'netflix_titles.csv'),
            ('disney', 'disney_plus_titles.csv'),
            ('hulu', 'hulu_titles.csv'),
            ('prime', 'amazon_prime_titles.csv')
        ]

        # Clear existing data first
        from models import DatabaseManager, StreamingTitle
        db_manager = DatabaseManager(functions.DUCKDB_PATH)
        session = db_manager.get_session()
        session.query(StreamingTitle).delete()
        session.commit()
        session.close()
        db_manager.close()

        for platform_name, csv_file in platforms:
            print(f"  Loading {platform_name} data using ORM...")
            load_csv_to_orm_direct(platform_name, csv_file)
            print(f"  ✅ {platform_name} data loaded successfully")

        # Test 2: Merge duplicate titles using ORM
        print("\n🔗 Step 2: Merging duplicate titles using ORM")
        merge_titles_using_orm()
        print("  ✅ Title merging completed successfully")

        # Test 3: Validate data using pure ORM
        print("\n✅ Step 3: Validating data using Pure ORM")
        validate_data_pure_orm()

        # Test 4: Cross-platform analysis using pure ORM
        print("\n🏆 Step 4: Cross-platform analysis using Pure ORM")
        analyze_with_pure_orm()

        # Test 5: Demonstrate ORM capabilities
        print("\n🎯 Step 5: Demonstrating ORM capabilities")
        demonstrate_orm_power()

        # Test 6: Run some sample ORM queries
        print("\n🔍 Step 6: Sample ORM queries")

        # Use ORM instead of raw DuckDB connection
        db_manager = DatabaseManager(functions.DUCKDB_PATH)
        session = db_manager.get_session()

        # Query 1: Content on multiple platforms using ORM
        multi_platform_titles = session.query(StreamingTitle).filter(
            (StreamingTitle.in_netflix.cast(int) +
             StreamingTitle.in_disney.cast(int) +
             StreamingTitle.in_hulu.cast(int) +
             StreamingTitle.in_prime.cast(int)) > 1
        ).order_by(
            (StreamingTitle.in_netflix.cast(int) +
             StreamingTitle.in_disney.cast(int) +
             StreamingTitle.in_hulu.cast(int) +
             StreamingTitle.in_prime.cast(int)).desc(),
            StreamingTitle.title
        ).limit(10).all()

        if multi_platform_titles:
            print("\n🎬 Top content available on multiple platforms (ORM):")
            for title in multi_platform_titles:
                print(f"  • {title.title} ({title.release_year}) - {title.platform_count} platforms: {', '.join(title.platforms_list)}")

        # Query 2: Platform comparison using ORM
        from sqlalchemy import func
        stats = session.query(
            func.sum(StreamingTitle.in_netflix.cast(int)).label('netflix'),
            func.sum(StreamingTitle.in_disney.cast(int)).label('disney'),
            func.sum(StreamingTitle.in_hulu.cast(int)).label('hulu'),
            func.sum(StreamingTitle.in_prime.cast(int)).label('prime'),
            func.count(StreamingTitle.show_id).label('total')
        ).first()

        print(f"\n📈 Platform Statistics (ORM):")
        print(f"  • Netflix: {stats.netflix:,} titles")
        print(f"  • Disney+: {stats.disney:,} titles")
        print(f"  • Hulu: {stats.hulu:,} titles")
        print(f"  • Amazon Prime: {stats.prime:,} titles")
        print(f"  • Total unique titles: {stats.total:,}")

        session.close()
        db_manager.close()

        print("\n🎉 Pure ORM Pipeline test completed successfully!")
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
