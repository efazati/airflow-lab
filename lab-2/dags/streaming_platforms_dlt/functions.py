"""
Pure ORM approach - letting SQLAlchemy handle everything
"""
import pandas as pd
import dlt
import os
import sys
from typing import List, Dict, Any
from sqlalchemy import func, text, and_, or_
from sqlalchemy.orm import Session

# Add current directory to Python path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models import StreamingTitle, DatabaseManager, StreamingQueries

# Configuration
DATASETS_PATH = '/opt/airflow/datasets'
DUCKDB_PATH = '/tmp/streaming_platforms.duckdb'
PIPELINE_NAME = 'streaming_platforms'
DATASET_NAME = 'streaming_data'

def load_csv_to_orm_direct(platform_name: str, csv_filename: str, datasets_path: str = None) -> None:
    """
    Load CSV directly into ORM models - pure ORM approach
    Uses separate database files for each platform to avoid concurrency issues
    """
    print(f"Loading {platform_name} data from {csv_filename} directly to ORM")

    # Use provided path or default
    data_path = datasets_path or DATASETS_PATH
    csv_path = os.path.join(data_path, csv_filename)
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    # Read CSV
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} records from {csv_filename}")

    # Clean and prepare data
    df = df.fillna('')
    df['release_year'] = pd.to_numeric(df['release_year'], errors='coerce').fillna(0).astype(int)

    # Ensure all required columns exist
    required_columns = ['show_id', 'type', 'title', 'director', 'cast', 'country', 'release_year', 'duration', 'listed_in']
    for col in required_columns:
        if col not in df.columns:
            df[col] = ''

    # Use separate database file for each platform to avoid concurrency issues
    platform_db_path = f"/tmp/streaming_platforms_{platform_name}.duckdb"
    print(f"Using platform-specific database: {platform_db_path}")

    # Remove existing database file to ensure clean state
    if os.path.exists(platform_db_path):
        os.remove(platform_db_path)
        print(f"Removed existing database file: {platform_db_path}")

    # Initialize database and session with platform-specific database
    db_manager = DatabaseManager(platform_db_path)
    session = db_manager.get_session()

    try:
        # Convert DataFrame rows to ORM objects
        titles_to_add = []
        for _, row in df.iterrows():
            # Create platform-specific show_id to avoid conflicts between platforms
            platform_show_id = f"{platform_name}_{row.get('show_id', '')}"

            # Create StreamingTitle object
            title = StreamingTitle(
                show_id=platform_show_id,  # Use platform-specific ID
                type=row.get('type', ''),
                title=row.get('title', ''),
                director=row.get('director', ''),
                cast=row.get('cast', ''),
                country=row.get('country', ''),
                release_year=int(row.get('release_year', 0)),
                duration=row.get('duration', ''),
                listed_in=row.get('listed_in', ''),
                # Set platform flags
                in_netflix=(platform_name == 'netflix'),
                in_disney=(platform_name == 'disney'),
                in_hulu=(platform_name == 'hulu'),
                in_prime=(platform_name == 'prime')
            )
            titles_to_add.append(title)

        # Bulk insert using ORM
        session.add_all(titles_to_add)
        session.commit()

        # Ensure database is properly flushed
        session.flush()

        print(f"✅ Successfully inserted {len(titles_to_add)} {platform_name} titles using ORM")

        # Verify the file was created
        if os.path.exists(platform_db_path):
            file_size = os.path.getsize(platform_db_path)
            print(f"✅ Database file created: {platform_db_path} ({file_size} bytes)")
        else:
            print(f"⚠️  Database file not found after creation: {platform_db_path}")

    except Exception as e:
        session.rollback()
        print(f"❌ Error inserting {platform_name} data: {str(e)}")
        raise
    finally:
        session.close()
        db_manager.close()

def merge_titles_using_orm() -> None:
    """
    Merge titles from separate platform databases into unified database using ORM
    Reads from platform-specific databases and merges into main database
    """
    print("Merging titles from platform databases using ORM operations...")

    # Platform database paths
    platform_dbs = {
        'netflix': f"/tmp/streaming_platforms_netflix.duckdb",
        'disney': f"/tmp/streaming_platforms_disney.duckdb",
        'hulu': f"/tmp/streaming_platforms_hulu.duckdb",
        'prime': f"/tmp/streaming_platforms_prime.duckdb"
    }

    # Initialize main database
    main_db_manager = DatabaseManager(DUCKDB_PATH)
    main_session = main_db_manager.get_session()

    try:
        # Clear existing data in main database
        main_session.query(StreamingTitle).delete()
        main_session.commit()
        print("Cleared existing data from main database")

        all_titles = {}  # Dictionary to store titles by normalized name

        # Read data from each platform database
        for platform_name, db_path in platform_dbs.items():
            if not os.path.exists(db_path):
                print(f"⚠️  Platform database not found: {db_path}")
                continue

            print(f"Reading {platform_name} data from {db_path}")

            # Connect to platform database
            platform_db_manager = DatabaseManager(db_path)
            platform_session = platform_db_manager.get_session()

            try:
                # Get all titles from this platform
                platform_titles = platform_session.query(StreamingTitle).all()
                print(f"  Found {len(platform_titles)} titles in {platform_name} database")

                for title in platform_titles:
                    # Normalize title for merging
                    normalized_title = title.title.lower().strip()

                    if normalized_title in all_titles:
                        # Merge with existing title
                        existing = all_titles[normalized_title]

                        # Merge platform flags
                        if title.in_netflix: existing.in_netflix = True
                        if title.in_disney: existing.in_disney = True
                        if title.in_hulu: existing.in_hulu = True
                        if title.in_prime: existing.in_prime = True

                        # Merge other fields if existing has empty values
                        if not existing.director and title.director:
                            existing.director = title.director
                        if not existing.cast and title.cast:
                            existing.cast = title.cast
                        if not existing.country and title.country:
                            existing.country = title.country
                        if not existing.duration and title.duration:
                            existing.duration = title.duration
                        if not existing.listed_in and title.listed_in:
                            existing.listed_in = title.listed_in
                        if existing.release_year == 0 and title.release_year > 0:
                            existing.release_year = title.release_year

                    else:
                        # Create new title entry
                        new_title = StreamingTitle(
                            show_id=title.show_id,
                            type=title.type,
                            title=title.title,
                            director=title.director,
                            cast=title.cast,
                            country=title.country,
                            release_year=title.release_year,
                            duration=title.duration,
                            listed_in=title.listed_in,
                            in_netflix=title.in_netflix,
                            in_disney=title.in_disney,
                            in_hulu=title.in_hulu,
                            in_prime=title.in_prime
                        )
                        all_titles[normalized_title] = new_title

            finally:
                platform_session.close()
                platform_db_manager.close()

        # Insert all merged titles into main database
        print(f"Inserting {len(all_titles)} unique titles into main database...")
        main_session.add_all(all_titles.values())
        main_session.commit()

        # Show statistics
        total_titles = len(all_titles)
        multi_platform_count = sum(1 for title in all_titles.values() if title.platform_count > 1)

        print(f"✅ Merge complete!")
        print(f"   Total unique titles: {total_titles:,}")
        print(f"   Multi-platform titles: {multi_platform_count:,}")

        # Show some merged examples
        print("\n🎬 Sample multi-platform titles:")
        multi_platform_titles = [title for title in all_titles.values() if title.platform_count > 1][:5]

        for title in multi_platform_titles:
            print(f"  • {title.title} ({title.release_year}) - Available on: {', '.join(title.platforms_list)}")

    except Exception as e:
        main_session.rollback()
        print(f"❌ Error during ORM merge: {str(e)}")
        raise
    finally:
        main_session.close()
        main_db_manager.close()

def validate_data_pure_orm() -> None:
    """
    Data validation using only ORM queries
    """
    print("Validating data using pure ORM...")

    db_manager = DatabaseManager(DUCKDB_PATH)
    session = db_manager.get_session()

    try:
        # Basic statistics
        total_titles = session.query(StreamingTitle).count()
        movies = session.query(StreamingTitle).filter(StreamingTitle.type == 'Movie').count()
        tv_shows = session.query(StreamingTitle).filter(StreamingTitle.type == 'TV Show').count()

        print(f"\n=== Pure ORM Statistics ===")
        print(f"Total titles: {total_titles:,}")
        print(f"Movies: {movies:,}")
        print(f"TV Shows: {tv_shows:,}")

        # Platform statistics
        netflix_count = session.query(StreamingTitle).filter(StreamingTitle.in_netflix == True).count()
        disney_count = session.query(StreamingTitle).filter(StreamingTitle.in_disney == True).count()
        hulu_count = session.query(StreamingTitle).filter(StreamingTitle.in_hulu == True).count()
        prime_count = session.query(StreamingTitle).filter(StreamingTitle.in_prime == True).count()

        print(f"\n=== Platform Statistics ===")
        print(f"Netflix: {netflix_count:,}")
        print(f"Disney+: {disney_count:,}")
        print(f"Hulu: {hulu_count:,}")
        print(f"Prime: {prime_count:,}")

        # Cross-platform analysis using simpler ORM queries
        multi_platform_counts = {}

        # Single platform titles
        netflix_only = session.query(StreamingTitle).filter(
            and_(StreamingTitle.in_netflix == True,
                 StreamingTitle.in_disney == False,
                 StreamingTitle.in_hulu == False,
                 StreamingTitle.in_prime == False)
        ).count()

        disney_only = session.query(StreamingTitle).filter(
            and_(StreamingTitle.in_netflix == False,
                 StreamingTitle.in_disney == True,
                 StreamingTitle.in_hulu == False,
                 StreamingTitle.in_prime == False)
        ).count()

        hulu_only = session.query(StreamingTitle).filter(
            and_(StreamingTitle.in_netflix == False,
                 StreamingTitle.in_disney == False,
                 StreamingTitle.in_hulu == True,
                 StreamingTitle.in_prime == False)
        ).count()

        prime_only = session.query(StreamingTitle).filter(
            and_(StreamingTitle.in_netflix == False,
                 StreamingTitle.in_disney == False,
                 StreamingTitle.in_hulu == False,
                 StreamingTitle.in_prime == True)
        ).count()

        single_platform = netflix_only + disney_only + hulu_only + prime_only
        multi_platform_counts["Single platform"] = single_platform

        # Multi-platform titles (any title on 2+ platforms)
        multi_platform = session.query(StreamingTitle).filter(
            or_(
                and_(StreamingTitle.in_netflix == True,
                     or_(StreamingTitle.in_disney == True, StreamingTitle.in_hulu == True, StreamingTitle.in_prime == True)),
                and_(StreamingTitle.in_disney == True,
                     or_(StreamingTitle.in_hulu == True, StreamingTitle.in_prime == True)),
                and_(StreamingTitle.in_hulu == True, StreamingTitle.in_prime == True)
            )
        ).count()

        multi_platform_counts["Multiple platforms"] = multi_platform

        print(f"\n=== Cross-Platform Distribution ===")
        for category, count in multi_platform_counts.items():
            print(f"{category}: {count:,}")

        # Recent movies by platform
        print(f"\n=== Recent Movies (2020+) by Platform ===")
        platforms = [
            ('Netflix', StreamingTitle.in_netflix),
            ('Disney+', StreamingTitle.in_disney),
            ('Hulu', StreamingTitle.in_hulu),
            ('Prime', StreamingTitle.in_prime)
        ]

        for platform_name, platform_col in platforms:
            count = session.query(StreamingTitle).filter(
                platform_col == True,
                StreamingTitle.type == 'Movie',
                StreamingTitle.release_year >= 2020
            ).count()
            print(f"{platform_name}: {count:,} recent movies")

    except Exception as e:
        print(f"❌ Error during pure ORM validation: {str(e)}")
        raise
    finally:
        session.close()
        db_manager.close()

def analyze_with_pure_orm() -> None:
    """
    Advanced analysis using only ORM
    """
    print("Advanced analysis using pure ORM...")

    db_manager = DatabaseManager(DUCKDB_PATH)
    session = db_manager.get_session()

    try:
        # Top multi-platform titles (using simplified filter)
        print(f"\n=== Top Multi-Platform Titles ===")
        multi_platform_titles = session.query(StreamingTitle).filter(
            or_(
                and_(StreamingTitle.in_netflix == True,
                     or_(StreamingTitle.in_disney == True, StreamingTitle.in_hulu == True, StreamingTitle.in_prime == True)),
                and_(StreamingTitle.in_disney == True,
                     or_(StreamingTitle.in_hulu == True, StreamingTitle.in_prime == True)),
                and_(StreamingTitle.in_hulu == True, StreamingTitle.in_prime == True)
            )
        ).order_by(
            StreamingTitle.release_year.desc()
        ).limit(10).all()

        for i, title in enumerate(multi_platform_titles, 1):
            print(f"{i:2d}. {title.title} ({title.release_year}) - {title.platform_count} platforms: {', '.join(title.platforms_list)}")

        # Directors with most titles (simplified without platform averaging)
        print(f"\n=== Top Directors by Title Count ===")
        directors = session.query(
            StreamingTitle.director,
            func.count(StreamingTitle.show_id).label('title_count')
        ).filter(
            StreamingTitle.director != '',
            StreamingTitle.director.isnot(None)
        ).group_by(
            StreamingTitle.director
        ).having(
            func.count(StreamingTitle.show_id) >= 3
        ).order_by(
            text('title_count DESC')
        ).limit(10).all()

        for director, count in directors:
            print(f"  • {director}: {count} titles")

    except Exception as e:
        print(f"❌ Error during pure ORM analysis: {str(e)}")
        raise
    finally:
        session.close()
        db_manager.close()

# Advanced ORM examples
def demonstrate_orm_power() -> None:
    """
    Show off what pure ORM can do
    """
    print("🚀 Demonstrating Pure ORM Power...")

    db_manager = DatabaseManager(DUCKDB_PATH)
    session = db_manager.get_session()

    try:
        # Complex query: Find movies that are Netflix exclusive but also have high director/cast info
        print(f"\n=== Netflix Exclusive Movies with Rich Metadata ===")
        netflix_exclusive_rich = session.query(StreamingTitle).filter(
            and_(
                StreamingTitle.in_netflix == True,
                StreamingTitle.in_disney == False,
                StreamingTitle.in_hulu == False,
                StreamingTitle.in_prime == False,
                StreamingTitle.type == 'Movie',
                StreamingTitle.director != '',
                StreamingTitle.cast != '',
                func.length(StreamingTitle.cast) > 50  # Rich cast info
            )
        ).order_by(StreamingTitle.release_year.desc()).limit(5).all()

        for movie in netflix_exclusive_rich:
            print(f"  • {movie.title} ({movie.release_year})")
            print(f"    Director: {movie.director}")
            print(f"    Cast: {', '.join(movie.cast_list[:3])}...")

        # Another complex query: Genre analysis
        print(f"\n=== Action Movies Across Platforms ===")
        action_movies_by_platform = {}
        platforms = [
            ('Netflix', StreamingTitle.in_netflix),
            ('Disney+', StreamingTitle.in_disney),
            ('Hulu', StreamingTitle.in_hulu),
            ('Prime', StreamingTitle.in_prime)
        ]

        for platform_name, platform_col in platforms:
            count = session.query(StreamingTitle).filter(
                and_(
                    platform_col == True,
                    StreamingTitle.type == 'Movie',
                    StreamingTitle.listed_in.contains('Action')
                )
            ).count()
            action_movies_by_platform[platform_name] = count

        for platform, count in action_movies_by_platform.items():
            print(f"  • {platform}: {count} action movies")

    except Exception as e:
        print(f"❌ Error in ORM demonstration: {str(e)}")
        raise
    finally:
        session.close()
        db_manager.close()
