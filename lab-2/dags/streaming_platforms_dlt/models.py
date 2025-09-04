"""
SQLAlchemy models for streaming platforms data
"""
from sqlalchemy import create_engine, Column, String, Integer, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from typing import Optional
import os

# Base class for all models
Base = declarative_base()

class StreamingTitle(Base):
    """
    Model representing a streaming platform title with cross-platform availability
    """
    __tablename__ = 'unified_streaming_platforms'

    # Primary key - using title + release_year for uniqueness
    show_id = Column(String, primary_key=True)

    # Basic title information
    type = Column(String, nullable=False)  # Movie or TV Show
    title = Column(String, nullable=False, index=True)
    director = Column(String, default='')
    cast = Column(Text, default='')  # Using Text for long cast lists
    country = Column(String, default='')
    release_year = Column(Integer, default=0, index=True)
    duration = Column(String, default='')
    listed_in = Column(Text, default='')  # Genres

    # Platform availability flags
    in_netflix = Column(Boolean, default=False, index=True)
    in_disney = Column(Boolean, default=False, index=True)
    in_hulu = Column(Boolean, default=False, index=True)
    in_prime = Column(Boolean, default=False, index=True)

    def __repr__(self):
        platforms = []
        if self.in_netflix: platforms.append("Netflix")
        if self.in_disney: platforms.append("Disney+")
        if self.in_hulu: platforms.append("Hulu")
        if self.in_prime: platforms.append("Prime")

        return f"<StreamingTitle(title='{self.title}', year={self.release_year}, platforms={platforms})>"

    @property
    def platform_count(self) -> int:
        """Number of platforms this title is available on"""
        return sum([self.in_netflix, self.in_disney, self.in_hulu, self.in_prime])

    @property
    def platforms_list(self) -> list[str]:
        """List of platform names where this title is available"""
        platforms = []
        if self.in_netflix: platforms.append("Netflix")
        if self.in_disney: platforms.append("Disney+")
        if self.in_hulu: platforms.append("Hulu")
        if self.in_prime: platforms.append("Prime Video")
        return platforms

    @property
    def genres_list(self) -> list[str]:
        """List of genres for this title"""
        if not self.listed_in:
            return []
        return [genre.strip() for genre in self.listed_in.split(',') if genre.strip()]

    @property
    def cast_list(self) -> list[str]:
        """List of cast members for this title"""
        if not self.cast:
            return []
        return [actor.strip() for actor in self.cast.split(',') if actor.strip()]

class DatabaseManager:
    """
    Database connection and session management
    """

    def __init__(self, database_path: str = '/tmp/streaming_platforms.duckdb'):
        self.database_path = database_path
        self.engine = None
        self.SessionLocal = None
        self._setup_database()

    def _setup_database(self):
        """Initialize database connection and create tables"""
        # Create DuckDB engine
        self.engine = create_engine(f'duckdb:///{self.database_path}', echo=False)

        # Create session factory
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

        # Create tables if they don't exist
        Base.metadata.create_all(bind=self.engine)

    def get_session(self):
        """Get a new database session"""
        return self.SessionLocal()

    def close(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()

# Convenience functions for common queries
class StreamingQueries:
    """
    Common queries for streaming platform data
    """

    @staticmethod
    def get_multi_platform_titles(session, min_platforms: int = 2):
        """Get titles available on multiple platforms"""
        return session.query(StreamingTitle).filter(
            (StreamingTitle.in_netflix.cast(Integer) +
             StreamingTitle.in_disney.cast(Integer) +
             StreamingTitle.in_hulu.cast(Integer) +
             StreamingTitle.in_prime.cast(Integer)) >= min_platforms
        ).order_by(
            (StreamingTitle.in_netflix.cast(Integer) +
             StreamingTitle.in_disney.cast(Integer) +
             StreamingTitle.in_hulu.cast(Integer) +
             StreamingTitle.in_prime.cast(Integer)).desc(),
            StreamingTitle.release_year.desc()
        )

    @staticmethod
    def get_platform_titles(session, platform: str, content_type: str = None):
        """Get titles for a specific platform"""
        platform_column_map = {
            'netflix': StreamingTitle.in_netflix,
            'disney': StreamingTitle.in_disney,
            'hulu': StreamingTitle.in_hulu,
            'prime': StreamingTitle.in_prime
        }

        if platform not in platform_column_map:
            raise ValueError(f"Unknown platform: {platform}")

        query = session.query(StreamingTitle).filter(
            platform_column_map[platform] == True
        )

        if content_type:
            query = query.filter(StreamingTitle.type == content_type)

        return query.order_by(StreamingTitle.release_year.desc())

    @staticmethod
    def get_recent_movies(session, platform: str = None, year_threshold: int = 2015):
        """Get recent movies, optionally filtered by platform"""
        query = session.query(StreamingTitle).filter(
            StreamingTitle.type == 'Movie',
            StreamingTitle.release_year > year_threshold
        )

        if platform:
            platform_column_map = {
                'netflix': StreamingTitle.in_netflix,
                'disney': StreamingTitle.in_disney,
                'hulu': StreamingTitle.in_hulu,
                'prime': StreamingTitle.in_prime
            }
            if platform in platform_column_map:
                query = query.filter(platform_column_map[platform] == True)

        return query.order_by(StreamingTitle.release_year.desc(), StreamingTitle.title)

    @staticmethod
    def get_titles_by_genre(session, genre: str):
        """Get titles by genre"""
        return session.query(StreamingTitle).filter(
            StreamingTitle.listed_in.contains(genre)
        ).order_by(StreamingTitle.release_year.desc())

    @staticmethod
    def get_platform_stats(session):
        """Get statistics for each platform"""
        from sqlalchemy import func

        stats = session.query(
            func.sum(StreamingTitle.in_netflix.cast(Integer)).label('netflix_count'),
            func.sum(StreamingTitle.in_disney.cast(Integer)).label('disney_count'),
            func.sum(StreamingTitle.in_hulu.cast(Integer)).label('hulu_count'),
            func.sum(StreamingTitle.in_prime.cast(Integer)).label('prime_count'),
            func.count(StreamingTitle.show_id).label('total_unique_titles')
        ).first()

        return {
            'netflix': stats.netflix_count,
            'disney': stats.disney_count,
            'hulu': stats.hulu_count,
            'prime': stats.prime_count,
            'total_unique': stats.total_unique_titles
        }
