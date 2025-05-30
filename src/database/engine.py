"""
Database engine and session management setup for the application.

This module provides the `create_database` function to initialize the SQLAlchemy
engine and session maker, configured with connection pooling.
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import pool

from .schemas import Database

# Create the database engine and session maker
def create_database(uri: str) -> Database:
    """
    Creates and configures the SQLAlchemy database engine and session maker.

    The engine is configured with a connection pool (QueuePool) for efficient
    database connection management. Adjust pool_size, max_overflow, and
    pool_recycle as needed for the specific workload.

    Args:
        uri (str): The database connection URI (e.g., "postgresql://user:pass@host/dbname").

    Returns:
        Database: A NamedTuple containing the initialized 'engine' and 'session_maker'.
                  The 'engine' is the SQLAlchemy engine instance.
                  The 'session_maker' is a callable that creates new database sessions.
    """
    engine = create_engine(
        uri,
        poolclass=pool.QueuePool,   # Use connection pooling to manage database connections efficiently.
        pool_size=20,               # Number of connections to keep open in the pool.
        max_overflow=10,            # Number of additional connections that can be opened beyond pool_size.
        pool_recycle=3600           # Recycle connections after 3600 seconds (1 hour) to prevent stale connections.
    )
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    return Database(engine=engine, session_maker=SessionLocal)
