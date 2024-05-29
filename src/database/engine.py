from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .schemas import Database

Base = declarative_base()

# Default session timeout: 30 minutes
DEFAULT_SESSION_TIMEOUT = 30*60

# Create the database engine and session maker
def create_database(uri, session_timeout: int = DEFAULT_SESSION_TIMEOUT):
    engine = create_engine(
        uri, 
        pool_pre_ping=True,
        pool_recycle=session_timeout,
        echo=True
    )
    SessionLocal = sessionmaker(
        autocommit=False, 
        autoflush=False, 
        bind=engine
    )

    return Database(engine=engine, session_maker=SessionLocal)