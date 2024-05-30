from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .schemas import Database

# Default session timeout: 5 hours
DEFAULT_SESSION_TIMEOUT = 5*60*60

# Create the database engine and session maker
def create_database(uri, session_timeout: int = DEFAULT_SESSION_TIMEOUT):
    engine = create_engine(uri)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    return Database(engine=engine, session_maker=SessionLocal)