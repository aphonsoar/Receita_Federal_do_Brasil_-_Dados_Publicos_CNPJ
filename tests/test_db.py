from sqlalchemy import create_engine  # Optional, if not already imported
from sqlalchemy.orm import sessionmaker
import psycopg2
from datetime import datetime

from src.models.pydantic import AuditDB
from core.setup import get_sink_folder, setup_database
from utils.models import (
    insert_audit_if_processed_later, 
    delete_filename
)
from utils.logging import logger 

# Get the database object
database = setup_database()
engine = database.engine

# Filename to insert
filename = "data_file.txt"

if engine:
    delete_filename(database, filename)
    
    # Use the context manager to create a session
    with database.session_maker() as session:
        # Data to insert
        processed_at = datetime.now()
        downloaded_at = None  # Optional, set to None if not downloaded yet
        inserted_at = datetime.now()

        # Create a new Audit object with data
        new_audit_entry = AuditDB(
            audi_filename=filename,
            audi_processed_at=processed_at,
            audi_downloaded_at=downloaded_at,
            audi_inserted_at=inserted_at,
        )

        # Add the new object to the session
        session.add(new_audit_entry)

        # Commit the changes to the database
        session.commit()

    logger.info("Data inserted successfully!")
    
    insert_audit_if_processed_later(database, filename, datetime.now())
else:
    logger.info("Error connecting to the database!")