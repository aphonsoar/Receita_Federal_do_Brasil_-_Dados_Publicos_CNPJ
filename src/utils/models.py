from datetime import datetime
from typing import Union

from sqlalchemy import or_, func

from models.pydantic import Database 
from models.database import AuditDB
from setup.logging import logger 

def create_audit(
    database: Database, 
    filename: str, 
    current_updated_at: datetime
) -> Union[AuditDB, None]:
    """
    Inserts a new audit entry if the provided processed_at is later than the latest existing entry for the filename.

    Args:
        filename: The filename for the audit entry.
        new_processed_at: The new processed datetime value.
    """
    if database.engine:
        with database.session_maker() as session:
            # Get the latest processed_at for the filename
            latest_source_updated_at = func.max(AuditDB.audi_source_updated_at)
            is_filename = AuditDB.audi_filename == filename
            query = session.query(latest_source_updated_at)
            
            latest_updated_at = query.filter(is_filename).first()[0]

            new_audit = AuditDB(
                audi_filename=filename,
                audi_file_size_bytes=0,
                audi_source_updated_at=current_updated_at,
                audi_created_at=datetime.now(),                        
                audi_downloaded_at=None,
                audi_processed_at=None,
                audi_inserted_at=None,
            )
            
            # Check if the new processed_at is greater
            if latest_updated_at is None:
                # Create and insert the new entry
                return new_audit

            else:
                if(current_updated_at > latest_updated_at):
                    # Create and insert the new entry
                    return new_audit
                
                else:
                    summary='Skipping create entry for file {filename}.'
                    explanation='Existing processed_at is later or equal.'
                    error_message=f"{summary} {explanation}"
                    logger.warn(error_message)
                    
                    return None

    else:
        logger.error("Error connecting to the database!")

def insert_audit(
    database: Database, 
    filename: str, 
    new_audit: AuditDB
) -> Union[AuditDB, None]:
    """
    Inserts a new audit entry if the provided processed_at is later than the latest existing entry for the filename.

    Args:
        filename: The filename for the audit entry.
        new_processed_at: The new processed datetime value.
    """
    if database.engine:
        with database.session_maker() as session:
            # Check if the new processed_at is greater
            if new_audit.is_precedence_met():
                session.add(new_audit)                
            else:
                timestamps = [
                    new_audit.audi_created_at,
                    new_audit.audi_downloaded_at,
                    new_audit.audi_inserted_at,
                    new_audit.audi_inserted_at,
                ]
                error_message=f"Skipping insert audit for file {filename}. "
                logger.warn(error_message)
                
                return None

    else:
        logger.error("Error connecting to the database!")

def delete_filename_on_audit(database: Database, filename: str):
    """
    Delete entries from the database with a specific filename.

    Args:
        database (Database): The database object.
        filename (str): The filename to be deleted.

    Returns:
        None
    """
    if database.engine:
        # Use the context manager to create a session
        with database.session_maker() as session:
            # Delete entries (adjust filter as needed)
            is_filename = AuditDB.audi_filename == filename
            session.query(AuditDB).filter(is_filename).delete()

            # Commit the changes
            session.commit()
            
            logger.info(f"Deleted entries for filename {filename}.")
            
    else:
        logger.error("Error connecting to the database!")