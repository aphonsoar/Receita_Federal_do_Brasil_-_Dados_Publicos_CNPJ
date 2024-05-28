from datetime import datetime
from typing import Union, List
from os import getcwd, path, listdir
from functools import reduce 
from uuid import uuid4

from sqlalchemy import or_, func

from models.pydantic import Database, AuditMetadata 
from models.database import AuditDB
from setup.logging import logger 
from utils.misc import list_zip_contents, invert_dict_list
from utils.etl import get_zip_to_tablename

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
                audi_id=uuid4(),
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
                    summary=f'Skipping create entry for file {filename}.'
                    explanation='Existing processed_at is later or equal.'
                    error_message=f"{summary} {explanation}"
                    logger.warn(error_message)
                    
                    return None

    else:
        logger.error("Error connecting to the database!")

def create_audits(database: Database, files_info: List) -> List:
    audits = []
    for file_info in files_info:    
        audit = create_audit(database, file_info.filename, file_info.updated_at)

        if audit:
            audits.append(audit)
            
    return audits

def insert_audit(
    database: Database, 
    new_audit: AuditDB
) -> Union[AuditDB, None]:
    """
    Inserts a new audit entry if the provided processed_at is later than the latest existing entry for the filename.

    Args:
        filename: The filename for the audit entry.
        new_processed_at: The new processed datetime value.
    """
    filename = new_audit.audi_filename
    if database.engine:
        with database.session_maker() as session:
            # Check if the new processed_at is greater
            
            if new_audit.is_precedence_met:
                session.add(new_audit)
                
                # Commit the changes to the database
                session.commit()
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

def insert_audits(database, new_audits: List):
    for new_audit in new_audits:
        try:
            insert_audit(database, new_audit)
        
        except Exception as e:
            summary=f"Error inserting audit for file {new_audit.audi_filename}"
            logger.error(f"{summary}: {e}")

def create_audit_metadata(
    database: Database,
    audits: List,
    to_path: str
):  
    """
    Creates audit metadata based on the provided database, files information, and destination path.

    Args:
        database (Database): The database object used for creating audits.
        files_info (List): A list of file information objects.
        to_path (str): The destination path for the files.

    Returns:
        AuditMetadata: An object containing the audit list, zip file dictionary, unzipped files list,
        zipped files list, and zipped file to tablename dictionary.
    """    
    # Traduzir arquivos zipados e seus conteúdos
    zip_file_dict = {
        zip_filename: [
            zip_file_content.filename
            for zip_file_content in list_zip_contents(path.join(to_path, zip_filename))
        ]
        for zip_filename in map(lambda audit: audit.audi_filename, audits)
    }

    # Arquivos
    zip_files = [ audit.audi_filename for audit in audits ]
    
    zipfiles_to_tablenames = get_zip_to_tablename(zip_file_dict)
    tablename_to_zipfile_dict = invert_dict_list(zipfiles_to_tablenames)

    tablename_to_zipfile_to_files = {
        tablename: {
            zipfile: zip_file_dict[zipfile]
            for zipfile in zipfiles
        }
        for tablename, zipfiles in tablename_to_zipfile_dict.items()
    }

    return AuditMetadata(
        audit_list=audits,
        tablename_to_zipfile_to_files=tablename_to_zipfile_to_files,
    )

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