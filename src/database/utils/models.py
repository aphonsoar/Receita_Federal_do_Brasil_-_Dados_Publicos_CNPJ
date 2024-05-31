from datetime import datetime
from typing import Union, List
from os import path
from uuid import uuid4

from sqlalchemy import func, text

from setup.logging import logger
from database.engine import Database 
from database.models import AuditDB
from utils.misc import invert_dict_list
from utils.zip import list_zip_contents
from core.utils.etl import get_zip_to_tablename
from core.schemas import FileGroupInfo, AuditMetadata

def create_new_audit(
    table_name:str, 
    filenames: List[str], 
    size_bytes:int, 
    update_at:datetime
) -> AuditDB:
    """
    Creates a new audit entry.

    Args:
        name (str): The name of the file.
        size_bytes (int): The size of the file in bytes.
        update_at (datetime): The datetime when the source was last updated.

    Returns:
        AuditDB: An audit entry object.
    """
    return AuditDB(
        audi_id=uuid4(),
        audi_created_at=datetime.now(),
        audi_table_name=table_name,
        audi_filenames=filenames,
        audi_file_size_bytes=size_bytes,
        audi_source_updated_at=update_at,
        audi_downloaded_at=None,
        audi_processed_at=None,
        audi_inserted_at=None,
    )

def create_audit(database: Database, file_group_info: FileGroupInfo) -> Union[AuditDB, None]:
    """
    Inserts a new audit entry if the provided processed_at is later than the latest existing entry for the filename.

    Args:
        filename: The filename for the audit entry.
        new_processed_at: The new processed datetime value.
    """
    if database.engine:
        with database.session_maker() as session:
            # NOTE: Uncomment this if you want to use SQLAlchemy ORM
            # # Get the latest processed_at for the filename
            # latest_source_updated_at = func.max(AuditDB.audi_source_updated_at)
            # is_table_name = AuditDB.audi_table_name == file_group_info.table_name
            # query = session.query(latest_source_updated_at)
            #
            # latest_updated_at = query.filter(is_table_name).first()[0]
            
            # Define raw SQL query
            sql_query = text(f"""SELECT max(audi_source_updated_at) 
                FROM public.audit 
                WHERE audi_table_name = \'{file_group_info.table_name}\';"""
            )
            
            # Execute query with parameters (optional)
            with database.engine.connect() as connection:
                result = connection.execute(sql_query)
            
                # Process results (e.g., fetchall, fetchone)
                latest_updated_at = result.fetchone()[0]
            
            # First entry: no existing audit entry
            if latest_updated_at is None:
                # Create and insert the new entry
                return create_new_audit(
                    file_group_info.table_name,
                    file_group_info.elements,
                    file_group_info.size_bytes, 
                    file_group_info.date_range[1]
                )

            # New entry: source updated_at is greater 
            elif(file_group_info.date_range[1] > latest_updated_at):
                # Create and insert the new entry
                return create_new_audit(
                    file_group_info.table_name, 
                    file_group_info.elements,
                    file_group_info.size_bytes, 
                    file_group_info.date_range[1]
                )
            
            # Not all files are updated in batch aka unreliable
            elif(file_group_info.date_diff() > 7):
                return None
            
            else:
                summary=f'Skipping create entry for file group {file_group_info.name}.'
                explanation='Existing processed_at is later or equal.'
                error_message=f"{summary} {explanation}"
                logger.warn(error_message)
                
                return None

    else:
        logger.error("Error connecting to the database!")

def create_audits(database: Database, files_info: List[FileGroupInfo]) -> List:
    """
    Creates a list of audit entries based on the provided database and files information.

    Args:
        database (Database): The database object.
        files_info (List): A list of file information objects.

    Returns:
        List: A list of audit entries.
    """
    
    audits = []
    for file_info in files_info:    
        audit = create_audit(database, file_info)

        if audit:
            audits.append(audit)
            
    return audits

def insert_audit(database: Database, new_audit: AuditDB) -> Union[AuditDB, None]:
    """
    Inserts a new audit entry if the provided processed_at is later than the latest existing entry for the filename.

    Args:
        filename: The filename for the audit entry.
        new_processed_at: The new processed datetime value.
    """
    table_name = new_audit.audi_table_name
    if database.engine:
        with database.session_maker() as session:
            # Check if the new processed_at is greater
            
            if new_audit.is_precedence_met:
                session.add(new_audit)
                
                # Commit the changes to the database
                session.commit()
            else:
                error_message=f"Skipping insert audit for table name {table_name}. "
                logger.warn(error_message)

                return None

    else:
        logger.error("Error connecting to the database!")

def insert_audits(database, new_audits: List[AuditDB]) -> None:
    """
    Inserts a list of new audits into the database.

    Args:
        database (Database): The database object.
        new_audits (List): A list of new audit entries.

    Returns:
        None
    """

    for new_audit in new_audits:
        try:
            insert_audit(database, new_audit)
        
        except Exception as e:
            summary=f"Error inserting audit for table {new_audit.audi_table_name}"
            logger.error(f"{summary}: {e}")

def create_audit_metadata(audits: List[AuditDB], to_path: str):  
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
            content.filename 
            for content in list_zip_contents(path.join(to_path, zip_filename))
        ]
        for audit in audits
        for zip_filename in audit.audi_filenames
    }

    # Arquivos
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

def delete_filename_on_audit(database: Database, table_name: str):
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
            is_table_name = AuditDB.audi_table_name == table_name
            session.query(AuditDB).filter(is_table_name).delete()

            # Commit the changes
            session.commit()
            
            logger.info(f"Deleted entries for table name {table_name}.")
            
    else:
        logger.error("Error connecting to the database!")