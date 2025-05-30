"""
Utility functions for interacting with database models, particularly the AuditDB model.

This module provides functions for creating, querying, and inserting audit records,
as well as for generating metadata structures (AuditMetadata) used in the ETL process.
It helps in tracking the state and history of data processing for different tables.
"""
from datetime import datetime
from typing import Union, List, Optional # Added Optional
from os import path
from uuid import uuid4

from sqlalchemy import text
import pytz

from setup.logging import logger
from database.engine import Database 
from database.models import AuditDB
from utils.misc import invert_dict_list
from utils.zip import list_zip_contents
from core.utils.etl import get_zip_to_tablename
from core.schemas import FileGroupInfo, AuditMetadata, AuditDBSchema

def create_new_audit(
    table_name: str, 
    filenames: List[str], 
    size_bytes: int, 
    update_at: datetime
) -> AuditDB:
    """
    Creates a new audit entry.

    Args:
        table_name (str): The name of the table the audit entry is for.
        filenames (List[str]): List of filenames associated with this audit entry
                               (e.g., parts of a table split into multiple files).
        size_bytes (int): Total size of the file(s) in bytes.
        update_at (datetime): The timestamp indicating when the source data was last updated.
                              This is typically derived from the scraped file information.

    Returns:
        AuditDB: A new `AuditDB` SQLAlchemy model instance, not yet persisted to the database.
    """
    return AuditDB(
        audi_id=str(uuid4()),               # Generate a new unique ID
        audi_created_at=datetime.now(),     # Set creation timestamp to current time
        audi_table_name=table_name,
        audi_filenames=filenames,
        audi_file_size_bytes=size_bytes,
        audi_source_updated_at=update_at,   # Timestamp from the source data
        audi_downloaded_at=None,            # To be filled when data is downloaded
        audi_processed_at=None,             # To be filled when data is processed/extracted
        audi_inserted_at=None,              # To be filled when data is inserted into the main table
    )

def get_latest_updated_at(database: Database, table_name: str) -> Optional[datetime]:
    """
    Retrieves the latest `audi_source_updated_at` timestamp from the audit table
    for a given table name.

    This is used to determine if new data files are more recent than what has
    already been processed.

    Args:
        database (Database): The database connection object.
        table_name (str): The name of the table to check in the audit log.

    Returns:
        Optional[datetime]: The latest `audi_source_updated_at` timestamp as a
                            timezone-aware datetime object (America/Sao_Paulo),
                            or None if no entry is found for the table.
    """
    sql_query = text(f"""
        SELECT MAX(audi_source_updated_at)
        FROM public.audit 
        WHERE audi_table_name = :table_name;
    """)
    
    with database.engine.connect() as connection:
        result = connection.execute(sql_query, {'table_name': table_name})
        latest_updated_at = result.fetchone()[0]
    
    if latest_updated_at:
        sao_paulo_timezone = pytz.timezone("America/Sao_Paulo")
        latest_updated_at = sao_paulo_timezone.localize(latest_updated_at)
    
    return latest_updated_at

def create_audit(database: Database, file_group_info: FileGroupInfo) -> Union[AuditDB, None]:
    """
    Inserts a new audit entry if the provided processed_at is later than the latest existing entry for the filename.

    Args:
        database (Database): The database connection object.
        file_group_info (FileGroupInfo): An object containing information about the
                                         scraped file group, including its name,
                                         associated table name, file elements, size,
                                         and date range of source update.

    Returns:
        Optional[AuditDB]: A new `AuditDB` instance if the conditions for creation are met
                           (i.e., the file group is newer than existing records for that
                           table, or no record exists). Returns `None` if the file group
                           is not new enough or if a database connection error occurs.

    Warning:
        This function's logic for deciding whether to create an audit appears
        to have duplicated conditional blocks. The behavior is based on the first
        encountered block within the `if database.engine:` check. This redundancy
        should be reviewed and refactored for clarity and correctness.
    """
    if not database.engine:
        logger.error("Database engine not available in create_audit.")
        return None

    # Query for the latest source update timestamp for this table in the audit log
    latest_updated_at_in_db: Optional[datetime] = None
    try:
        with database.engine.connect() as connection:
            sql_query = text("SELECT MAX(audi_source_updated_at) FROM audit WHERE audi_table_name = :table_name")
            result = connection.execute(sql_query, {'table_name': file_group_info.table_name})
            timestamp_from_db = result.scalar_one_or_none() # scalar_one_or_none is safer
            if timestamp_from_db:
                # Ensure it's a datetime object and make it timezone-aware (consistent with source)
                if isinstance(timestamp_from_db, str): # Handle if string from DB
                    latest_updated_at_in_db = datetime.fromisoformat(timestamp_from_db)
                elif isinstance(timestamp_from_db, datetime):
                    latest_updated_at_in_db = timestamp_from_db

                if latest_updated_at_in_db and latest_updated_at_in_db.tzinfo is None:
                    sao_paulo_timezone = pytz.timezone("America/Sao_Paulo")
                    latest_updated_at_in_db = sao_paulo_timezone.localize(latest_updated_at_in_db)
    except Exception as e:
        logger.error(f"Error querying latest_updated_at for {file_group_info.table_name}: {e}")
        # Depending on policy, might want to return None or allow creation if DB state is uncertain
        pass # Allow to proceed to logic below, None latest_updated_at_in_db will be handled

    # Logic to decide whether to create a new audit entry:
    # Condition 1: No existing audit entry for this table_name.
    if latest_updated_at_in_db is None:
        logger.info(f"No existing audit entry for {file_group_info.table_name}. Creating new audit.")
        return create_new_audit(
            file_group_info.table_name,
            file_group_info.elements,
            file_group_info.size_bytes, 
            file_group_info.date_range[1] # date_range[1] is the most recent update in the group
        )
    # Condition 2: The current file group's update time is more recent than the latest in DB.
    elif file_group_info.date_range[1] > latest_updated_at_in_db:
        logger.info(f"Newer file group found for {file_group_info.table_name} ({file_group_info.date_range[1]} > {latest_updated_at_in_db}). Creating new audit.")
        return create_new_audit(
            file_group_info.table_name,
            file_group_info.elements,
            file_group_info.size_bytes,
            file_group_info.date_range[1]
        )
    # Condition 3: Date difference check (seems to be a specific business rule for "unreliable" batches)
    # This condition is only checked if the file is NOT newer.
    elif file_group_info.date_diff() > 7 : # Assuming date_diff() is a method on FileGroupInfo
        logger.warning(f"Skipping create entry for file group {file_group_info.name}. Date difference > 7 days, considered unreliable or out of sync.")
        return None
    # Condition 4: File is not newer, and date_diff is not > 7.
    else:
        logger.info(f"Skipping create entry for file group {file_group_info.name}. Source data at {file_group_info.date_range[1]} is not newer than DB record at {latest_updated_at_in_db}.")
        return None

    # NOTE: The original code had a duplicated block of the above logic outside the
    # `if database.engine:` check. That block is omitted here as it would be unreachable
    # if database.engine is None, or redundant if it's not.
    # The documented behavior reflects the logic within the `if database.engine:` scope.

def create_audits(database: Database, files_info: List[FileGroupInfo]) -> List[AuditDB]:
    """
    Processes a list of `FileGroupInfo` objects and creates `AuditDB` entries
    for those that meet the criteria for needing a new audit record (e.g., are new or updated).

    Args:
        database (Database): The database connection object.
        files_info (List[FileGroupInfo]): A list of `FileGroupInfo` objects,
                                         typically from scraping the data source.

    Returns:
        List[AuditDB]: A list of newly created (but not yet persisted) `AuditDB`
                       model instances. Entries for which `create_audit` returns `None`
                       (e.g., because they are not new enough) are filtered out.
    """
    new_audits = []
    for file_info_group in files_info:
        audit_entry = create_audit(database, file_info_group)
        if audit_entry:
            new_audits.append(audit_entry)
    return new_audits

def insert_audit(database: Database, new_audit: AuditDB) -> Optional[AuditDB]:
    """
    Inserts a single `AuditDB` model instance into the database.

    Before insertion, it checks if the audit entry's timestamp precedence is met
    (i.e., `audi_created_at <= audi_downloaded_at <= audi_processed_at <= audi_inserted_at`,
    where applicable).

    Args:
        database (Database): The database connection object.
        new_audit (AuditDB): The `AuditDB` model instance to insert.

    Returns:
        Optional[AuditDB]: The inserted `AuditDB` instance if successful and precedence is met,
                           `None` otherwise (e.g., database error, precedence not met).
    """
    if not database.engine or not database.session_maker:
        logger.error("Database engine or session_maker not available in insert_audit.")
        return None

    with database.session_maker() as session:
        try:
            if new_audit.is_precedence_met: # Assuming is_precedence_met handles None timestamps gracefully
                session.add(new_audit)
                session.commit()
                logger.info(f"Successfully inserted audit for table: {new_audit.audi_table_name}, ID: {new_audit.audi_id}")
                return new_audit
            else:
                logger.warning(f"Skipping insert audit for table {new_audit.audi_table_name} due to timestamp precedence not met. ID: {new_audit.audi_id}")
                return None
        except Exception as e:
            logger.error(f"Error inserting audit for table {new_audit.audi_table_name}, ID: {new_audit.audi_id}. Error: {e}")
            session.rollback()
            return None


def insert_audits(database: Database, new_audits: List[AuditDB]) -> None:
    """
    Inserts a list of `AuditDB` model instances into the database.

    Each audit entry is inserted individually using the `insert_audit` function.
    Errors during the insertion of any single audit entry are logged, and the
    function continues to attempt to insert the remaining entries.

    Args:
        database (Database): The database connection object.
        new_audits (List[AuditDB]): A list of `AuditDB` instances to insert.

    Returns:
        None
    """
    for new_audit in new_audits:
        try:
            insert_audit(database, new_audit)
        except Exception as e:
            logger.error(f"Error inserting audit for table {new_audit.audi_table_name}: {e}")

def create_new_audit_metadata(audits: List[AuditDB]) -> AuditMetadata:  
    """
    Creates audit metadata based on the provided database, files information, and destination path.

    Args:
        audits (List[AuditDB]): A list of `AuditDB` instances, typically representing
                                new or updated states of data files/tables.
        # 'to_path' was in the original signature but not used. Removed for clarity.

    Returns:
        AuditMetadata: An `AuditMetadata` object. This object contains:
            - `audit_list`: A list of `AuditDBSchema` Pydantic models, converted
                            from the input `AuditDB` SQLAlchemy models.
            - `tablename_to_zipfile_to_files`: A nested dictionary mapping table names
                                               to their associated original ZIP filenames,
                                               and then to a list of extracted content
                                               filenames (which is empty in this function).
    Note:
        This function seems to prepare metadata *before* files are downloaded or extracted,
        as `zip_file_dict` values are initialized to empty lists. Compare with
        `create_audit_metadata` which attempts to list ZIP contents.
    """
    # Initialize with empty lists for content files, as this function might be called
    # before actual extraction.
    zip_file_dict: Dict[str, List[str]] = {
        zip_filename: []
        for audit in audits
        for zip_filename in audit.audi_filenames if audit.audi_filenames # Ensure audi_filenames is not None
    }

    # Map ZIP filenames to table names (e.g., based on prefixes in ZIP names)
    zipfiles_to_tablenames = get_zip_to_tablename(zip_file_dict)
    # Invert the mapping: table name -> list of ZIP files
    tablename_to_zipfile_dict = invert_dict_list(zipfiles_to_tablenames)

    # Structure for AuditMetadata: table -> original_zip_name -> list_of_extracted_files
    # In this function, list_of_extracted_files is kept empty.
    tablename_to_zipfile_to_files: Dict[str, Dict[str, List[str]]] = {
        tablename: {
            zipfile: zip_file_dict.get(zipfile, []) # Use .get for safety, though keys should exist
            for zipfile in zipfiles
        }
        for tablename, zipfiles in tablename_to_zipfile_dict.items()
    }

    # Convert AuditDB SQLAlchemy models to AuditDBSchema Pydantic models
    pydantic_audit_list = [
        AuditDBSchema.model_validate(audit, from_attributes=True) for audit in audits
    ]

    return AuditMetadata(
        audit_list=pydantic_audit_list,
        tablename_to_zipfile_to_files=tablename_to_zipfile_to_files,
    )

def create_audit_metadata(audits: List[AuditDB], to_path: str) -> AuditMetadata:  
    """
    Creates `AuditMetadata` by inspecting contents of downloaded ZIP files.

    This function builds a comprehensive `AuditMetadata` object. It takes a list of
    `AuditDB` entries (which know about ZIP filenames) and a path where these ZIPs
    are stored. It then lists the actual content filenames from each ZIP archive.
    This information is used to map table names to their original ZIP sources and
    further to the specific data files extracted from those ZIPs.

    Args:
        audits (List[AuditDB]): A list of `AuditDB` SQLAlchemy model instances.
                                Each instance should have `audi_filenames` populated
                                with the names of relevant ZIP files.
        to_path (str): The directory path where the ZIP files listed in `audits`
                       are located.

    Returns:
        AuditMetadata: An `AuditMetadata` object containing:
            - `audit_list`: Pydantic versions (`AuditDBSchema`) of the input `audits`.
            - `tablename_to_zipfile_to_files`: A nested dictionary:
                `{table_name: {zip_filename: [extracted_file1, extracted_file2, ...]}}`.
    """
    zip_file_dict: Dict[str, List[str]] = {}
    for audit in audits:
        if audit.audi_filenames: # Ensure there are filenames
            for zip_filename in audit.audi_filenames:
                full_zip_path = path.join(to_path, zip_filename)
                try:
                    # List contents of the actual ZIP file
                    zip_contents = list_zip_contents(full_zip_path)
                    zip_file_dict[zip_filename] = [content.filename for content in zip_contents]
                except FileNotFoundError:
                    logger.warning(f"ZIP file {full_zip_path} not found during metadata creation. Content list will be empty.")
                    zip_file_dict[zip_filename] = []
                except Exception as e:
                    logger.error(f"Error listing contents of ZIP {full_zip_path}: {e}. Content list will be empty.")
                    zip_file_dict[zip_filename] = []

    zipfiles_to_tablenames = get_zip_to_tablename(zip_file_dict)
    tablename_to_zipfile_dict = invert_dict_list(zipfiles_to_tablenames)

    tablename_to_zipfile_to_files: Dict[str, Dict[str, List[str]]] = {
        tablename: {
            zipfile: zip_file_dict.get(zipfile, []) # Use .get for safety
            for zipfile in zipfiles
        }
        for tablename, zipfiles in tablename_to_zipfile_dict.items()
    }

    pydantic_audit_list = [
        AuditDBSchema.model_validate(audit, from_attributes=True) for audit in audits
    ]

    return AuditMetadata(
        audit_list=pydantic_audit_list,
        tablename_to_zipfile_to_files=tablename_to_zipfile_to_files,
    )

def delete_audits_for_table(database: Database, table_name: str) -> None: # Renamed for clarity
    """
    Deletes all audit entries from the `audit` table for a specific `table_name`.

    Args:
        database (Database): The database connection object.
        table_name (str): The name of the table whose audit entries are to be deleted.

    Returns:
        None
    """
    if database.engine:
        with database.session_maker() as session:
            session.query(AuditDB).filter(AuditDB.audi_table_name == table_name).delete()
            session.commit()
            logger.info(f"Deleted entries for table name {table_name}.")
    else:
        logger.error("Error connecting to the database!")
