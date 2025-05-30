"""
ETL (Extract, Transform, Load) utility functions for the CNPJ project.

This module provides functions for downloading files from the Receita Federal website,
extracting data from ZIP archives, processing files in parallel or serially,
and loading data into the database. It leverages external libraries like requests,
tqdm, and tenacity for robust and observable operations.
"""
from wget import download
import requests
from tqdm import tqdm
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from os import path
from functools import reduce
from tenacity import retry, stop_after_attempt, wait_exponential

from core.schemas import AuditMetadata
from database.models import AuditDB
from database.dml import populate_table
from utils.misc import get_file_size, get_max_workers
from utils.zip import extract_zip_file
from core.constants import TABLES_INFO_DICT
from setup.logging import logger 

# Tabelas
tablename_list = list(TABLES_INFO_DICT.keys())
trimmed_tablename_list = [name[:5] for name in TABLES_INFO_DICT.keys()]
tablename_tuples = list(zip(tablename_list, trimmed_tablename_list))

import requests
from tqdm import tqdm
from os import path
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def download_zipfile(
    audit: AuditDB, url: str, zip_filename: str, download_path: str, has_progress_bar: bool
) -> AuditDB:
    """
    Downloads a single ZIP file from the given URL to the specified output path.

    This function uses `requests` for downloading and `tqdm` for a progress bar if enabled.
    It includes retry logic using `@tenacity.retry` for handling transient network issues.
    Updates the provided AuditDB object with download timestamp and file size upon success.

    Args:
        audit (AuditDB): The AuditDB object associated with this file.
                         It will be updated with `audi_downloaded_at` and
                         `audi_file_size_bytes` on successful download.
        url (str): The base URL from which to download the file.
        zip_filename (str): The name of the ZIP file to download.
        download_path (str): The local directory path where the file will be saved.
        has_progress_bar (bool): If True, displays a tqdm progress bar during download.

    Returns:
        AuditDB: The updated AuditDB object.

    Raises:
        requests.exceptions.HTTPError: If the HTTP request returns an unsuccessful status code.
        requests.exceptions.RequestException: For other network-related issues.
        OSError: If there's an issue writing the file to disk.
        Any other exception encountered during the process will also be raised after logging.
    """
    full_path = path.join(download_path, zip_filename)
    file_url = f"{url}/{zip_filename}"
    local_filename = path.join(download_path, zip_filename)

    try:
        logger.info(f"Starting download for file: {zip_filename}")
        
        with requests.get(file_url, stream=True) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('Content-Length', 0))  # Get total file size
            
            # Initialize tqdm progress bar if enabled
            progress = tqdm(
                total=total_size, unit='B', unit_scale=True, desc=zip_filename
            ) if has_progress_bar else None
            
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=131072):
                    if chunk:
                        f.write(chunk)
                        if has_progress_bar:
                            progress.update(len(chunk))  # Update the progress bar
            
            # Close the progress bar if enabled
            if has_progress_bar and progress is not None:
                progress.close()

        logger.info(f"Successfully downloaded file: {zip_filename}")
        audit.audi_downloaded_at = datetime.now()
        audit.audi_file_size_bytes = get_file_size(local_filename)

    except Exception as e:
        logger.error(f"Error downloading {zip_filename}: {e}")
        if has_progress_bar and progress is not None:
            progress.close()  # Close the progress bar in case of failure
        raise

    return audit


def extract_zipfile(audit: AuditDB, zip_filename: str, download_path: str, extracted_path: str) -> AuditDB:
    """
    Extracts a single ZIP file to the specified directory and updates audit timestamp.

    This function is a wrapper around `utils.zip.extract_zip_file`. It logs the
    extraction process and updates the `audi_processed_at` timestamp on the
    provided AuditDB object.

    Args:
        audit (AuditDB): The AuditDB object associated with this file.
                         It will be updated with `audi_processed_at`.
        zip_filename (str): The name of the ZIP file to extract (expected to be in `download_path`).
        download_path (str): The directory where the ZIP file is located.
        extracted_path (str): The directory where the contents will be extracted.

    Returns:
        AuditDB: The updated AuditDB object.
    """
    full_path = path.join(download_path, zip_filename)

    try:
        logger.info(f"Extracting file {zip_filename}...")
        extract_zip_file(full_path, extracted_path) # Actual extraction utility
    except Exception as e:
        logger.error(f"Error extracting file {zip_filename}: {e}")
        # We still update the timestamp and return audit, as processing was attempted.
        # The error is logged, and downstream processes should check file existence.
    finally:
        logger.info(f"Finished file extraction attempt for {zip_filename}.")
        audit.audi_processed_at = datetime.now()
        return audit

def download_and_extract_file(
    audit: AuditDB, url: str, zip_filename: str,
    download_path: str, extracted_path: str, has_progress_bar: bool
) -> Optional[AuditDB]:
    """
    Downloads and then extracts a single specified ZIP file.

    This function orchestrates the download of a `zip_filename` and its subsequent
    extraction. It updates the `audit` object with timestamps from both operations.

    Args:
        audit (AuditDB): The AuditDB object related to this file. It's passed through
                         and updated by the download and extraction functions.
        url (str): Base URL for downloading.
        zip_filename (str): The specific ZIP file to process.
        download_path (str): Directory to download ZIP files to.
        extracted_path (str): Directory to extract ZIP contents to.
        has_progress_bar (bool): Whether to show a progress bar for downloads.

    Returns:
        Optional[AuditDB]: The updated AuditDB object if both download and extraction
                           were successful (or attempted, in case of extraction error),
                           or None if the download failed critically before extraction.
    """
    try:
        # Download the file, updating audit with download timestamp and size
        audit_after_download = download_zipfile(audit, url, zip_filename, download_path, has_progress_bar)

        # If download was successful, proceed to extraction
        # extract_zipfile updates audit with processed timestamp
        audit_after_extraction = extract_zipfile(audit_after_download, zip_filename, download_path, extracted_path)
        return audit_after_extraction
    except Exception as e: # This broad except catches failures primarily from download_zipfile
        logger.error(f"Error processing file {zip_filename} during download stage: {e}. Extraction skipped.")
        return None

def download_and_extract_files(
    audit: AuditDB, url: str, download_path: str, extracted_path: str, has_progress_bar: bool
) -> AuditDB:
    """
    Downloads and extracts all files listed in `audit.audi_filenames`.

    Iterates through each `zip_filename` in the `audit.audi_filenames` list,
    calling `download_and_extract_file` for each. The `audit` object is
    accumulatively updated with timestamps from these operations.

    Args:
        audit (AuditDB): The AuditDB object, which contains a list of filenames
                         (`audi_filenames`) to be processed. This object is modified.
        url (str): Base URL for downloads.
        download_path (str): Directory for downloaded ZIPs.
        extracted_path (str): Directory for extracted files.
        has_progress_bar (bool): Whether to show progress bars for downloads.

    Returns:
        AuditDB: The AuditDB object, updated by the processing of each file.
                 If multiple files are in `audi_filenames`, timestamps will reflect the
                 last successfully processed file in the list.
    """
    # The 'audit' object is passed along and modified by each call.
    # Timestamps on the audit object will reflect the status of the *last* file processed.
    # This is generally acceptable as the audit object represents a group of files for a table,
    # and the overall processed_at timestamp marks the completion of the group.
    for zip_filename in audit.audi_filenames:
        result_audit = download_and_extract_file(
            audit, url, zip_filename, download_path, extracted_path, has_progress_bar
        )
        if result_audit is None:
            logger.warning(f"Processing via download_and_extract_file failed for {zip_filename} within {audit.audi_table_name}. Audit object may not have all timestamps updated for this file.")
            # Continue processing other files in the audit group if any.
            # The 'audit' object itself will retain its state from before this failed call for the next iteration.
    return audit # Return the audit object, which has been modified in place.

def process_files_parallel(
    url: str, audits: List[AuditDB], output_path: str, extracted_path: str, max_workers: int
) -> List[Optional[AuditDB]]:
    """
    Downloads and extracts multiple audit groups' files in parallel using a ThreadPoolExecutor.

    Args:
        url (str): The base URL for downloading files.
        audits (List[AuditDB]): A list of AuditDB objects, each representing a file or
                                group of files to be processed.
        output_path (str): The local directory to save downloaded ZIP files.
        extracted_path (str): The local directory to extract file contents to.
        max_workers (int): The maximum number of worker threads for parallel processing.

    Returns:
        List[Optional[AuditDB]]: A list containing the updated AuditDB objects for each
                                 processed audit group. If an error occurs for an audit
                                 group, the corresponding list item may be the original
                                 audit object or an incompletely updated one.
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Each future will eventually return an updated AuditDB object
        futures = [
            executor.submit(
                download_and_extract_files, 
                audit, url, output_path, extracted_path, False # No individual progress bars in parallel mode
            ) for audit in audits
        ]
        results = []
        # tqdm wrapper for futures to show overall progress
        for future in tqdm(as_completed(futures), total=len(audits), desc="Processing Files (Parallel)", unit="audit_group"):
            try:
                results.append(future.result()) # result() is an updated AuditDB
            except Exception as e:
                # This exception would typically be from the future itself if not caught within the target function
                logger.error(f"Error retrieving result from parallel execution future: {e}")
                # It's hard to know which audit this belonged to without more tracking.
                # We append None or some indicator of failure.
                results.append(None)

        return results

def process_files_serial(url: str, audits: List[AuditDB], output_path: str, extracted_path: str) -> List[AuditDB]:
    """
    Downloads and extracts multiple audit groups' files serially.

    This function processes each audit group one by one, displaying a progress bar
    for individual file downloads within that group. Updates to `AuditDB` objects
    in the `audits` list are done in-place.

    Args:
        url (str): The base URL for downloading files.
        audits (List[AuditDB]): A list of AuditDB objects to process. These objects
                                will be updated in place.
        output_path (str): The local directory to save downloaded ZIP files.
        extracted_path (str): The local directory to extract file contents to.

    Returns:
        List[AuditDB]: The list of AuditDB objects, updated in place.
    """
    error_count = 0
    error_basefiles = []
    total_count = len(audits)
    # Sort by size to process smaller files first, potentially giving quicker feedback.
    # The original 'audits' list is not modified by sorted(), a new list is created.
    # However, download_and_extract_files will modify the AuditDB objects themselves.
    audits_to_process = sorted(audits, key=lambda x: x.audi_file_size_bytes)

    for index, audit_item in enumerate(audits_to_process):
        try:
            # download_and_extract_files updates the audit_item object in place
            download_and_extract_files(audit_item, url, output_path, extracted_path, True)
        except OSError as e: # Catching specific OS errors if needed
            logger.error(f"OS Error while processing {audit_item.audi_table_name}: {e}")
            error_count += 1
            error_basefiles.append(audit_item.audi_table_name)
        # General catch for other unexpected errors
        except Exception as e:
            logger.error(f"Generic Error while processing {audit_item.audi_table_name}: {e}")
            error_count += 1
            error_basefiles.append(audit_item.audi_table_name)
        finally:
            logger.info(f"Progress: ({index + 1}/{total_count}) audit groups processed. Errors so far: {error_count} (tables: {error_basefiles})")
    # The original 'audits' list elements are modified because audit_item objects are modified in place.
    return audits

def download_and_extract_RF_data(
    zips_url: str, layout_url: str, audits: List[AuditDB],
    output_path: str, extracted_path: str, is_parallel: bool = True
) -> List[Optional[AuditDB]]:
    """
    Orchestrates the download and extraction of Receita Federal data files.

    It can operate in parallel or serial mode. Also downloads the layout PDF.

    Args:
        zips_url (str): Base URL for the ZIP files.
        layout_url (str): URL for the layout PDF file.
        audits (List[AuditDB]): List of AuditDB objects to be processed. These objects
                                may be updated in-place (serial) or new/updated objects
                                returned (parallel).
        output_path (str): Directory to save downloaded ZIPs.
        extracted_path (str): Directory to extract files to.
        is_parallel (bool): If True, process files in parallel. Defaults to True.

    Returns:
        List[Optional[AuditDB]]: A list of AuditDB objects. In parallel mode, these are the
                                 results from futures. In serial mode, this is the
                                 original list whose elements have been updated in place.
                                 Elements can be None if processing for an audit group failed
                                 in parallel mode, or represent the last state in serial mode.
    """
    max_workers = get_max_workers()
    # Ensure parallel is only true if workers are available and requested
    is_parallel_effective = max_workers > 1 and is_parallel

    processed_audits_list: List[Optional[AuditDB]]
    if is_parallel_effective:
        logger.info(f"Processing files in parallel with {max_workers} workers.")
        processed_audits_list = process_files_parallel(zips_url, audits, output_path, extracted_path, max_workers)
    else:
        logger.info("Processing files serially.")
        # process_files_serial modifies the 'audits' list in-place and returns it.
        processed_audits_list = process_files_serial(zips_url, audits, output_path, extracted_path)

    try:
        logger.info(f"Downloading layout file from {layout_url} to {output_path}")
        download(layout_url, out=output_path)
        logger.info("Layout PDF downloaded successfully.")
    except Exception as e:
        logger.error(f"Failed to download layout PDF from {layout_url}. Error: {e}")

    return processed_audits_list

def get_RF_data(
    data_url: str, layout_url: str, audits: List[AuditDB],
    from_folder: str, to_folder: str, is_parallel: bool = True
) -> List[Optional[AuditDB]]:
    """
    Main function to retrieve and extract data files from the Receita Federal.

    This is a high-level wrapper around `download_and_extract_RF_data`, providing
    a simpler interface for the core ETL logic to call.

    Args:
        data_url (str): Base URL for data files.
        layout_url (str): URL for the layout PDF.
        audits (List[AuditDB]): List of audit objects to process.
        from_folder (str): Path to the download folder for ZIP files.
        to_folder (str): Path to the extraction folder for CSV files.
        is_parallel (bool): Whether to process in parallel. Defaults to True.

    Returns:
        List[Optional[AuditDB]]: List of audit objects, potentially updated.
                                 See `download_and_extract_RF_data` for details on
                                 how objects are updated.
    """
    return download_and_extract_RF_data(data_url, layout_url, audits, from_folder, to_folder, is_parallel)

def load_RF_data_on_database(
    database, source_folder: str, audit_metadata: AuditMetadata
) -> AuditMetadata:
    """
    Populates database tables with data from extracted CSV files.

    Iterates through table information in `audit_metadata`, identifies the
    relevant source CSV files, and uses `database.dml.populate_table` to load
    the data. Updates `audi_inserted_at` timestamp on each audit item upon successful
    completion of its corresponding table load.

    Args:
        database: DuckDBPyConnection (or similar) database connection object.
        source_folder (str): The directory where extracted CSV files are located.
        audit_metadata (AuditMetadata): Metadata object containing table mappings
                                        and lists of files for each table.

    Returns:
        AuditMetadata: The input `audit_metadata` object, with `audi_inserted_at`
                       timestamps updated for successfully loaded audit items.
    """
    table_to_filenames = audit_metadata.tablename_to_zipfile_to_files

    # Load data for each table defined in the audit metadata
    # Iterate through audit_list as the source of truth for what needs to be loaded
    for audit_item in audit_metadata.audit_list:
        table_name = audit_item.audi_table_name

        # Check if this table from audit_list has corresponding file info in table_to_filenames
        if table_name in table_to_filenames:
            zipfile_content_dict = table_to_filenames[table_name]

            # Aggregate all actual data filenames for this table
            # e.g., K3241.K03200Y0.D40129.ESTABELE.csv
            table_data_files = []
            for original_zip_name, extracted_files_for_zip in zipfile_content_dict.items():
                table_data_files.extend(extracted_files_for_zip)

            if table_data_files:
                logger.info(f"Loading data for table '{table_name}' from files: {table_data_files}")
                try:
                    # Populate this table using the identified data files
                    populate_table(database, table_name, source_folder, table_data_files)
                    audit_item.audi_inserted_at = datetime.now() # Mark as inserted
                    logger.info(f"Successfully loaded data for table '{table_name}'.")
                except Exception as e:
                    logger.error(f"Failed to load data for table '{table_name}'. Error: {e}")
            else:
                logger.warning(f"No data files found for table '{table_name}' in audit metadata's file mapping for this table.")
        else:
            logger.warning(f"Table name '{table_name}' from audit list not found in file mapping (table_to_filenames). Skipping load for this audit item.")

    # Log overall completion based on the initial set of ZIPs intended for processing
    all_zip_filenames_in_metadata = []
    if table_to_filenames: # Ensure it's not empty
        for zip_map in table_to_filenames.values():
            all_zip_filenames_in_metadata.extend(list(zip_map.keys()))

    if all_zip_filenames_in_metadata:
        logger.info(f"Database loading process completed for ZIP source groups: {list(set(all_zip_filenames_in_metadata))}.")
    else:
        logger.info("Database loading process completed, but no source ZIP groups were mapped in the audit metadata for loading.")

    return audit_metadata # Return the metadata, now with updated audit_items

def get_zip_to__tablename(zip_file_dict: Dict[str, List[str]]) -> Dict[str, List[str]]: # Corrected name
    """
    Maps ZIP filenames to a list of possible table names based on prefixes.

    This function attempts to identify which database table(s) a given ZIP file
    (or its contents) might belong to by checking if known table name prefixes
    (e.g., "EMPRECSV" for "empresas" - this example might be off, uses 5 char prefixes)
    are part of the ZIP file's name. It uses `tablename_tuples` from constants,
    which stores (full_table_name, 5_char_prefix).

    Args:
        zip_file_dict (Dict[str, List[str]]): A dictionary where keys are ZIP filenames
                                             and values are lists of files extracted
                                             from that ZIP. (The values are not directly
                                             used in this function's logic, only the keys).

    Returns:
        Dict[str, List[str]]: A dictionary where keys are the input ZIP filenames,
                              and values are lists of matching full table names.
                              An empty list for a key means no mapping was found for that ZIP.
    """
    mapping = {}
    for zipped_file_key in zip_file_dict.keys():
        found_tables = []
        # tablename_tuples contains (full_name, 5_char_prefix)
        for full_table_name, five_char_prefix in tablename_tuples:
            # Check if the 5-character prefix is in the zip filename (case-insensitive)
            if five_char_prefix.lower() in zipped_file_key.lower():
                found_tables.append(full_table_name)
        mapping[zipped_file_key] = found_tables
    return mapping
