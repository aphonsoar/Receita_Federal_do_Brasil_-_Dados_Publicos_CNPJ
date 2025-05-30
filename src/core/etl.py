from typing import Dict, List, Optional, Any
import re
from urllib import request
from bs4 import BeautifulSoup
from datetime import datetime
from functools import reduce
from os import getenv
import pytz

from setup.logging import logger
from core.utils.schemas import create_file_groups
from database.models import AuditDB
from core.schemas import FileInfo, AuditMetadata
from database.utils.models import (
    create_audits, 
    create_audit_metadata, 
    insert_audit
)
from database.dml import generate_tables_indices
from core.utils.etl import (
    get_RF_data, 
    load_RF_data_on_database
)
from utils.misc import convert_to_bytes, remove_folder

class CNPJ_ETL:
    """
    Orchestrates the End-to-End ETL process for Brazilian Federal Revenue CNPJ data.

    This class handles scraping file information from the government website,
    downloading data files, extracting them, loading data into a database,
    creating necessary database indices, and recording audit trails.
    """

    def __init__(
        self, database, data_url: str, layout_url: str,
        download_folder: str, extract_folder: str,
        is_parallel: bool = True, delete_zips: bool = True
    ) -> None:
        """
        Initializes the CNPJ_ETL orchestrator.

        Args:
            database: An initialized database connection object (e.g., DuckDBPyConnection).
            data_url (str): Base URL for fetching the list of CNPJ data files.
            layout_url (str): URL for the PDF file describing the data layout.
            download_folder (str): Path to the folder where downloaded ZIP files will be stored.
            extract_folder (str): Path to the folder where extracted CSV files will be stored.
            is_parallel (bool): If True, enables parallel download and extraction of files.
                                Defaults to True.
            delete_zips (bool): If True, deletes ZIP files after successful extraction.
                                Defaults to True.
        """
        self.database = database
        self.data_url = data_url
        self.layout_url = layout_url
        self.download_folder = download_folder
        self.extract_folder = extract_folder
        self.is_parallel = is_parallel
        self.delete_zips = delete_zips
        
    def scrap_data(self) -> List[FileInfo]:
        """
        Scrapes the Receita Federal website (defined in self.data_url) to extract
        information about available data files (name, update date, size).

        It handles potential network errors during fetching and parsing errors with
        BeautifulSoup.
        
        Returns:
            List[FileInfo]: A list of FileInfo objects, each containing metadata for a
                            scraped file (filename, updated_at, file_size). Returns an
                            empty list if scraping fails at any stage.
        """
        try:
            # Attempt to fetch the HTML content from the data URL
            raw_html = request.urlopen(self.data_url).read()
        except Exception as e:
            logger.error(f"Failed to fetch URL: {self.data_url}. Error: {e}")
            return []

        try:
            page_items = BeautifulSoup(raw_html, 'lxml')
        except Exception as e:
            logger.error(f"Failed to parse HTML from URL: {self.data_url}. Error: {e}")
            return []

        table_rows = page_items.find_all('tr')

        files_info = []
        for row in table_rows:
            # Try to find cells by their typical content/structure
            filename_cell = row.find('a')  # File links are usually in <a> tags
            # Date cells often contain text matching YYYY-MM-DD format
            date_cell = row.find('td', text=lambda text: text and re.search(r'\d{4}-\d{2}-\d{2}', text))
            # Size cells typically end with a size unit (K, M, G, etc.)
            size_cell = row.find('td', text=lambda text: text and any(text.endswith(size_type) for size_type in ['K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y']))

            if filename_cell and date_cell and size_cell:
                filename = filename_cell.text.strip()
                if filename.endswith('.zip'):
                    updated_at = self._parse_date(date_cell.text.strip(), filename)
                    if not updated_at:
                        continue

                    file_info = FileInfo(
                        filename=filename,
                        updated_at=updated_at,
                        file_size=convert_to_bytes(size_cell.text.strip())
                    )
                    files_info.append(file_info)

        return files_info

    @staticmethod
    def _parse_date(date_str: str, filename: str) -> Optional[datetime]:
        """
        Parses date from string, logs errors if date parsing fails.
        
        Args:
            date_str (str): The date string to parse.
            filename (str): The file being processed (for error logging).
        
        Returns:
            Optional[datetime]: Parsed datetime object or None if parsing fails.
        """
        try:
            updated_at = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
            return pytz.timezone("America/Sao_Paulo")\
                .localize(updated_at)\
                .replace(hour=0, minute=0, second=0, microsecond=0)
        except ValueError as e:
            logger.error(f"Error parsing date string '{date_str}' for file: {filename}. Error: {e}")
            return None    
    
    def get_data(self, audits: List[AuditDB]) -> None:
        """
        Downloads the files from the Receita Federal website.
        
        Args:
            audits (List[AuditDB]): A list of AuditDB objects representing the files
                                    to be downloaded. Each object contains metadata
                                    like filenames.

        Note:
            This method calls `core.utils.etl.get_RF_data`, which has its own
            internal retry mechanisms for downloads. Errors during individual file
            downloads within `get_RF_data` are logged by that function. The try-except
            block here is intended for more general or unexpected failures during the
            overall data retrieval orchestration.
        """
        if not audits:
            logger.info("No audit records provided to download data.")
            return

        try:
            get_RF_data(
                self.data_url, self.layout_url,
                audits, self.download_folder, self.extract_folder,
                self.is_parallel
            )
        except Exception as e:
            logger.error(f"An unexpected error occurred during data retrieval (get_RF_data). Error: {e}")
            # Depending on desired behavior, could re-raise or handle specific follow-up

    def audit_scrapped_files(self, files_info: List[FileInfo]) -> List[AuditDB]:
        """
        Audits the scrapped files by creating database audit records.

        "Auditing" in this context means creating `AuditDB` records for each relevant
        file found during scraping. These records help track the status and metadata
        of each file through the ETL pipeline.
        
        Args:
            files_info (List[FileInfo]): A list of `FileInfo` objects obtained from
                                         the `scrap_data` method.
        
        Returns:
            List[AuditDB]: A list of `AuditDB` objects that have been created (but not
                           necessarily persisted to the database yet by this specific method).
        """
        # Filter for relevant files: ZIP archives and the layout PDF.
        filtered_files = [
            info for info in files_info if info.filename.endswith('.zip') or
            (info.filename.endswith('.pdf') and re.match(r'layout', info.filename, re.IGNORECASE))
        ]

        # Group related files (e.g., multiple parts of a large table)
        file_groups_info = create_file_groups(filtered_files)
        
        # Create audit entries in the database for these file groups
        return create_audits(self.database, file_groups_info)

    def fetch_data(self) -> List[AuditDB]:
        """
        Orchestrates the initial data fetching phase.

        This involves scraping the website for file information and then auditing
        these files (i.e., creating initial audit records in the database).
        No actual file downloads occur in this method.
        
        Returns:
            List[AuditDB]: A list of `AuditDB` objects representing the audited files.
                           Returns an empty list if scraping yields no files.
        """
        files_info = self.scrap_data()

        return self.audit_scrapped_files(files_info)

    def retrieve_data(self) -> List[AuditDB]:
        """
        Retrieves file information, optionally filters it for development, and then
        triggers the actual download of the data files.

        This method orchestrates:
        1. Fetching file listings and creating initial audit records (`fetch_data`).
        2. If in a "development" environment, filtering these audit records to
           process only smaller files for quicker testing.
        3. Triggering the download of the selected files (`get_data`).
        
        Returns:
            List[AuditDB]: A list of `AuditDB` objects for which data download was
                           attempted. This list might be filtered if in development mode.
                           Returns an empty list if no files are identified for download.
        """
        audits = self.fetch_data()

        # In development environment, filter for smaller files for faster testing
        if getenv("ENVIRONMENT") == "development":
            logger.info("Development environment detected: filtering for smaller audit files.")
            audits = [
                audit for audit in sorted(audits, key=lambda x: x.audi_file_size_bytes)
                if audit.audi_file_size_bytes < 50000  # Threshold for small files
            ]

        if audits:
            self.get_data(audits)
        else:
            logger.warning("No audits to retrieve.")
        return audits

    def load_data(self, audit_metadata: AuditMetadata) -> None:
        """
        Loads data from extracted files into the database.
        
        Args:
            audit_metadata (AuditMetadata): An `AuditMetadata` object containing
                                            information about the files to be loaded
                                            and their corresponding table mappings.

        Raises:
            This method calls `load_RF_data_on_database` which might raise various
            exceptions related to database operations (e.g., connection errors,
            SQL execution errors, data constraint violations) if not handled internally
            by `load_RF_data_on_database`. Such exceptions are caught and logged here.
        """
        if not audit_metadata or not audit_metadata.audit_list:
            logger.warning("No audit metadata provided to load_data.")
            return
        try:
            load_RF_data_on_database(self.database, self.extract_folder, audit_metadata)
        except Exception as e:
            logger.error(f"Failed to load data into database. Error: {e}")
            # Consider if this error should halt further processing or be re-raised

    def insert_audits(self, audit_metadata: AuditMetadata) -> None:
        """
        Inserts or updates audit trail records in the database based on the provided
        audit metadata.

        Args:
            audit_metadata (AuditMetadata): An `AuditMetadata` object containing a list
                                            of audit items to be persisted.

        Note:
            Potential database errors (e.g., connection issues, constraint violations)
            during the insertion of individual audit records are caught and logged.
            The method attempts to insert all audit items despite individual failures.
        """
        if not audit_metadata or not audit_metadata.audit_list:
            logger.warning("No audit metadata provided to insert_audits.")
            return

        for audit_item in audit_metadata.audit_list:
            try:
                insert_audit(self.database, audit_item.to_audit_db())
            except Exception as e:
                logger.error(f"Error inserting audit {audit_item.audi_id} ({audit_item.audi_table_name}) into database. Error: {e}")

    def load_without_download(self) -> None:
        """
        Executes a data loading process assuming files are already downloaded and extracted.

        This method is useful for scenarios where:
        - Data files were manually downloaded and placed in the extraction folder.
        - A previous ETL run failed after download/extraction, and only loading is needed.

        It fetches file listings (to understand what *should* be there), prepares
        audit metadata (marking items as if they were "downloaded" now), and then
        proceeds with the standard data loading and auditing process.
        """
        audits = self.fetch_data() # Scrape and create initial audit entries
        if audits:
            # For load_without_download, timestamps are set to now as files aren't truly downloaded.
            audit_metadata = self._prepare_audit_metadata(audits, update_timestamps=True)
            self._process_and_load(audit_metadata)
        else:
            logger.warning("No data to load!")

    def _prepare_audit_metadata(self, audits: List[AuditDB], update_timestamps: bool = False) -> AuditMetadata:
        """
        Creates audit metadata from a list of audit database objects.
        Optionally updates download and processed timestamps.

        Args:
            audits (List[AuditDB]): List of audit database objects.
            update_timestamps (bool): If True, sets downloaded_at and processed_at to current time.
                                      Useful for scenarios where files are not actually downloaded
                                      (e.g., load_without_download).

        Returns:
            AuditMetadata: The created audit metadata.
        """
        audit_metadata = create_audit_metadata(audits, self.download_folder)
        if update_timestamps:
            logger.info("Updating timestamps for audit metadata (e.g., for load_without_download scenario).")
            current_time = datetime.now()
            for audit in audit_metadata.audit_list:
                audit.audi_downloaded_at = current_time
                audit.audi_processed_at = current_time
        return audit_metadata

    def _process_and_load(self, audit_metadata: AuditMetadata) -> None:
        """
        Internal method to orchestrate data loading, index creation, and audit insertion.

        This method is called by both the full `run` and `load_without_download` flows
        after the necessary `AuditMetadata` has been prepared.

        Args:
            audit_metadata (AuditMetadata): The metadata guiding the loading process.
        """
        try:
            self.load_data(audit_metadata)
            self.create_indices()
            self.insert_audits(audit_metadata)
        except Exception as e:
            logger.critical(f"A critical error occurred in _process_and_load, preventing further processing for this batch. Error: {e}")
            # Depending on overall application flow, might need to re-raise or handle state

    def only_create_indices(self) -> None:
        """
        Generates and inserts table indices.
        """
        self.create_indices()

    def create_indices(self) -> None:
        """
        Generates and applies database indices for predefined tables and columns.

        The specific tables and columns for which indices are created are hardcoded
        within this method in the `tables_with_indices` dictionary. This method
        is typically called after data loading to improve query performance.
        """
        logger.info("Starting index creation process.")
        # Defines which tables and columns should have indices.
        tables_with_indices = {
            "estabelecimento": {
                "cnpj_basico", "cnpj_ordem", "cnpj_dv", 
                "cnae_fiscal_principal", "cnae_fiscal_secundaria", 
                "cep", "municipio", "uf"
            },
            "empresa": {"cnpj_basico"},
            "simples": {"cnpj_basico"}, 
            "socios": {"cnpj_basico"},
            "cnae": {"codigo"},
            "moti": {"codigo"},
            "munic": {"codigo"}
        }

        renew_table_indices = {
            table_name: columns 
            for table_name, columns in tables_with_indices.items() 
        }

        if renew_table_indices:
            try:
                generate_tables_indices(self.database.engine, renew_table_indices)
                logger.info("Successfully generated/updated table indices.")
            except Exception as e:
                logger.error(f"Failed to generate/update table indices. Error: {e}")
        else:
            logger.info("No tables identified for index creation/update.")

    def run(self) -> None:
        """
        Executes the full ETL (Extract, Transform, Load) process.

        This is the main public method to trigger the entire pipeline, including:
        1. Retrieving data: Scraping file lists, downloading ZIPs, extracting CSVs.
           (`retrieve_data` method)
        2. Preparing metadata: Creating comprehensive audit metadata for the retrieved files.
           (`_prepare_audit_metadata` method)
        3. Processing and loading: Loading data from CSVs into database tables,
           creating database indices, and inserting final audit records.
           (`_process_and_load` method)
        4. Cleanup: Optionally deleting downloaded ZIP files if `self.delete_zips` is True.

        This method does not return any value. Its progress and outcome are primarily
        communicated through logging.
        """
        # Step 1: Retrieve data (scrape, download, extract)
        audits = self.retrieve_data()

        if audits:
            # Step 2: Prepare metadata for the successfully retrieved files
            # For a full run, timestamps are set during actual download/extraction, so don't update here.
            audit_metadata = self._prepare_audit_metadata(audits, update_timestamps=False)

            # Step 3: Load data into DB, create indices, and insert audit trail
            self._process_and_load(audit_metadata)

            # Step 4: Optional cleanup of downloaded ZIP files
            if self.delete_zips:
                logger.info(f"Attempting to remove download folder: {self.download_folder}")
                try:
                    remove_folder(self.download_folder)
                    logger.info(f"Successfully removed folder: {self.download_folder}")
                except Exception as e:
                    logger.error(f"Failed to remove folder {self.download_folder}. Error: {e}")
        else:
            logger.warning("No data to load at the end of run!")
            # For a full run, timestamps are set during actual download/extraction, so don't update here.
            audit_metadata = self._prepare_audit_metadata(audits, update_timestamps=False)
            self._process_and_load(audit_metadata)

            if self.delete_zips:
                logger.info(f"Attempting to remove download folder: {self.download_folder}")
                try:
                    remove_folder(self.download_folder)
                    logger.info(f"Successfully removed folder: {self.download_folder}")
                except Exception as e:
                    logger.error(f"Failed to remove folder {self.download_folder}. Error: {e}")
        else:
            logger.warning("No data to load at the end of run!")
