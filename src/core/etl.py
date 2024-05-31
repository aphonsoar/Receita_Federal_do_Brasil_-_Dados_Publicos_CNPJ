from typing import List
import re
from urllib import request
from bs4 import BeautifulSoup
from datetime import datetime
from functools import reduce
from shutil import rmtree

from setup.logging import logger
from core.utils.schemas import create_file_groups
from database.models import AuditDB
from core.schemas import FileInfo, AuditMetadata
from database.utils.models import (
    create_audits, 
    create_audit_metadata, 
    insert_audit
)
from core.utils.etl import (
    get_RF_data, 
    load_RF_data_on_database
)
from utils.misc import convert_to_bytes, remove_folder

class CNPJ_ETL:

    def __init__(
        self, database, data_url, layout_url,
        download_folder, extract_folder,
        is_parallel=True, delete_zips=True
    ) -> None:
        self.database = database
        self.data_url = data_url
        self.layout_url = layout_url
        self.download_folder = download_folder
        self.extract_folder = extract_folder
        self.is_parallel = is_parallel
        
    def scrap(self):
        """
        Scrapes the RF (Receita Federal) website to extract file information.

        Returns:
            list: A list of tuples containing the updated date and filename of the files found on the RF website.
        """
        raw_html = request.urlopen(self.data_url)
        raw_html = raw_html.read()

        # Formatar página e converter em string
        page_items = BeautifulSoup(raw_html, 'lxml')

        # Find all table rows
        table_rows = page_items.find_all('tr')
        
        # Extract data from each row
        files_info = []
        for row in table_rows:
            # Find cells containing filename (anchor tag) and date
            filename_cell = row.find('a')
            regex_pattern=r'\d{4}-\d{2}-\d{2}'
            collect_date=lambda text: text and re.search(regex_pattern, text)
            date_cell = row.find('td', text=collect_date)
            
            size_types=['K', 'M', 'G']
            or_map = lambda a, b: a or b
            is_size_type = lambda text: reduce(
                or_map, 
                [text.endswith(size_type) for size_type in size_types]
            )
            size_cell = row.find('td', text=lambda text: text and is_size_type(text))
            
            has_data = filename_cell and date_cell and size_cell
            if has_data:
                filename = filename_cell.text.strip()

                if filename.lower().endswith('.zip'):
                    date_text = date_cell.text.strip()

                    # Try converting date text to datetime object (adjust format if needed)
                    try:
                        updated_at = datetime.strptime(date_text, "%Y-%m-%d %H:%M")
                        updated_at = updated_at.replace(hour=0, minute=0, second=0, microsecond=0)
                    
                    except ValueError:
                        # Handle cases where date format doesn't match
                        logger.error(f"Error parsing date for file: {filename}")

                    size_value_str = size_cell.text.strip()
                    
                    file_info = FileInfo(
                        filename=filename, 
                        updated_at=updated_at,
                        file_size=convert_to_bytes(size_value_str)
                    )
                    files_info.append(file_info)
                
        return files_info
    
    def get_data(self, audits: List[AuditDB]):
        """
        Downloads the files from the RF (Receita Federal) website.

        Returns:
            None
        """
        
        # Get data
        if audits:
            get_RF_data(
                self.data_url, self.layout_url, 
                audits, self.download_folder, self.extract_folder, 
                self.is_parallel
            )

            # Create audit metadata
            audit_metadata = create_audit_metadata(audits, self.download_folder)

            return audit_metadata

        # Delete download folder content
        remove_folder(self.download_folder)


    def retrieve_data(self):
        """
        Retrieves the data from the database.

        Returns:
            None
        """
        # Scrap data
        files_info = self.scrap()

        # Create file groups
        file_groups_info = create_file_groups(files_info)

        # Create audits
        audits = create_audits(self.database, file_groups_info)

        # # Test purpose only
        # from os import getenv
        # if getenv("ENVIRONMENT") == "development": 
        #     audits = list(
        #         filter(
        #             lambda x: x.audi_file_size_bytes < 5000, 
        #             sorted(audits, key=lambda x: x.audi_file_size_bytes)
        #         )
        #     )
        
        if audits:
            # Get data
            return self.get_data(audits)
        else:
            None

    def load_data(self, audit_metadata: AuditMetadata):
        """
        Loads the data into the database.

        Returns:
            None
        """
        # Load database
        audit_metadata = load_RF_data_on_database(
            self.database, self.extract_folder, audit_metadata
        )

        # Insert audit metadata
        for audit in audit_metadata.audit_list:
            insert_audit(self.database, audit)


    def run(self):
        """
        Runs the ETL process.

        Returns:
            None
        """
        audit_metadata = self.retrieve_data()

        if audit_metadata:
            # Load data
            self.load_data(audit_metadata)
        else: 
            logger.warn("No data to load!")