"""
Main script for the ETL (Extract, Transform, Load) process of CNPJ data
from the Brazilian Federal Revenue (Receita Federal).

This script orchestrates the download, extraction, processing, and loading of
CNPJ data into a DuckDB database. It utilizes the CNPJ_ETL class for the core
ETL logic and configuration parameters defined in `src.config`.
"""
# Project: ETL - CNPJs da Receita Federal do Brasil
# Objective: Download, transform, and load Brazilian Federal Revenue CNPJ data

import time
from uuid import uuid4
from pathlib import Path
import logging
from itertools import islice
from datetime import datetime
from charset_normalizer import from_path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import duckdb
from duckdb import DuckDBPyConnection, ConversionException
import polars as pl
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

from setup.base import get_sink_folder, init_database
from core.etl import CNPJ_ETL
from core.constants import TABLES_INFO_DICT
from src.config import YEAR, MONTH, DB_FILE_TEMPLATE, PARQUET_DIR, FILES_URL_TEMPLATE, LAYOUT_URL

# ─── Configuration ───
logging.basicConfig(
    level="INFO", format="%(message)s", datefmt="[%X]", handlers=[RichHandler()]
)
log = logging.getLogger("cnpj_etl")
start_time = time.time()

def main():
    """
    Main function to execute the CNPJ ETL process.

    It initializes configurations, sets up folders and database connections,
    instantiates the CNPJ_ETL class, and triggers the ETL run.
    Finally, it logs the total execution time.
    """
    # Format database file name and files URL using year and month from config
    DB_FILE = DB_FILE_TEMPLATE.format(YEAR=YEAR, MONTH=MONTH)
    FILES_URL = FILES_URL_TEMPLATE.format(YEAR=YEAR, MONTH=MONTH)

    log.info(f"ETL start: {datetime.now():%Y-%m-%d %H:%M:%S}")

    # Phase 0: Setup - Prepare download/extract folders and initialize database
    download_folder, extract_folder = get_sink_folder()  # Get or create designated folders
    database = init_database(DB_FILE)  # Initialize DuckDB database instance
    
    # Instantiate the main ETL orchestrator
    scraper = CNPJ_ETL(
        database=database,
        data_url=FILES_URL,  # URL for CNPJ data files
        layout_url=LAYOUT_URL,  # URL for the layout metadata PDF
        download_folder=download_folder, # Folder to store downloaded ZIP files
        extract_folder=extract_folder, # Folder to store extracted CSV files
        is_parallel=True,  # Enable parallel processing for download/extraction
        delete_zips=True  # Delete ZIP files after successful extraction
    )
    # Execute the full ETL process
    audits = scraper.run()

    elapsed = time.time() - start_time
    log.info(f"ETL complete in {elapsed:.2f}s — DB at {DB_FILE}")


if __name__ == "__main__":
    main()
