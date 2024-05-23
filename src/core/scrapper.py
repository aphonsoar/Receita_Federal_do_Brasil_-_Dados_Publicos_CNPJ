import re
from urllib import request
from bs4 import BeautifulSoup
from datetime import datetime

from utils.logging import logger
from core.constants import DADOS_RF_URL 

def scrap_RF():
    """
    Scrapes the RF (Receita Federal) website to extract file names and their corresponding dates.

    Returns:
        A list of tuples, where each tuple contains the date (in the format 'YYYY-MM-DD') and the filename.
    """
    raw_html = request.urlopen(DADOS_RF_URL)
    raw_html = raw_html.read()

    # Formatar página e converter em string
    page_items = BeautifulSoup(raw_html, 'lxml')

    # Find all table rows
    table_rows = page_items.find_all('tr')
    
    # Extract data from each row
    files_with_date = []
    for row in table_rows:
        # Find cells containing filename (anchor tag) and date
        filename_cell = row.find('a')
        regex_pattern=r'\d{4}-\d{2}-\d{2}'
        collect_date=lambda text: text and re.search(regex_pattern, text)
        date_cell = row.find('td', text=collect_date)

        if filename_cell and date_cell:
            filename = filename_cell.text.strip()
            
            if filename.lower().endswith('.zip'):
                date_text = date_cell.text.strip()

                # Try converting date text to datetime object (adjust format if needed)
                try:
                    updated_at = datetime.strptime(date_text, "%Y-%m-%d %H:%M")
                    updated_at = updated_at.replace(hour=0, minute=0, second=0, microsecond=0)
                    updated_at_str = updated_at.strftime("%Y-%m-%d")
                
                except ValueError:
                    # Handle cases where date format doesn't match
                    logger.error(f"Error parsing date for file: {filename}")
                    updated_at_str = ''

                file_info = (updated_at_str, filename)
                files_with_date.append(file_info)
            
    return files_with_date


