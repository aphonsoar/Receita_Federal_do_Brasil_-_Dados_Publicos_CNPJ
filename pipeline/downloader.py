import re
import urllib.request
import bs4
import wget
import os
from utils.filesystem import check_diff
from config import Config

def fetch_file_list():
    raw_html = urllib.request.urlopen(Config.BASE_URL).read()
    page_items = bs4.BeautifulSoup(raw_html, 'lxml')
    html_str = str(page_items)

    files = []
    for m in re.finditer(".zip", html_str):
        i_start, i_end = m.start()-40, m.end()
        i_loc = html_str[i_start:i_end].find('href=')+6
        files.append(html_str[i_start+i_loc:i_end])

    return [f for f in files if ".zip\">" not in f]

def download_files(files):
    for f in files:
        url = Config.BASE_URL + f
        file_name = os.path.join(Config.OUTPUT_FILES, f)
        if check_diff(url, file_name):
            print(f"Baixando {f}...")
            wget.download(url, out=Config.OUTPUT_FILES)