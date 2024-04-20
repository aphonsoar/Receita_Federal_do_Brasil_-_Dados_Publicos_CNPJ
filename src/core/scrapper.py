import re
from urllib import request
from bs4 import BeautifulSoup

from utils.misc import delete_var
from core.constants import DADOS_RF_URL 

def scrap_RF():
    raw_html = request.urlopen(DADOS_RF_URL)
    raw_html = raw_html.read()

    # Formatar página e converter em string
    page_items = BeautifulSoup(raw_html, 'lxml')
    html_str = str(page_items)

    # Obter arquivos
    files = []
    text = '.zip'
    for m in re.finditer(text, html_str):
        i_start = m.start()-40
        i_end = m.end()
        i_loc = html_str[i_start:i_end].find('href=')+6
        files.append(html_str[i_start+i_loc:i_end])

    files_clean = []
    for i in range(len(files)):
        if not files[i].find('.zip">') > -1:
            files_clean.append(files[i])

    delete_var(files)

    print('Arquivos que serão baixados:')
    i_f = 0
    for f in files_clean:
        i_f += 1
        print(str(i_f) + ' - ' + f)

    return files_clean