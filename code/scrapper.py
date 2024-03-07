import re

from urllib import request
from bs4 import BeautifulSoup

from .utils import delete_var
from .URLS import DADOS_RF_URL 

def scrap_rf():
    raw_html = request.urlopen(DADOS_RF_URL)
    raw_html = raw_html.read()

    # Formatar página e converter em string
    page_items = BeautifulSoup(raw_html, 'lxml')
    html_str = str(page_items)

    # Obter arquivos
    Files = []
    text = '.zip'
    for m in re.finditer(text, html_str):
        i_start = m.start()-40
        i_end = m.end()
        i_loc = html_str[i_start:i_end].find('href=')+6
        Files.append(html_str[i_start+i_loc:i_end])

    # Correcao do nome dos arquivos devido a mudanca na estrutura do HTML da pagina
    # 31/07/22 - Aphonso Rafael
    Files_clean = []
    for i in range(len(Files)):
        if not Files[i].find('.zip">') > -1:
            Files_clean.append(Files[i])

    delete_var(Files)
    
    print('Arquivos que serão baixados:')
    i_f = 0
    for f in Files_clean:
        i_f += 1
        print(str(i_f) + ' - ' + f)

    return Files_clean