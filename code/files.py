from utils import bar_progress, get_formated_pages
from var import dados_rf, extracted_files, output_files
import os
import re
import wget
import zipfile
import sys
from color import print_in_gray, print_in_blue

#%%
def get_file_list():
    Files = []
    text = '.zip'
    html_str = get_formated_pages(dados_rf)
    for m in re.finditer(text, html_str):
        i_start = m.start()-40
        i_end = m.end()
        i_loc = html_str[i_start:i_end].find('href=')+6
        Files.append(html_str[i_start+i_loc:i_end])

    print_in_blue('Arquivos que serão baixados:', bold=True)
    i_f = 0
    total = len(Files)
    for file_name in Files:
        i_f += 1
        print_in_gray(f' [{str(i_f)}/{total}] ', break_line=False)
        print(file_name)

    return Files

def download_files(Files):
    i_l = 0
    total = len(Files)
    for l in Files:
        # Download dos arquivos
        i_l += 1
        file_path = output_files / l
        if not os.path.exists(file_path):
            print(f'[{str(i_l)}/{total}] {l}')
            url = dados_rf+l
            wget.download(url, out=str(output_files), bar=bar_progress)

def download_layout():
    if not os.path.exists(output_files / 'NOVOLAYOUTDOSDADOSABERTOSDOCNPJ.pdf'):
        Layout = 'https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/arquivos/NOVOLAYOUTDOSDADOSABERTOSDOCNPJ.pdf'
        print('Baixando layout:')
        wget.download(Layout, out=output_files, bar=bar_progress)

def extract_zip_files(Files):
    i_l = 0
    for l in Files:
        try:
            i_l += 1
            extracted_file_name = re.search('(.*)(.zip$)', l).group(1)
            print(f'Descompactando arquivo {i_l} - {l} para {extracted_file_name}')

            if not os.path.exists(extracted_files / extracted_file_name):
                with zipfile.ZipFile(output_files / l, 'r') as zip_ref:
                    zip_ref.extractall(extracted_files)
        except:
            pass

def sort_files_by_category(items, arquivos):
    for i in range(len(items)):
        if items[i].find('EMPRE') > -1:
            arquivos['empresa'].append(items[i])
        elif items[i].find('ESTABELE') > -1:
            arquivos['estabelecimento'].append(items[i])
        elif items[i].find('SOCIO') > -1:
            arquivos['socios'].append(items[i])
        elif items[i].find('SIMPLES') > -1:
            arquivos['simples'].append(items[i])
        elif items[i].find('CNAE') > -1:
            arquivos['cnae'].append(items[i])
        elif items[i].find('MOTI') > -1:
            arquivos['moti'].append(items[i])
        elif items[i].find('MUNIC') > -1:
            arquivos['munic'].append(items[i])
        elif items[i].find('NATJU') > -1:
            arquivos['natju'].append(items[i])
        elif items[i].find('PAIS') > -1:
            arquivos['pais'].append(items[i])
        elif items[i].find('QUALS') > -1:
            arquivos['quals'].append(items[i])
        else:
            pass
