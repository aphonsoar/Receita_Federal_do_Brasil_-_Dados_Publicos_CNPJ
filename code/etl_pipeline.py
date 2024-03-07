from wget import download 
from os import path, listdir
from timy import timer
import pandas as pd

from .utils import check_diff, delete_var, to_sql
from .URLS import DADOS_RF_URL, LAYOUT_URL
from .scrapper import scrapper_rf
from .utils import repeat_token, bar_progress

##########################################################################
## LER E INSERIR DADOS ###################################################
##########################################################################
@timer()
def ler_dados_receita_federal(extracted_files_path):
    # Files:
    Items = [name for name in listdir(extracted_files_path) if name.endswith('')]

    # Separar arquivos:
    arquivos = {
        'empresa': [],
        'estabelecimento': [],
        'socios': [],
        'simples': [],
        'cnae': [],
        'moti': [],
        'munic': [],
        'natju': [],
        'pais': [],
        'quals': [],
    }
    for i in range(len(Items)):
        if Items[i].find('EMPRE') > -1:
            arquivos['empresa'].append(Items[i])
        elif Items[i].find('ESTABELE') > -1:
            arquivos['estabelecimento'].append(Items[i])
        elif Items[i].find('SOCIO') > -1:
            arquivos['socios'].append(Items[i])
        elif Items[i].find('SIMPLES') > -1:
            arquivos['simples'].append(Items[i])
        elif Items[i].find('CNAE') > -1:
            arquivos['cnae'].append(Items[i])
        elif Items[i].find('MOTI') > -1:
            arquivos['moti'].append(Items[i])
        elif Items[i].find('MUNIC') > -1:
            arquivos['munic'].append(Items[i])
        elif Items[i].find('NATJU') > -1:
            arquivos['natju'].append(Items[i])
        elif Items[i].find('PAIS') > -1:
            arquivos['pais'].append(Items[i])
        elif Items[i].find('QUALS') > -1:
            arquivos['quals'].append(Items[i])
        else:
            pass

    return arquivos

##########################################################################
## DOWNLOAD ##############################################################
##########################################################################
def baixar_dados_receita_federal(base_files, output_files_path):
    # Download arquivos ######################################################
    i_l = 0
    for base_file in base_files:
        # Download dos arquivos
        i_l += 1
        print('Baixando arquivo:')
        print(str(i_l) + ' - ' + base_file)
        url = DADOS_RF_URL+l
        file_name = path.join(output_files_path, base_file)

        if check_diff(url, file_name):
            download(url, out=output_files_path, bar=bar_progress)

    # Download layout:
    print('Baixando layout:')
    download(LAYOUT_URL, out=output_files_path, bar=bar_progress)

from zipfile import ZipFile
from os import path

##########################################################################
## EXTRACT ###############################################################
##########################################################################
def extrair_dados_receita_federal(base_files, output_files_path, extracted_files_path):
    # Extracting files:
    i_l = 0
    for l in base_files:
        try:
            i_l += 1
            print('Descompactando arquivo:')
            print('\t' + str(i_l) + ' - ' + l)
            full_path = path.join(output_files_path, l)
            with ZipFile(full_path, 'r') as zip_ref:
                zip_ref.extractall(extracted_files_path)
        except:
            pass

def buscar_dados_receita_federal(output_files_path, extracted_files_path):
    #%%
    # Raspar dados da página 
    base_files = scrapper_rf()

    # Baixar arquivos
    baixar_dados_receita_federal(base_files, output_files_path)

    # Extrair arquivos
    extrair_dados_receita_federal(base_files, output_files_path, extracted_files_path)

    # Ler e inserir dados
    arquivos = ler_dados_receita_federal(extracted_files_path)

    return arquivos

##########################################################################
## LOAD AND TRANSFORM
##########################################################################
@timer()
def popular_tabela(
    engine, cur, conn, 
    label: str, table_name: str, files: list,
    columns: list, to_folder: str, 
    encoding: str, cleanse_transform_map: lambda x: x
):
    fence=repeat_token('#', 35)
    title=f'## Arquivos de {label.upper()}:'
    header=f'{fence}\n{title}\n{fence}'
    print(header)

    # Drop table antes do insert
    cur.execute(f'DROP TABLE IF EXISTS "{table_name}";')
    conn.commit()

    # Inserir dados
    for e in range(0, len(files)):
        print('Trabalhando no arquivo: '+files[e]+' [...]')
        delete_var(artefato)

        artefato = pd.DataFrame(columns=list(range(0, len(columns))))
        dtypes = { column: 'object' for column in columns }
        extracted_file_path = path.join(to_folder, files[e])

        artifact = pd.read_csv(
            filepath_or_buffer=extracted_file_path,
            sep=';',
            skiprows=0,
            header=None,
            dtype=dtypes,
            encoding=encoding,
        )

        # Tratamento do arquivo antes de inserir na base:
        artifact = artifact.reset_index()
        del artifact['index']

        # Renomear colunas
        artifact.columns = columns

        artifact = cleanse_transform_map(artifact)

        # Gravar dados no banco:
        to_sql(artifact, name=table_name, con=engine, if_exists='append', index=False)
        print('Arquivos ' + files[e] + ' inserido com sucesso no banco de dados!')

        delete_var(artefato)

        print(f'Arquivos de {label} finalizados!')

