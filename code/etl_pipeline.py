from wget import download 
from os import path, listdir
from timy import timer
import pandas as pd
from numpy import ceil
from zipfile import ZipFile

from utils import check_diff, delete_var, to_sql
from URLS import DADOS_RF_URL, LAYOUT_URL
from scrapper import raspar_receita_federal
from utils import repeat_token, bar_progress
from constants import CERCA_COMPRIMENTO, TAMANHO_DAS_PARTES

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
    base_files = raspar_receita_federal()

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
@timer('Popular tabela')
def popular_tabela(
    engine, cur, conn, 
    label: str, table_name: str, files: list,
    columns: list, to_folder: str, 
    encoding: str, cleanse_transform_map: lambda x: x
):
    fence=repeat_token('#', CERCA_COMPRIMENTO)
    title=f'## Arquivos de {label.upper()}:'
    header=f'{fence}\n{title}\n{fence}'
    print(header)

    # Drop table antes do insert
    cur.execute(f'DROP TABLE IF EXISTS "{table_name}";')
    conn.commit()

    # Inserir dados
    for file in files:
        print('Trabalhando no arquivo: '+file+' [...]')
        delete_var(artefato)

        artefato = pd.DataFrame(columns=list(range(0, len(columns))))
        dtypes = { column: 'object' for column in columns }
        extracted_file_path = path.join(to_folder, file)

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
        print('Arquivos ' + file + ' inserido com sucesso no banco de dados!')

        delete_var(artefato)

        print(f'Arquivos de {label} finalizados!')

@timer('Popular tabela por partes')
def popular_tabela_por_partes(
    engine, cur, conn, 
    label: str, nome_da_tabela: str, arquivos: list,
    colunas: list, para_pasta: str, 
    encoding: str, cleanse_transform_map: lambda x: x
):
    fence=repeat_token('#', CERCA_COMPRIMENTO)
    title=f'## Arquivos de {label.upper()}:'
    header=f'{fence}\n{title}\n{fence}'
    print(header)

    # Drop table antes do insert
    cur.execute(f'DROP TABLE IF EXISTS "{nome_da_tabela}";')
    conn.commit()

    # Inserir dados
    for arquivo in arquivos:
        print('Trabalhando no arquivo: '+arquivo+' [...]')
        delete_var(artefato)

        artefato = pd.DataFrame(columns=list(range(0, len(colunas))))
        dtypes = { column: 'object' for column in colunas }
        extracted_file_path = path.join(para_pasta, colunas[e])

        contadem_de_linhas = sum(1 for _ in open(extracted_file_path, "r"))
        print('Linhas no arquivo '+ arquivo +': '+str(contadem_de_linhas))

        n_partes = ceil(contadem_de_linhas / TAMANHO_DAS_PARTES)
        nrows = TAMANHO_DAS_PARTES
        skiprows = 0

        for _ in range(0, n_partes):
            artifact = pd.read_csv(
                filepath_or_buffer=extracted_file_path,
                sep=';',
                skiprows=skiprows,
                header=None,
                dtype=dtypes,
                encoding=encoding,
            )

            # Tratamento do arquivo antes de inserir na base:
            artifact = artifact.reset_index()
            del artifact['index']

            # Renomear colunas
            artifact.columns = colunas
            artifact = cleanse_transform_map(artifact)

            skiprows = skiprows+nrows

            # Gravar dados no banco:
            to_sql(artifact, name=nome_da_tabela, con=engine, if_exists='append', index=False)
            print('Arquivos ' + arquivo + ' inserido com sucesso no banco de dados!')

            delete_var(artefato)

        print(f'Arquivos de {label} finalizados!')

