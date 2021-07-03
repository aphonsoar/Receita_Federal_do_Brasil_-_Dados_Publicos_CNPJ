from datetime import date
from pathlib import Path
import ftplib
import os
import pandas as pd
import re
import sys
import time
import zipfile
import var
import files
import database

#%%
###########################
## DOWNLOAD/EXTRACTION ####
###########################

#%%%
# Obter arquivos
Files = files.get_file_list()

#%%%
# Download dos arquivos .zip
files.download_files(Files)

#%%%
# Download do layout .pdf
files.download_layout()

#%%%
# Creating directory to store the extracted files:
if not os.path.exists(var.extracted_files):
    os.mkdir(var.extracted_files)

#%%%
# Extracting files:
files.extract_zip_files(Files)

#%%
###########################
## LER E INSERIR DADOS ####
###########################

insert_start = time.time()

# Files:
items = [name for name in os.listdir(var.extracted_files) if name.endswith('')]

# Separar arquivos:
files.sort_files_by_category(items, var.arquivos)

# Conectar no banco de dados:
db_engine, db_connection, db_cursor = database.start()

#%%%
# Arquivos de empresa:
column_labels = [
    'cnpj_basico',
    'razao_social',
    'natureza_juridica',
    'qualificacao_responsavel',
    'capital_social',
    'porte_empresa',
    'ente_federativo_responsavel'
]
database.insert_data(
    db_engine,
    db_connection,
    db_cursor,
    var.arquivos['empresa'],
    'empresa',
    column_labels
)

#%%%
# Arquivos de estabelecimento:
column_labels = [
    'cnpj_basico',
    'cnpj_ordem',
    'cnpj_dv',
    'identificador_matriz_filial',
    'nome_fantasia',
    'situacao_cadastral',
    'data_situacao_cadastral',
    'motivo_situacao_cadastral',
    'nome_cidade_exterior',
    'pais',
    'data_inicio_atividade',
    'cnae_fiscal_principal',
    'cnae_fiscal_secundaria',
    'tipo_logradouro',
    'logradouro',
    'numero',
    'complemento',
    'bairro',
    'cep',
    'uf',
    'municipio',
    'ddd_1',
    'telefone_1',
    'ddd_2',
    'telefone_2',
    'ddd_fax',
    'fax',
    'correio_eletronico',
    'situacao_especial',
    'data_situacao_especial'
]
    
database.insert_data(
    db_engine,
    db_connection,
    db_cursor,
    var.arquivos['estabelecimento'],
    'estabelecimento',
    column_labels
)

#%%%
# Arquivos de socios:
column_labels = [
    'cnpj_basico',
    'identificador_socio',
    'nome_socio_razao_social',
    'cpf_cnpj_socio',
    'qualificacao_socio',
    'data_entrada_sociedade',
    'pais',
    'representante_legal',
    'nome_do_representante',
    'qualificacao_representante_legal',
    'faixa_etaria'
]
database.insert_data(
    db_engine,
    db_connection,
    db_cursor,
    var.arquivos['socios'],
    'socios',
    column_labels
)

#%%%
# Arquivos de simples:
column_labels = [
    'cnpj_basico',
    'opcao_pelo_simples',
    'data_opcao_simples',
    'data_exclusao_simples',
    'opcao_mei',
    'data_opcao_mei',
    'data_exclusao_mei'
]
database.insert_data(
    db_engine, db_connection,
    db_cursor, var.arquivos['simples'],
    'simples',
    column_labels
)

#%%%
# Arquivos de cnae:
column_labels = ['codigo', 'descricao']
database.insert_data(
    db_engine,
    db_connection,
    db_cursor,
    var.arquivos['cnae'],
    'cnae',
    column_labels
)

#%%%
# Arquivos de moti:
column_labels = ['codigo', 'descricao']
database.insert_data(db_engine, db_connection, db_cursor, var.arquivos['moti'], 'moti', column_labels)

#%%%
# Arquivos de munic:
column_labels = ['codigo', 'descricao']
database.insert_data(
    db_engine, db_connection,
    db_cursor,
    var.arquivos['munic'],
    'munic',
    column_labels
)

#%%%
# Arquivos de natju:
column_labels = ['codigo', 'descricao']
database.insert_data(
    db_engine,
    db_connection,
    db_cursor,
    var.arquivos['natju'],
    'natju',
    column_labels
)

#%%%
# Arquivos de pais:
column_labels = ['codigo', 'descricao']
database.insert_data(
    db_engine,
    db_connection,
    db_cursor,
    var.arquivos['pais'],
    'pais',
    column_labels
)

#%%%
# Arquivos de qualificação de sócios:
column_labels = ['codigo', 'descricao']
database.insert_data(
    db_engine,
    db_connection,
    db_cursor,
    var.arquivos['quals'],
    'quals',
    column_labels
)

#%%
insert_end = time.time()
Tempo_insert = round((insert_end - insert_start))

print("""
#############################################
## Processo de carga dos arquivos finalizado!
#############################################
""")

print('Tempo total de execução do processo de carga (em segundos): ' + str(Tempo_insert)) # Tempo de execução do processo (em segundos): 17.770 (4hrs e 57 min)

# ###############################
# Tamanho dos arquivos:
# empresa = 45.811.638
# estabelecimento = 48.421.619
# socios = 20.426.417
# simples = 27.893.923
# ###############################

#%%
# Criar índices na base de dados:
index_start = time.time()
print("""
#######################################
## Criar índices na base de dados [...]
#######################################
""")
db_cursor.execute("""
create index empresa_cnpj on empresa(cnpj_basico);
commit;
create index estabelecimento_cnpj on estabelecimento(cnpj_basico);
commit;
create index socios_cnpj on socios(cnpj_basico);
commit;
create index simples_cnpj on simples(cnpj_basico);
commit;
""")
db_connection.commit()
print("""
############################################################
## Índices criados nas tabelas, para a coluna `cnpj_basico`:
   - empresa
   - estabelecimento
   - socios
   - simples
############################################################
""")
index_end = time.time()
index_time = round(index_end - index_start)
print('Tempo para criar os índices (em segundos): ' + str(index_time))

#%%
print("""Processo 100% finalizado! Você já pode usar seus dados no BD!
 - Desenvolvido por: Aphonso Henrique do Amaral Rafael
 - Contribua com esse projeto aqui: https://github.com/aphonsoar/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ
""")