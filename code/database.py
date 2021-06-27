from pathlib import Path
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import re
import subprocess
import time
import var
from codec import codec

#%%
def start():
    db_engine = create_engine(
        'postgresql://' +
        var.user + ':' +
        var.password + '@' +
        var.host + ':' +
        var.port + '/' +
        var.database
    )

    db_connection = psycopg2.connect(
        'dbname=' + var.database + ' ' +
        'user=' + var.user + ' ' +
        'host=' + var.host + ' ' +
        'password=' + var.password
    )

    db_cursor = db_connection.cursor()
    
    return db_engine, db_connection, db_cursor

#%%
def insert_data(
    db_engine,
    db_connection,
    db_cursor,
    files,
    table_name,
    column_labels
):
    insert_start = time.time()
    print(f'#######################\
    ## Arquivos de {table_name.upper()}:\
    #######################\
    ')

    # Drop table antes do insert
    db_cursor.execute(f'DROP TABLE IF EXISTS "{table_name}";')
    db_connection.commit()
    
    for e in range(0, len(files)):
        print('Trabalhando no arquivo: '+files[e]+' [...]')
        try:
            del table
        except:
            pass

        extracted_file_path = Path(var.extracted_files / files[e])
        extracted_file_io = open(extracted_file_path, "rb")
        file_length = sum(1 for line in extracted_file_io)

        print(f'Linhas no arquivo {files[e]}: {file_length}')

        row_size = 1000000 # Registros por carga
        if file_length < row_size:
            row_size = file_length

        if(row_size > 0):
            partes = round(file_length / row_size)
            nrows = row_size
            skiprows = 0

            print('Este arquivo será dividido em ' + str(partes) + ' partes para inserção no banco de dados')

            for i in range(0, partes):
                print('Iniciando a parte ' + str(i+1) + ' [...]')

                escaped_extracted_file_path = str(extracted_file_path).replace("$", "\$")
                commandOutput = subprocess.check_output(f'file -bi {escaped_extracted_file_path}', shell=True)
                charset = re.search('charset=(.*)', commandOutput.decode("utf-8")).group(1)
                charset = codec[charset]
                charset = 'iso-8859-1'

                table = pd.read_csv(
                    filepath_or_buffer=extracted_file_path,
                    sep=';',
                    na_values='',
                    engine='python',
                    encoding=charset,
                    nrows=nrows,
                    skiprows=skiprows,
                    header=None,
                    dtype='object'
                )

                # Tratamento do arquivo antes de inserir na base:
                table = table.reset_index()
                del table['index']

                # Renomear colunas
                table.columns = column_labels

                if 'empresa' == table_name:
                    # Replace "," por "."
                    table['capital_social'] = table['capital_social'].apply(lambda x: x.replace(',','.'))
                    table['capital_social'] = table['capital_social'].astype(float)

                skiprows = skiprows+nrows

                # Gravar dados no banco:
                table.to_sql(name=table_name, con=db_engine, if_exists='append', index=False)
                print('Arquivo ' + files[e] + ' inserido com sucesso no banco de dados! - Parte '+ str(i+1))

                try:
                    del table
                except:
                    pass

    try:
        del table
    except:
        pass
    print('Arquivos de empresa finalizados!')
    insert_end = time.time()
    tempo_insert = round((insert_end - insert_start))
    print('Tempo de execução do processo de empresa (em segundos): ' + str(tempo_insert))