from time import time
import os, pathlib, logging

from formatadorDeTempo import formatarIntervalorTemporal
from environmental_variable_fetcher import loadEnvironmentalVariables
from databaseContext import DatabaseContext
from data_download import (download, 
                           unzipFiles, 
                           segregarDadosPorTabelaDoBD)

from estabelecimentos import processarEstabelecimentos
from simples import processarSimples
from socios import processarSocios
from empresas import processarEmpresas
from cnae import processarCNAEs
from moti import processarMOTI
from municipio import processarMunicipios
from natju import processarNATJU
from pais import processarPaises
from qualificacaoDeSocio import processarQualificacoesDeSocios
from outputDirectoryManager import loadOoutPutDirectories

#%% Iniciando contador de tempo


localdir = os.getcwd()
logfilename = os.path.join(localdir, 'app.log')
logging.basicConfig(level=logging.INFO, 
                    filename=logfilename,  # Specify the log file name
                    format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("Iniciando processamento")

loadEnvironmentalVariables()

start = time()

#%% diretórios para trabalho

(output_directory_of_downloaded_files, 
 output_directory_of_extracted_files) = loadOoutPutDirectories()

#%% Extracting files:
    
Files: list[str] = download(output_directory_of_downloaded_files)
unzipFiles(Files, 
           output_directory_of_downloaded_files,
           output_directory_of_extracted_files)

dataHolder = segregarDadosPorTabelaDoBD(output_directory_of_extracted_files)

#%% Contexto de comunicação com o BD

databaseContext = DatabaseContext()
databaseContext.sanitizarBD()

#%% processando estabelecimentos

print('Tem %i arquivos de estabelecimento!' % len(dataHolder.arquivos_estabelecimento))

processarEstabelecimentos(dataHolder.arquivos_estabelecimento, 
                           output_directory_of_extracted_files,
                           databaseContext)
        

#%% Arquivos de empresa:

processarEmpresas(dataHolder, 
                  databaseContext,
                  output_directory_of_extracted_files)

#%% Arquivos de socios:
    
processarSocios(dataHolder,
                databaseContext,
                output_directory_of_extracted_files)
    

#%% Arquivos de simples:

processarSimples(dataHolder, 
                 databaseContext,
                 output_directory_of_extracted_files)

#%% Arquivos de cnae:
    
processarCNAEs(dataHolder, 
               databaseContext,
               output_directory_of_extracted_files)

#%% Arquivos de moti:
    
processarMOTI(dataHolder, 
               databaseContext,
               output_directory_of_extracted_files)
    
#%% Arquivos de munic:

processarMunicipios(dataHolder, 
                    databaseContext,
                    output_directory_of_extracted_files)    

#%% Arquivos de natureza jurídica:
    
processarNATJU(dataHolder, 
               databaseContext,
               output_directory_of_extracted_files)    


#%% Arquivos de pais:
    
processarPaises(dataHolder, 
                databaseContext,
                output_directory_of_extracted_files)   
    
#%% Arquivos de qualificação de sócios:

processarQualificacoesDeSocios(dataHolder, 
                               databaseContext,
                               output_directory_of_extracted_files)  


#%% Estabelecendo índices no BD

def criarIndicesNasTabelasdoBd():
    index_start = time()
    print("""
    #######################################
    ## Criar índices na base de dados [...]
    #######################################
    """)
    databaseContext.cur.execute("""
    create index if not exists empresa_cnpj on empresa(cnpj_basico);
    commit;
    create index if not exists estabelecimento_cnpj on estabelecimento(cnpj_basico);
    commit;
    create index if not exists socios_cnpj on socios(cnpj_basico);
    commit;
    create index if not exists simples_cnpj on simples(cnpj_basico);
    commit;
    """)
    databaseContext.conn.commit()
    print("""
    ############################################################
    ## Índices criados nas tabelas, para a coluna `cnpj_basico`:
       - empresa
       - estabelecimento
       - socios
       - simples
    ############################################################
    """)
    
    index_end = time()
    index_time = round(index_end - index_start)
    print('Tempo para criar os índices (em segundos): ' + str(index_time))


criarIndicesNasTabelasdoBd()

#%% Finalização da rotina

end = time()
dt = formatarIntervalorTemporal(end - start)
del  start, end

print("""
#############################################
## Processo de carga dos arquivos finalizado!
#############################################
""")

print('Tempo total de execução do processo de carga: ' + str(dt)) # Tempo de execução do processo (em segundos): 17.770 (4hrs e 57 min)

logging.info(f"Processamento finalizado. {dt}")

databaseContext.close()


