# Dados Públicos CNPJ

A Receita Federal do Brasil disponibiliza bases com os dados públicos do cadastro nacional de pessoas jurídicas (CNPJ). De forma geral, nelas constam as mesmas informações que conseguimos ver no cartão do CNPJ, quando fazemos uma consulta individual, acrescidas de outros dados de Simples Nacional, sócios e etc. Análises muito ricas podem sair desses dados, desde econômicas, mercadológicas até investigações.

Nesse repositório consta um processo de ETL para: 

  1. baixar os arquivos; 
  
  2. descompactar; 
  
  3. ler e tratar
  
  4. inserir num banco de dados relacional PostgreSQL.

---------------------

## Base de dados:

- Fonte de dados: https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-jurdica---cnpj.
- Layout: https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf.

---------------------

## Infraestrutura necessária:

- [Python 3.8](https://www.python.org/downloads/release/python-3810/)
- [PostgreSQL 14.2](https://www.postgresqltutorial.com/postgresql-getting-started/install-postgresql-linux/)
  
---------------------

## Como usar:

1. Com o Postgres instalado, inicie a instância do servidor (pode ser local) e crie o banco de dados conforme o arquivo `banco_de_dados.sql`. Os comandos abaixo são executador em ambiente Linux:

   - Execute os comandos `sudo -u postgres psql`;
   - Crie um usuário e senha de preferência. Exemplo: `ALTER USER postgres PASSWORD 'postgres';`
   - Copie-cole o conteúdo do arquivo `banco_de_dados.sql`
 
2. Crie um arquivo `.env` no diretório `code`, conforme as variáveis de ambiente do seu ambiente de trabalho (localhost). Utilize como referência o arquivo `.env_template`. Você pode também, por exemplo, renomear o arquivo de `.env_template` para apenas `.env` e então utilizá-lo:

   - `POSTGRES_USER`        : usuário do banco de dados criado pelo arquivo `banco_de_dados.sql`
   - `POSTGRES_PASSWORD`    : senha do usuário do BD
   - `POSTGRES_HOST`        : host da conexão com o BD 
   - `POSTGRES_PORT`        : porta da conexão com o BD 
   - `POSTGRES_NAME`        : nome da base de dados na instância (`Dados_RFB` - conforme arquivo `banco_de_dados.sql`)
   - `OUTPUT_PATH`          : (Opcional) diretório de destino para o donwload dos arquivos
   - `EXTRACT_PATH`         : (Opcional) diretório de destino para a extração dos arquivos .zip
   - `ENVIRONMENT`          : (Opcional) ambiente "development", "staging", "production"

3. Instale as bibliotecas necessárias, disponíveis em `requirements.txt`:
```
pip install uv && uv pip install -r requirements.txt
```

4. Execute o arquivo `src/main.py` e aguarde a finalização do processo.
   - Os arquivos são grandes. Dependendo da infraestrutura isso deve levar muitas horas para conclusão.
   - Arquivos de 08/05/2021: `4,68 GB` compactados e `17,1 GB` descompactados.
    
---------------------

## Tabelas geradas:

Para maiores informações, consulte o [layout](https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/arquivos/NOVOLAYOUTDOSDADOSABERTOSDOCNPJ.pdf).

  - `empresa`: dados cadastrais da empresa em nível de matriz
  - `estabelecimento`: dados analíticos da empresa por unidade / estabelecimento (telefones, endereço, filial, etc)
  - `socios`: dados cadastrais dos sócios das empresas
  - `simples`: dados de MEI e Simples Nacional
  - `cnae`: código e descrição dos CNAEs
  - `quals`: tabela de qualificação das pessoas físicas - sócios, responsável e representante legal.  
  - `natju`: tabela de naturezas jurídicas - código e descrição.
  - `moti`: tabela de motivos da situação cadastral - código e descrição.
  - `pais`: tabela de países - código e descrição.
  - `munic`: tabela de municípios - código e descrição.


Pelo volume de dados, as tabelas  `empresa`, `estabelecimento`, `socios` e `simples` possuem índices para a coluna `cnpj_basico`, a principal chave de ligação entre elas.

### Modelo de Entidade Relacionamento:

![alt text](https://github.com/brunolnetto/RF_CNPJ/blob/master/images/Dados_RFB_ERD.png)


