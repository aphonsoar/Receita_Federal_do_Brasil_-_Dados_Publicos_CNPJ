# CNPJ Data Processor

Este projeto é um pipeline de ETL (Extração, Transformação e Carga) para processar os dados públicos de CNPJ da Receita Federal do Brasil e carregá-los em um banco de dados SQL Server Express.

## Funcionalidades

- **Download Automático**: Baixa os arquivos de dados mais recentes diretamente do site da Receita Federal.
- **Extração de Dados**: Descompacta os arquivos baixados.
- **Limpeza e Higienização**: Corrige inconsistências e erros de formatação nos arquivos CSV.
- **Carga de Dados Otimizada**: Carrega os dados em um banco de dados SQL Server Express de forma eficiente.
- **Criação de Views**: Inclui scripts SQL para criar views que facilitam a consulta dos dados.

## Requisitos

- Python 3.8 ou superior
- SQL Server Express
- Driver ODBC para SQL Server

## Como Usar

1. **Clone o repositório:**
   ```bash
   git clone https://github.com/aphonsoar/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ.git
   cd Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ
   ```

2. **Instale as dependências:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure as variáveis de ambiente:**
   - Copie o arquivo `code/.env_template` para `code/.env`.
   - Edite o arquivo `code/.env` com as suas configurações de banco de dados:
     ```
     # URL for downloading the data from Receita Federal
     DADOS_RF_URL="https://arquivos.receitafederal.gov.br/dados-publicos-cnpj/"

     # Path to store the downloaded zip files
     OUTPUT_FILES_PATH="../OUTPUT/"

     # Path to store the extracted csv files
     EXTRACTED_FILES_PATH="../EXTRACTED/"

     # Database connection settings for SQL Server
     DB_DRIVER="ODBC Driver 17 for SQL Server"
     DB_SERVER="your_server_name"
     DB_NAME="Dados_RFB"
     DB_USER="your_username"
     DB_PASSWORD="your_password"
     ```

4. **Execute o processador:**
   ```bash
   python code/cnpj_processor.py
   ```

5. **Execute os scripts SQL (opcional):**
   - Para criar as views de consulta, execute os scripts na pasta `sql/` no seu banco de dados.

## Estrutura do Projeto

- `code/`: Contém o código fonte do projeto.
  - `cnpj_processor.py`: O script principal do pipeline de ETL.
  - `.env_template`: Template para o arquivo de configuração de ambiente.
- `sql/`: Contém scripts SQL para criar views no banco de dados.
- `OUTPUT/`: Diretório padrão para os arquivos .zip baixados.
- `EXTRACTED/`: Diretório padrão para os arquivos .csv extraídos.
- `LICENSE`: A licença do projeto.
- `README.md`: Este arquivo.
- `requirements.txt`: As dependências do projeto.
