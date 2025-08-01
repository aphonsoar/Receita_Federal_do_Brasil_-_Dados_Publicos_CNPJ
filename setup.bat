@echo off
REM Script para configurar o ambiente do projeto de ETL de dados da Receita Federal no Windows.

echo Iniciando a configuracao do ambiente...

REM Muda o diretorio para o local do script para garantir que os caminhos estejam corretos
cd /d "%~dp0"

REM Verifica se o arquivo .env existe na pasta code. Se nao, copia o template.
if exist "code\.env" (
    echo O arquivo 'code\.env' ja existe. Nenhuma acao necessaria.
) else (
    echo O arquivo 'code\.env' nao foi encontrado. Copiando de 'code\.env_template'...
    copy "code\.env_template" "code\.env"
    echo Arquivo 'code\.env' criado com sucesso.
)

echo.
echo Instalando as dependencias do Python listadas em requirements.txt...

REM Instala as dependencias usando pip
pip install -r requirements.txt

echo.
echo ------------------------------------------------------------------
echo Setup concluido com sucesso!
echo.
echo Acao necessaria: Edite o arquivo 'code\.env' com as suas
echo configuracoes de banco de dados e caminhos de pasta.
echo ------------------------------------------------------------------
pause
