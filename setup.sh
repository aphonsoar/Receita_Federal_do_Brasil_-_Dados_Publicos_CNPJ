#!/bin/bash

# Script para configurar o ambiente do projeto de ETL de dados da Receita Federal.

echo "Iniciando a configuração do ambiente..."

# Navega para o diretório do script para garantir que os caminhos estejam corretos
cd "$(dirname "$0")"

# Verifica se o arquivo .env existe na pasta code. Se não, copia o template.
if [ -f "code/.env" ]; then
    echo "O arquivo 'code/.env' já existe. Nenhuma ação necessária."
else
    echo "O arquivo 'code/.env' não foi encontrado. Copiando de 'code/.env_template'..."
    cp "code/.env_template" "code/.env"
    echo "Arquivo 'code/.env' criado com sucesso."
fi

echo ""
echo "Instalando as dependências do Python listadas em requirements.txt..."

# Instala as dependências usando pip
pip install -r requirements.txt

echo ""
echo "------------------------------------------------------------------"
echo "Setup concluído com sucesso!"
echo "Ação necessária: Edite o arquivo 'code/.env' com as suas"
echo "configurações de banco de dados e caminhos de pasta."
echo "------------------------------------------------------------------"
