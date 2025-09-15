import os
import argparse
from pipeline.extractor import download_files, extract_zip_files
from pipeline.loader import load_all
from pipeline.organizer import group_files_by_type
from config import Config

def main(args):
    # 1️⃣ Garantir que os diretórios existem
    os.makedirs(Config.OUTPUT_FILES, exist_ok=True)
    os.makedirs(Config.EXTRACTED_FILES, exist_ok=True)

    # 2️⃣ Download dos arquivos
    if args.download:
        print("🔹 Iniciando download dos arquivos...")
        download_files(Config.DATA_URL, Config.OUTPUT_FILES)
        print("✅ Download concluído.")

    # 3️⃣ Extração dos arquivos zip
    if args.extract:
        print("🔹 Extraindo arquivos...")
        extract_zip_files(Config.OUTPUT_FILES, Config.EXTRACTED_FILES)
        print("✅ Extração concluída.")

    # 4️⃣ Agrupamento dos arquivos por tipo
    print("🔹 Agrupando arquivos por tipo...")
    groups = group_files_by_type(Config.EXTRACTED_FILES)
    for key, files in groups.items():
        print(f"  {key}: {len(files)} arquivos encontrados.")

    # 5️⃣ Carregamento no banco de dados
    if args.load:
        print("🔹 Carregando dados no banco de dados...")
        load_all(groups)
        print("✅ Carregamento finalizado com criação de índices.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline de ingestão de dados do CNPJ.")
    parser.add_argument("--download", action="store_true", help="Fazer download dos arquivos do servidor")
    parser.add_argument("--extract", action="store_true", help="Extrair arquivos ZIP baixados")
    parser.add_argument("--load", action="store_true", help="Carregar arquivos no banco de dados")

    args = parser.parse_args()
    main(args)