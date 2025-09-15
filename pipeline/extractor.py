import os
import zipfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Generator, Tuple
import tempfile
import shutil
from tqdm import tqdm

from config import Config

class OptimizedExtractor:
    def __init__(self):
        self.settings = Config.get_optimal_settings()
        self.lock = threading.Lock()

    def get_zip_files(self) -> List[Path]:
        """Retorna lista de arquivos ZIP para extração"""
        output_path = Path(Config.OUTPUT_FILES)
        return list(output_path.glob("*.zip"))

    def extract_single_file(self, zip_path: Path, progress_bar: tqdm = None) -> dict:
        """Extrai um único arquivo ZIP de forma otimizada"""
        try:
            extracted_path = Path(Config.EXTRACTED_FILES)
            extracted_path.mkdir(parents=True, exist_ok=True)

            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Lista arquivos no ZIP
                file_list = zip_ref.namelist()

                for file_name in file_list:
                    # Extração otimizada com buffer maior
                    source = zip_ref.open(file_name)
                    target_path = extracted_path / file_name

                    # Evita sobrescrever se já existe e tem mesmo tamanho
                    if target_path.exists():
                        zip_info = zip_ref.getinfo(file_name)
                        if target_path.stat().st_size == zip_info.file_size:
                            continue

                    # Extração com buffer otimizado
                    with open(target_path, 'wb') as target:
                        shutil.copyfileobj(source, target, length=1024*1024)  # Buffer 1MB
                    source.close()

            if progress_bar:
                with self.lock:
                    progress_bar.update(1)

            return {
                "file": zip_path.name,
                "status": "extracted",
                "files_count": len(file_list)
            }

        except Exception as e:
            if progress_bar:
                with self.lock:
                    progress_bar.update(1)
            return {
                "file": zip_path.name,
                "status": "failed",
                "error": str(e)
            }

    def extract_all_parallel(self, zip_files: List[Path]) -> List[dict]:
        """Extrai todos os arquivos ZIP em paralelo"""
        if not zip_files:
            print("⚠️  Nenhum arquivo ZIP encontrado para extração")
            return []

        print(f"📦 Extraindo {len(zip_files)} arquivos ZIP...")

        results = []
        with tqdm(total=len(zip_files), desc="Extraindo", unit="arquivo") as pbar:
            with ThreadPoolExecutor(max_workers=self.settings['io_workers']) as executor:
                # Submete todas as tarefas
                future_to_file = {
                    executor.submit(self.extract_single_file, zip_path, pbar): zip_path
                    for zip_path in zip_files
                }

                # Coleta resultados conforme completam
                for future in as_completed(future_to_file):
                    result = future.result()
                    results.append(result)

        successful = sum(1 for r in results if r.get("status") == "extracted")
        failed = len(results) - successful
        total_files = sum(r.get("files_count", 0) for r in results if r.get("status") == "extracted")

        print(f"✅ Extração concluída: {successful} sucessos, {failed} falhas")
        print(f"📄 Total de arquivos extraídos: {total_files}")

        return results

class StreamingExtractor:
    """Extrator que processa arquivos diretamente do ZIP sem extrair completamente"""

    def __init__(self):
        self.settings = Config.get_optimal_settings()

    def stream_csv_from_zip(self, zip_path: Path, csv_filename: str) -> Generator[str, None, None]:
        """Lê CSV diretamente do ZIP linha por linha"""
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                with zip_ref.open(csv_filename, 'r') as csv_file:
                    # Lê em chunks para economizar memória
                    buffer = ""
                    while True:
                        chunk = csv_file.read(8192).decode('latin1', errors='ignore')
                        if not chunk:
                            if buffer:
                                yield buffer
                            break

                        buffer += chunk
                        lines = buffer.split('\n')
                        buffer = lines[-1]  # Mantém linha incompleta no buffer

                        for line in lines[:-1]:
                            if line.strip():  # Ignora linhas vazias
                                yield line

        except Exception as e:
            print(f"❌ Erro ao ler {csv_filename} de {zip_path}: {e}")

    def get_csv_files_in_zip(self, zip_path: Path) -> List[str]:
        """Lista arquivos CSV dentro do ZIP"""
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                return [f for f in zip_ref.namelist() if f.lower().endswith('.csv')]
        except Exception:
            return []

def extract_files_optimized():
    """Função principal para extração otimizada"""
    extractor = OptimizedExtractor()
    zip_files = extractor.get_zip_files()

    if not zip_files:
        print("⚠️  Nenhum arquivo ZIP encontrado em", Config.OUTPUT_FILES)
        return []

    return extractor.extract_all_parallel(zip_files)