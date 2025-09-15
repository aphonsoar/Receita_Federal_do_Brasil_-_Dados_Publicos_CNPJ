import re
import os
import requests
import asyncio
import aiohttp
import aiofiles
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
from pathlib import Path
import time
from typing import List, Dict
import bs4
from tqdm import tqdm

from config import Config
from utils.filesystem import check_diff

class OptimizedDownloader:
    def __init__(self):
        self.settings = Config.get_optimal_settings()
        self.session = None

    async def __aenter__(self):
        # Configurações otimizadas para downloads paralelos
        timeout = aiohttp.ClientTimeout(total=300, connect=30)
        connector = aiohttp.TCPConnector(
            limit=self.settings['io_workers'] * 2,
            limit_per_host=4,
            ttl_dns_cache=300,
            use_dns_cache=True,
        )
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def fetch_file_list(self) -> List[str]:
        """Busca lista de arquivos de forma mais eficiente"""
        print("🔍 Buscando lista de arquivos...")

        try:
            response = requests.get(Config.BASE_URL, timeout=30)
            response.raise_for_status()

            soup = bs4.BeautifulSoup(response.content, 'lxml')
            links = soup.find_all('a', href=True)

            files = []
            for link in links:
                href = link['href']
                if href.endswith('.zip') and not href.startswith('/'):
                    files.append(href)

            print(f"✅ Encontrados {len(files)} arquivos ZIP")
            return files

        except Exception as e:
            print(f"❌ Erro ao buscar lista de arquivos: {e}")
            return []

    async def download_file(self, filename: str, progress_bar: tqdm = None) -> Dict:
        """Download assíncrono de um arquivo com retry e progress"""
        url = urljoin(Config.BASE_URL, filename)
        file_path = Path(Config.OUTPUT_FILES) / filename

        # Verifica se precisa baixar
        if not check_diff(url, str(file_path)):
            if progress_bar:
                progress_bar.update(1)
            return {"file": filename, "status": "skipped", "size": file_path.stat().st_size}

        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with self.session.get(url) as response:
                    if response.status == 200:
                        file_size = int(response.headers.get('content-length', 0))

                        async with aiofiles.open(file_path, 'wb') as f:
                            downloaded = 0
                            async for chunk in response.content.iter_chunked(8192):
                                await f.write(chunk)
                                downloaded += len(chunk)

                        if progress_bar:
                            progress_bar.update(1)

                        return {
                            "file": filename,
                            "status": "downloaded",
                            "size": downloaded,
                            "attempts": attempt + 1
                        }
                    else:
                        raise aiohttp.ClientResponseError(
                            request_info=response.request_info,
                            history=response.history,
                            status=response.status
                        )

            except Exception as e:
                if attempt == max_retries - 1:
                    if progress_bar:
                        progress_bar.update(1)
                    return {"file": filename, "status": "failed", "error": str(e)}

                await asyncio.sleep(2 ** attempt)  # Backoff exponencial

    async def download_all_files(self, files: List[str]) -> List[Dict]:
        """Download paralelo de todos os arquivos"""
        if not files:
            return []

        # Garante que o diretório existe
        Path(Config.OUTPUT_FILES).mkdir(parents=True, exist_ok=True)

        print(f"📥 Iniciando download de {len(files)} arquivos...")

        with tqdm(total=len(files), desc="Downloads", unit="arquivo") as pbar:
            # Limita concorrência para não sobrecarregar o servidor
            semaphore = asyncio.Semaphore(self.settings['io_workers'])

            async def download_with_semaphore(filename):
                async with semaphore:
                    return await self.download_file(filename, pbar)

            tasks = [download_with_semaphore(f) for f in files]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        # Processa resultados
        successful = sum(1 for r in results if isinstance(r, dict) and r.get("status") in ["downloaded", "skipped"])
        failed = len(results) - successful
        total_size = sum(r.get("size", 0) for r in results if isinstance(r, dict))

        print(f"✅ Downloads concluídos: {successful} sucessos, {failed} falhas")
        print(f"📊 Total baixado: {total_size / (1024**3):.2f} GB")

        return [r for r in results if isinstance(r, dict)]

def run_downloads():
    """Função principal para executar downloads"""
    async def main():
        async with OptimizedDownloader() as downloader:
            files = downloader.fetch_file_list()
            if files:
                results = await downloader.download_all_files(files)
                return results
            return []

    return asyncio.run(main())