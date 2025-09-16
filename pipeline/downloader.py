import re
import os
import requests
import asyncio
import aiohttp
import aiofiles
from urllib.parse import urljoin
from pathlib import Path
import bs4
from tqdm import tqdm

from utils.config import Config
from utils.filesystem import check_diff

class OptimizedDownloader:
    def __init__(self):
        self.settings = Config.get_optimal_settings()
        self.session = None
        self.failed_downloads = []  # Lista para rastrear falhas

    async def __aenter__(self):
        # Configurações otimizadas para downloads paralelos
        timeout = aiohttp.ClientTimeout(total=600, connect=60)
        connector = aiohttp.TCPConnector(
            limit=self.settings['io_workers'] * 2,
            limit_per_host=self.settings['io_workers'],  # Aumentado para paralelismo real
            ttl_dns_cache=300,
            use_dns_cache=True,
        )
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def fetch_file_list(self) -> list:
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

    async def download_file(self, filename: str, progress_bar: tqdm = None) -> dict:
        """Download assíncrono de um arquivo com retry melhorado e progress"""
        url = urljoin(Config.BASE_URL, filename)
        file_path = Path(Config.OUTPUT_FILES) / filename

        try:
            if not check_diff(url, str(file_path)):
                if progress_bar:
                    progress_bar.update(1)
                    progress_bar.set_postfix({'arquivo': filename[:30], 'status': 'já existe'})
                return {"file": filename, "status": "skipped", "size": file_path.stat().st_size}
        except Exception as e:
            print(f"⚠️  Erro ao verificar {filename}: {e}")

        max_retries = 5
        backoff_delays = [2, 5, 10, 20, 30]
        
        for attempt in range(max_retries):
            try:
                if progress_bar:
                    progress_bar.set_postfix({
                        'arquivo': filename[:25], 
                        'tentativa': f'{attempt + 1}/{max_retries}'
                    })

                async with self.session.get(url) as response:
                    if response.status == 200:
                        file_size = int(response.headers.get('content-length', 0))
                        
                        async with aiofiles.open(file_path, 'wb') as f:
                            downloaded = 0
                            chunk_size = 256 * 1024  # 256 KB
                            
                            async for chunk in response.content.iter_chunked(chunk_size):
                                await f.write(chunk)
                                downloaded += len(chunk)
                                
                                # Atualiza progress a cada chunk
                                if progress_bar and file_size > 0 and downloaded % chunk_size == 0:
                                    percent = (downloaded / file_size) * 100
                                    progress_bar.set_postfix({
                                        'arquivo': filename[:20],
                                        'progresso': f'{percent:.1f}%'
                                    })

                        if progress_bar:
                            progress_bar.update(1)
                            progress_bar.set_postfix({
                                'arquivo': filename[:25], 
                                'status': 'concluído'
                            })

                        return {
                            "file": filename,
                            "status": "downloaded",
                            "size": downloaded,
                            "attempts": attempt + 1,
                            "url": url
                        }
                    else:
                        error_msg = f"HTTP {response.status}"
                        if attempt == max_retries - 1:
                            self.failed_downloads.append({
                                'file': filename,
                                'url': url,
                                'error': error_msg,
                                'attempts': max_retries
                            })
                            if progress_bar:
                                progress_bar.update(1)
                                progress_bar.set_postfix({
                                    'arquivo': filename[:25], 
                                    'status': f'FALHA {response.status}'
                                })
                            return {
                                "file": filename, 
                                "status": "failed", 
                                "error": error_msg,
                                "attempts": max_retries,
                                "url": url
                            }
                        else:
                            print(f"⚠️  {filename}: HTTP {response.status}, tentativa {attempt + 1}/{max_retries}")

            except asyncio.TimeoutError:
                error_msg = "Timeout"
                if attempt == max_retries - 1:
                    self.failed_downloads.append({
                        'file': filename,
                        'url': url,
                        'error': error_msg,
                        'attempts': max_retries
                    })
                    if progress_bar:
                        progress_bar.update(1)
                        progress_bar.set_postfix({
                            'arquivo': filename[:25], 
                            'status': 'TIMEOUT'
                        })
                    return {
                        "file": filename, 
                        "status": "failed", 
                        "error": error_msg,
                        "attempts": max_retries,
                        "url": url
                    }
                else:
                    print(f"⚠️  {filename}: Timeout, tentativa {attempt + 1}/{max_retries}")

            except Exception as e:
                error_msg = str(e)
                if attempt == max_retries - 1:
                    self.failed_downloads.append({
                        'file': filename,
                        'url': url,
                        'error': error_msg,
                        'attempts': max_retries
                    })
                    if progress_bar:
                        progress_bar.update(1)
                        progress_bar.set_postfix({
                            'arquivo': filename[:25], 
                            'status': 'ERRO'
                        })
                    return {
                        "file": filename, 
                        "status": "failed", 
                        "error": error_msg,
                        "attempts": max_retries,
                        "url": url
                    }
                else:
                    print(f"⚠️  {filename}: {str(e)[:50]}, tentativa {attempt + 1}/{max_retries}")

            if attempt < max_retries - 1:
                delay = backoff_delays[attempt]
                await asyncio.sleep(delay)

    async def download_all_files(self, files: list) -> list:
        """Download paralelo de todos os arquivos"""
        if not files:
            return []

        Path(Config.OUTPUT_FILES).mkdir(parents=True, exist_ok=True)

        print(f"📥 Iniciando download de {len(files)} arquivos...")
        print(f"🔄 Usando {self.settings['io_workers']} workers paralelos")

        with tqdm(total=len(files), desc="Downloads", unit="arquivo", 
                  bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}') as pbar:
            
            tasks = [self.download_file(f, pbar) for f in files]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        valid_results = []
        exceptions = []
        
        for result in results:
            if isinstance(result, Exception):
                exceptions.append(result)
                print(f"❌ Exceção durante download: {result}")
            elif isinstance(result, dict):
                valid_results.append(result)

        successful = sum(1 for r in valid_results if r.get("status") in ["downloaded", "skipped"])
        failed = len(valid_results) - successful + len(exceptions)
        total_size = sum(r.get("size", 0) for r in valid_results if r.get("status") in ["downloaded", "skipped"])

        print(f"✅ Downloads concluídos: {successful} sucessos, {failed} falhas")
        print(f"📊 Total baixado: {total_size / (1024**3):.2f} GB")

        if self.failed_downloads:
            print(f"\n❌ ARQUIVOS QUE FALHARAM ({len(self.failed_downloads)}):")
            print("=" * 60)
            for failure in self.failed_downloads:
                print(f"📁 Arquivo: {failure['file']}")
                print(f"   🔗 URL: {failure['url']}")
                print(f"   ❌ Erro: {failure['error']}")
                print(f"   🔄 Tentativas: {failure['attempts']}")
                print()
            
            print("💡 Para tentar baixar apenas os arquivos que falharam:")
            print("   python retry_failed_downloads.py")

        return valid_results

    def save_failed_list(self):
        """Salva lista de arquivos que falharam para retry posterior"""
        if self.failed_downloads:
            failed_file = Path("failed_downloads.txt")
            with open(failed_file, 'w', encoding='utf-8') as f:
                f.write("# Arquivos que falharam no download\n")
                f.write("# Execute: python retry_failed_downloads.py\n\n")
                for failure in self.failed_downloads:
                    f.write(f"{failure['file']}\n")
            
            print(f"💾 Lista de falhas salva em: {failed_file}")

def run_downloads():
    """Função principal para executar downloads"""
    async def main():
        async with OptimizedDownloader() as downloader:
            files = downloader.fetch_file_list()
            if files:
                results = await downloader.download_all_files(files)
                downloader.save_failed_list()
                return results
            return []

    return asyncio.run(main())
