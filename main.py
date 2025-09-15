import os
import argparse
import time
import psutil
from pathlib import Path

from pipeline.downloader import run_downloads
from pipeline.extractor import extract_files_optimized
from pipeline.loader import load_all_ultra_optimized
from pipeline.organizer import organize_files_optimized, group_files_by_type
from config import Config

def show_system_info():
    """Mostra informações do sistema para otimização"""
    print("\n💻 INFORMAÇÕES DO SISTEMA")
    print("=" * 50)

    # CPU
    cpu_count = psutil.cpu_count()
    cpu_freq = psutil.cpu_freq()
    print(f"🔧 CPU: {cpu_count} cores")
    if cpu_freq:
        print(f"🔧 Frequência: {cpu_freq.current:.0f} MHz")

    # Memória
    memory = psutil.virtual_memory()
    print(f"🧠 RAM Total: {memory.total / (1024**3):.1f} GB")
    print(f"🧠 RAM Disponível: {memory.available / (1024**3):.1f} GB")
    print(f"🧠 RAM em uso: {memory.percent:.1f}%")

    # Disco
    disk = psutil.disk_usage('.')
    print(f"💾 Disco Livre: {disk.free / (1024**3):.1f} GB")

    # Configurações otimizadas calculadas
    settings = Config.get_optimal_settings()
    print(f"\n⚙️  CONFIGURAÇÕES OTIMIZADAS CALCULADAS")
    print("=" * 50)
    print(f"📦 Chunk Size: {settings['chunk_size']:,} linhas")
    print(f"🔄 Workers I/O: {settings['io_workers']}")
    print(f"🔄 Workers CPU: {settings['cpu_workers']}")
    print(f"🔗 Pool Conexões DB: {settings['db_pool_size']}")
    print(f"🧠 Limite Memória: {settings['max_memory_mb']:,} MB")
    print()

def ensure_directories():
    """Garante que os diretórios necessários existem"""
    directories = [Config.OUTPUT_FILES, Config.EXTRACTED_FILES]

    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"📁 Diretório garantido: {directory}")

def monitor_resources():
    """Monitora uso de recursos durante o processamento"""
    process = psutil.Process()

    return {
        'cpu_percent': process.cpu_percent(),
        'memory_mb': process.memory_info().rss / (1024**2),
        'memory_percent': process.memory_percent(),
        'system_memory_percent': psutil.virtual_memory().percent
    }

def main(args):
    """Pipeline principal otimizado"""
    start_time = time.time()

    print("🚀 PIPELINE ETL CNPJ OTIMIZADO")
    print("=" * 50)

    # Mostra informações do sistema
    show_system_info()

    # 1️⃣ Garantir que os diretórios existem
    print("📁 Preparando diretórios...")
    ensure_directories()

    # 2️⃣ Download dos arquivos (se solicitado)
    if args.download:
        print("\n🔹 FASE 1: DOWNLOAD DE ARQUIVOS")
        print("=" * 50)
        download_start = time.time()

        try:
            results = run_downloads()
            download_time = time.time() - download_start

            successful = sum(1 for r in results if r.get("status") in ["downloaded", "skipped"])
            failed = len(results) - successful

            print(f"✅ Download concluído em {download_time:.2f}s")
            print(f"📊 Sucessos: {successful}, Falhas: {failed}")

        except Exception as e:
            print(f"❌ Erro no download: {e}")
            return

    # 3️⃣ Extração dos arquivos (se solicitado)
    if args.extract:
        print("\n🔹 FASE 2: EXTRAÇÃO DE ARQUIVOS")
        print("=" * 50)
        extract_start = time.time()

        try:
            results = extract_files_optimized()
            extract_time = time.time() - extract_start

            successful = sum(1 for r in results if r.get("status") == "extracted")
            print(f"✅ Extração concluída em {extract_time:.2f}s")
            print(f"📊 Arquivos extraídos: {successful}")

        except Exception as e:
            print(f"❌ Erro na extração: {e}")
            return

    # 4️⃣ Organização e análise dos arquivos
    print("\n🔹 FASE 3: ORGANIZAÇÃO DE ARQUIVOS")
    print("=" * 50)

    try:
        if args.smart_organize:
            # Organização inteligente com análise detalhada
            organized = organize_files_optimized()
            # Converte para formato compatível
            groups = {}
            for file_type, file_infos in organized.items():
                groups[file_type] = [info["name"] for info in file_infos]
        else:
            # Organização simples (compatibilidade)
            groups = group_files_by_type()

        total_files = sum(len(files) for files in groups.values())
        print(f"✅ Organização concluída: {total_files} arquivos classificados")

    except Exception as e:
        print(f"❌ Erro na organização: {e}")
        return

    # 5️⃣ Carregamento no banco de dados (se solicitado)
    if args.load:
        print("\n🔹 FASE 4: CARREGAMENTO NO BANCO")
        print("=" * 50)
        load_start = time.time()

        # Monitora recursos antes do carregamento
        resources_before = monitor_resources()
        print(f"🔧 Recursos antes: CPU {resources_before['cpu_percent']:.1f}%, "
              f"RAM {resources_before['memory_percent']:.1f}%")

        try:
            load_all_optimized(groups)
            load_time = time.time() - load_start

            # Monitora recursos após carregamento
            resources_after = monitor_resources()
            print(f"🔧 Recursos após: CPU {resources_after['cpu_percent']:.1f}%, "
                  f"RAM {resources_after['memory_percent']:.1f}%")

            print(f"✅ Carregamento concluído em {load_time:.2f}s")

        except Exception as e:
            print(f"❌ Erro no carregamento: {e}")
            return

    # 6️⃣ Relatório final
    total_time = time.time() - start_time
    print(f"\n🎉 PIPELINE CONCLUÍDO COM SUCESSO!")
    print("=" * 50)
    print(f"⏱️  Tempo total: {total_time:.2f}s ({total_time/60:.1f} minutos)")

    # Mostra uso final de recursos
    final_resources = monitor_resources()
    print(f"📊 Uso final de recursos:")
    print(f"   - CPU: {final_resources['cpu_percent']:.1f}%")
    print(f"   - RAM: {final_resources['memory_mb']:.1f} MB ({final_resources['memory_percent']:.1f}%)")
    print(f"   - RAM Sistema: {final_resources['system_memory_percent']:.1f}%")

def main_streaming(args):
    """Pipeline alternativo com processamento streaming (experimental)"""
    print("🌊 PIPELINE STREAMING (EXPERIMENTAL)")
    print("=" * 50)

    # TODO: Implementar processamento streaming direto dos ZIPs
    # sem extração completa, para economizar espaço em disco
    print("⚠️  Pipeline streaming ainda não implementado")
    print("💡 Use o pipeline normal por enquanto")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pipeline ETL otimizado para dados CNPJ da Receita Federal",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  
  # Pipeline completo
  python main_optimized.py --download --extract --load
  
  # Apenas download paralelo
  python main_optimized.py --download
  
  # Processar arquivos já baixados
  python main_optimized.py --extract --load
  
  # Organização inteligente com análise detalhada
  python main_optimized.py --smart-organize --load
  
  # Apenas carregamento (arquivos já organizados)
  python main_optimized.py --load
        """
    )

    # Argumentos principais
    parser.add_argument("--download", action="store_true",
                        help="Fazer download paralelo dos arquivos do servidor")
    parser.add_argument("--extract", action="store_true",
                        help="Extrair arquivos ZIP de forma paralela")
    parser.add_argument("--load", action="store_true",
                        help="Carregar arquivos no banco com otimizações")

    # Argumentos de otimização
    parser.add_argument("--smart-organize", action="store_true",
                        help="Usar organização inteligente com análise detalhada")
    parser.add_argument("--streaming", action="store_true",
                        help="Usar pipeline streaming (experimental)")

    # Argumentos de configuração
    parser.add_argument("--chunk-size", type=int,
                        help="Tamanho do chunk para processamento (sobrescreve automático)")
    parser.add_argument("--workers", type=int,
                        help="Número de workers paralelos (sobrescreve automático)")

    # Argumentos informativos
    parser.add_argument("--show-config", action="store_true",
                        help="Apenas mostra configurações otimizadas e sai")

    args = parser.parse_args()

    # Sobrescreve configurações se especificado
    if args.chunk_size:
        Config.CHUNK_SIZE = args.chunk_size
    if args.workers:
        Config.WORKERS = args.workers

    # Apenas mostra configurações
    if args.show_config:
        show_system_info()
        exit(0)

    # Executa pipeline apropriado
    if args.streaming:
        main_streaming(args)
    else:
        main(args)