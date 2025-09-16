import os
import argparse
import time
import psutil
from pathlib import Path

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
    try:
        from utils.config import Config
        settings = Config.get_optimal_settings()
        print(f"\n⚙️  CONFIGURAÇÕES OTIMIZADAS CALCULADAS")
        print("=" * 50)
        print(f"📦 Chunk Size: {settings['chunk_size']:,} linhas")
        print(f"🔄 Workers I/O: {settings['io_workers']}")
        print(f"🔄 Workers CPU: {settings['cpu_workers']}")
        print(f"🔗 Pool Conexões DB: {settings['db_pool_size']}")
        print(f"🧠 Limite Memória: {settings['max_memory_mb']:,} MB")
    except ImportError as e:
        print(f"⚠️  Erro ao carregar configurações: {e}")
    print()

def ensure_directories():
    """Garante que os diretórios necessários existem"""
    try:
        from utils.config import Config
        directories = [Config.OUTPUT_FILES, Config.EXTRACTED_FILES]
    except ImportError:
        directories = ["./downloads", "./extracted"]

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

def run_downloads():
    """Função de download - importa apenas quando necessário"""
    try:
        from pipeline.downloader import run_downloads as actual_run_downloads
        return actual_run_downloads()
    except ImportError as e:
        print(f"❌ Erro ao importar módulo de download: {e}")
        print("💡 Verifique se o arquivo pipeline/downloader.py existe")
        return []

def extract_files_optimized():
    """Função de extração - importa apenas quando necessário"""
    try:
        from pipeline.extractor import extract_files_optimized as actual_extract
        return actual_extract()
    except ImportError as e:
        print(f"❌ Erro ao importar módulo de extração: {e}")
        print("💡 Verifique se o arquivo pipeline/extractor.py existe")
        return []

def organize_files_optimized():
    """Função de organização - importa apenas quando necessário"""
    try:
        from pipeline.organizer import organize_files_optimized as actual_organize
        return actual_organize()
    except ImportError as e:
        print(f"❌ Erro ao importar módulo de organização: {e}")
        print("💡 Verifique se o arquivo pipeline/organizer.py existe")
        return {}

def group_files_by_type():
    """Função de organização simples - importa apenas quando necessário"""
    try:
        from pipeline.organizer import group_files_by_type as actual_group
        return actual_group()
    except ImportError as e:
        print(f"❌ Erro ao importar módulo de organização: {e}")
        print("💡 Verifique se o arquivo pipeline/organizer.py existe")
        return {}

def load_all_optimized(groups):
    """Função de carregamento - importa apenas quando necessário"""
    try:
        from pipeline.loader import load_all_ultra_optimized
        return load_all_ultra_optimized(groups)
    except ImportError as e:
        print(f"❌ Erro ao importar módulo de carregamento: {e}")
        print("💡 Verifique se o arquivo pipeline/loader.py existe")
        return False

def main(args):
    """Pipeline principal otimizado"""
    start_time = time.time()

    print("🚀 PIPELINE ETL CNPJ OTIMIZADO")
    print("=" * 50)

    # Mostra informações do sistema
    show_system_info()

    # Se nenhum argumento específico foi passado, executa pipeline completo
    if not any([args.download, args.extract, args.load, args.smart_organize, args.show_config]):
        print("💡 Executando pipeline completo automaticamente...")
        args.download = True
        args.extract = True
        args.load = True
        args.smart_organize = True

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

            if results:
                successful = sum(1 for r in results if r.get("status") in ["downloaded", "skipped"])
                failed = len(results) - successful
                print(f"✅ Download concluído em {download_time:.2f}s")
                print(f"📊 Sucessos: {successful}, Falhas: {failed}")
            else:
                print("⚠️  Nenhum resultado de download retornado")

        except Exception as e:
            print(f"❌ Erro no download: {e}")
            if not args.extract and not args.load:
                return

    # 3️⃣ Extração dos arquivos (se solicitado)
    if args.extract:
        print("\n🔹 FASE 2: EXTRAÇÃO DE ARQUIVOS")
        print("=" * 50)
        extract_start = time.time()

        try:
            results = extract_files_optimized()
            extract_time = time.time() - extract_start

            if results:
                successful = sum(1 for r in results if r.get("status") == "extracted")
                print(f"✅ Extração concluída em {extract_time:.2f}s")
                print(f"📊 Arquivos extraídos: {successful}")
            else:
                print("⚠️  Nenhum resultado de extração retornado")

        except Exception as e:
            print(f"❌ Erro na extração: {e}")
            if not args.load:
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

        # Se não há arquivos para processar, não continua para o carregamento
        if total_files == 0:
            print("⚠️  Nenhum arquivo encontrado para processamento")
            print("💡 Execute primeiro as etapas de download e extração:")
            print("     python main.py --download --extract")
            
    except Exception as e:
        print(f"❌ Erro na organização: {e}")
        groups = {}

    # 5️⃣ Carregamento no banco de dados (se solicitado)
    if args.load and groups and sum(len(files) for files in groups.values()) > 0:
        print("\n🔹 FASE 4: CARREGAMENTO NO BANCO")
        print("=" * 50)
        load_start = time.time()

        # Monitora recursos antes do carregamento
        resources_before = monitor_resources()
        print(f"🔧 Recursos antes: CPU {resources_before['cpu_percent']:.1f}%, "
              f"RAM {resources_before['memory_percent']:.1f}%")

        try:
            # Primeiro testa a conexão
            print("🔗 Testando conexão com banco de dados...")
            from utils.db import test_connection, ensure_database_exists
            
            if not ensure_database_exists():
                print("❌ Não foi possível verificar/criar o banco de dados")
                return
            
            if not test_connection():
                print("❌ Não foi possível conectar ao banco de dados")
                return
            
            print("✅ Conexão com banco OK! Iniciando carregamento...")
            
            success = load_all_optimized(groups)
            load_time = time.time() - load_start

            # Monitora recursos após carregamento
            resources_after = monitor_resources()
            print(f"🔧 Recursos após: CPU {resources_after['cpu_percent']:.1f}%, "
                  f"RAM {resources_after['memory_percent']:.1f}%")

            if success is not False:
                print(f"✅ Carregamento concluído em {load_time:.2f}s")
                
                # Mostra estatísticas finais do banco
                try:
                    from utils.db import get_engine
                    from sqlalchemy import text
                    engine = get_engine()
                    with engine.connect() as conn:
                        print(f"\n📊 ESTATÍSTICAS DO BANCO:")
                        tables = ['empresa', 'estabelecimento', 'socios', 'simples', 'cnae', 'moti', 'munic', 'natju', 'pais', 'quals']
                        for table in tables:
                            try:
                                result = conn.execute(text(f"SELECT COUNT(*) FROM {table};")).fetchone()
                                if result and result[0] > 0:
                                    print(f"   📋 {table}: {result[0]:,} registros")
                            except:
                                pass
                except Exception as e:
                    print(f"⚠️  Erro ao obter estatísticas: {e}")
            else:
                print("⚠️  Carregamento apresentou problemas")

        except Exception as e:
            print(f"❌ Erro no carregamento: {e}")
            print("💡 Possíveis soluções:")
            print("   - Verifique se PostgreSQL está rodando")
            print("   - Confirme as configurações no arquivo .env")
            print("   - Execute: python test_config.py")
            return

    elif args.load:
        print("\n⚠️  CARREGAMENTO PULADO - Nenhum arquivo para processar")
        print("💡 Execute primeiro: python main.py --download --extract")

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
    print("⚠️  Pipeline streaming ainda não implementado")
    print("💡 Use o pipeline normal por enquanto")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pipeline ETL otimizado para dados CNPJ da Receita Federal",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  
  # Pipeline completo (padrão se nenhum argumento for passado)
  python main.py
  
  # Pipeline completo explícito
  python main.py --download --extract --load
  
  # Apenas download paralelo
  python main.py --download
  
  # Processar arquivos já baixados
  python main.py --extract --load
  
  # Organização inteligente com análise detalhada
  python main.py --smart-organize --load
  
  # Apenas carregamento (arquivos já organizados)
  python main.py --load
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
    try:
        from utils.config import Config
        if args.chunk_size:
            Config.CHUNK_SIZE = args.chunk_size
        if args.workers:
            Config.WORKERS = args.workers
    except ImportError:
        pass

    # Apenas mostra configurações
    if args.show_config:
        show_system_info()
        exit(0)

    # Executa pipeline apropriado
    if args.streaming:
        main_streaming(args)
    else:
        main(args)