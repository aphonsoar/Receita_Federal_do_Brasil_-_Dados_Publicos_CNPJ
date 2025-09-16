import os
import re
from pathlib import Path
from typing import Dict, List, Tuple
from collections import defaultdict
import time

from utils.config import Config

class SmartOrganizer:
    """Organizador inteligente que classifica e prioriza arquivos"""

    def __init__(self):
        self.patterns = {
            # Padrões mais específicos primeiro
            "empresa": [r"empresa", r"empre"],
            "estabelecimento": [r"estabele", r"estab"],
            "socios": [r"socio", r"soci"],
            "simples": [r"simples", r"simpl"],
            "cnae": [r"cnae"],
            "moti": [r"moti"],
            "munic": [r"munic"],
            "natju": [r"natju"],
            "pais": [r"pais"],
            "quals": [r"quals", r"qual"],
        }

        # Prioridade de processamento (maior número = maior prioridade)
        self.priority = {
            "cnae": 10,      # Tabelas de referência primeiro
            "moti": 10,
            "munic": 10,
            "natju": 10,
            "pais": 10,
            "quals": 10,
            "empresa": 8,    # Tabela principal menor
            "simples": 6,    # Tabela média
            "socios": 4,     # Tabela grande
            "estabelecimento": 2,  # Maior tabela por último
        }

    def classify_file(self, filename: str) -> Tuple[str, int]:
        """Classifica um arquivo e retorna tipo e prioridade"""
        filename_lower = filename.lower()

        for file_type, patterns in self.patterns.items():
            for pattern in patterns:
                if re.search(pattern, filename_lower):
                    return file_type, self.priority.get(file_type, 0)

        return "unknown", 0

    def get_file_info(self, file_path: Path) -> Dict:
        """Coleta informações detalhadas do arquivo"""
        try:
            stat = file_path.stat()
            return {
                "path": file_path,
                "name": file_path.name,
                "size_mb": stat.st_size / (1024 * 1024),
                "modified": stat.st_mtime,
                "exists": True
            }
        except Exception:
            return {
                "path": file_path,
                "name": file_path.name,
                "size_mb": 0,
                "modified": 0,
                "exists": False
            }

    def organize_files_smart(self) -> Dict[str, List[Dict]]:
        """Organiza arquivos com informações detalhadas e priorização"""
        extracted_path = Path(Config.EXTRACTED_FILES)

        if not extracted_path.exists():
            print(f"⚠️  Diretório {extracted_path} não encontrado")
            return {}

        # Coleta todos os arquivos
        all_files = list(extracted_path.glob("*"))

        if not all_files:
            print(f"⚠️  Nenhum arquivo encontrado em {extracted_path}")
            return {}

        # Classifica e organiza
        organized = defaultdict(list)
        unknown_files = []

        for file_path in all_files:
            if file_path.is_file():
                file_type, priority = self.classify_file(file_path.name)
                file_info = self.get_file_info(file_path)
                file_info.update({
                    "type": file_type,
                    "priority": priority
                })

                if file_type == "unknown":
                    unknown_files.append(file_info)
                else:
                    organized[file_type].append(file_info)

        # Ordena arquivos dentro de cada categoria por prioridade e tamanho
        for file_type in organized:
            organized[file_type].sort(
                key=lambda x: (-x["priority"], x["size_mb"])  # Maior prioridade, menor tamanho primeiro
            )

        # Relatório de organização
        self._print_organization_report(organized, unknown_files)

        return dict(organized)

    def _print_organization_report(self, organized: Dict, unknown_files: List):
        """Imprime relatório da organização"""
        print("\n📋 RELATÓRIO DE ORGANIZAÇÃO DE ARQUIVOS")
        print("=" * 50)

        total_files = sum(len(files) for files in organized.values()) + len(unknown_files)
        total_size = sum(
            sum(f["size_mb"] for f in files)
            for files in organized.values()
        ) + sum(f["size_mb"] for f in unknown_files)

        print(f"📊 Total: {total_files} arquivos, {total_size:.2f} MB")
        print()

        # Ordena tipos por prioridade
        sorted_types = sorted(
            organized.keys(),
            key=lambda x: self.priority.get(x, 0),
            reverse=True
        )

        for file_type in sorted_types:
            files = organized[file_type]
            type_size = sum(f["size_mb"] for f in files)
            priority = self.priority.get(file_type, 0)

            print(f"📁 {file_type.upper():<15} | "
                  f"Arquivos: {len(files):>3} | "
                  f"Tamanho: {type_size:>8.2f} MB | "
                  f"Prioridade: {priority}")

            # Mostra primeiros arquivos se houver muitos
            for i, file_info in enumerate(files[:3]):
                print(f"   └─ {file_info['name']:<30} ({file_info['size_mb']:.2f} MB)")

            if len(files) > 3:
                print(f"   └─ ... e mais {len(files) - 3} arquivos")
            print()

        if unknown_files:
            print(f"❓ ARQUIVOS NÃO CLASSIFICADOS: {len(unknown_files)}")
            for file_info in unknown_files[:5]:
                print(f"   └─ {file_info['name']}")
            if len(unknown_files) > 5:
                print(f"   └─ ... e mais {len(unknown_files) - 5} arquivos")

    def get_processing_order(self, organized: Dict) -> List[Tuple[str, List[Dict]]]:
        """Retorna ordem otimizada de processamento"""
        # Ordena por prioridade (tabelas de referência primeiro)
        return sorted(
            organized.items(),
            key=lambda x: self.priority.get(x[0], 0),
            reverse=True
        )

    def estimate_processing_time(self, organized: Dict) -> Dict[str, float]:
        """Estima tempo de processamento baseado no tamanho dos arquivos"""
        # Estimativas baseadas em benchmarks típicos (MB/segundo)
        processing_rates = {
            "cnae": 50,      # Tabelas pequenas processam rápido
            "moti": 50,
            "munic": 50,
            "natju": 50,
            "pais": 50,
            "quals": 50,
            "empresa": 20,   # Tabelas com mais colunas
            "simples": 25,
            "socios": 15,    # Muitos índices e relacionamentos
            "estabelecimento": 10,  # Tabela mais complexa
        }

        estimates = {}
        for file_type, files in organized.items():
            total_size = sum(f["size_mb"] for f in files)
            rate = processing_rates.get(file_type, 15)
            estimates[file_type] = total_size / rate  # segundos

        return estimates

def group_files_by_type(extracted_path: str = None) -> Dict[str, List[str]]:
    """Função de compatibilidade com o código existente"""
    if extracted_path:
        Config.EXTRACTED_FILES = extracted_path

    organizer = SmartOrganizer()
    organized = organizer.organize_files_smart()

    # Converte para formato esperado pelo código existente
    result = {}
    for file_type, file_infos in organized.items():
        result[file_type] = [info["name"] for info in file_infos]

    return result

def organize_files_optimized() -> Dict[str, List[Dict]]:
    """Função principal para organização otimizada"""
    organizer = SmartOrganizer()
    organized = organizer.organize_files_smart()

    # Mostra estimativa de tempo
    estimates = organizer.estimate_processing_time(organized)
    total_estimate = sum(estimates.values())

    print(f"\n⏱️  ESTIMATIVA DE TEMPO DE PROCESSAMENTO")
    print("=" * 50)
    for file_type, estimate in estimates.items():
        print(f"{file_type:<15}: {estimate/60:.1f} minutos")
    print(f"{'TOTAL':<15}: {total_estimate/60:.1f} minutos")
    print()

    return organized