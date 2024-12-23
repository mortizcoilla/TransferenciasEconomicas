"""
Organizador de archivos para Transferencias Económicas.
Implementa la lógica específica para copiar archivos desde unzipped a processed
siguiendo una estructura predefinida.
"""

import os
import shutil
import logging
from pathlib import Path
from typing import Dict, List, Optional
import re
from datetime import datetime

logger = logging.getLogger(__name__)


class FileOrganizer:
    """
    Organizador de archivos que implementa las reglas específicas de copiado
    para archivos de Transferencias Económicas.
    """

    def __init__(self, config: Dict, periodo: str):
        """
        Inicializa el organizador con rutas y patrones específicos.

        Args:
            config: Configuración del sistema
            periodo: Periodo en formato YYYYMM
        """
        self.periodo = periodo
        # Convertimos periodo YYYYMM a formato corto YMM (ej: 202401 -> 401)
        self.periodo_corto = periodo[2:]
        self.unzipped_path = Path(config["paths"]["unzipped"]) / periodo
        self.processed_path = Path(config["paths"]["processed"]) / periodo
        self.errors = {}

        # Definimos los patrones de archivos y sus destinos
        self.file_patterns = {
            "balance_valorizado": {
                "patterns": [
                    f"Balance_Valorizado_{self.periodo_corto}_Data_VALORIZADO_15min.csv",
                    f"Balance_Valorizado_{periodo}_Data_VALORIZADO_15min.csv",
                ],
                "destination": "valorizado",
                "required": True,
            },
            "balance": {
                "patterns": [
                    f"Balance_{self.periodo_corto}_BD01.xlsm",
                    f"Balance_{periodo}_BD01.xlsm",
                ],
                "destination": "balance",
                "required": True,
            },
            "cmg_mensual": {
                "patterns": [
                    f"cmg{self.periodo_corto}_def_15minutal.csv",
                    f"cmg{periodo}_def_15minutal.csv",
                ],
                "destination": "cmg",
                "required": True,
            },
            "sobrecostos": {
                "patterns": ["Detalle Sobrecostos *.xlsx"],
                "destination": "sobrecostos/diarios",
                "required": True,
            },
            "sscc": {
                "patterns": [
                    f"CUADROS_PAGO_SSCC_{self.periodo_corto}_def.xlsm",
                    f"1_CUADROS_PAGO_SSCC_{self.periodo_corto}_def.xlsm",
                ],
                "destination": "sscc",
                "required": True,
            },
            "precio_estabilizado": {
                "patterns": [
                    f"Precio_estabilizado_{self.periodo_corto}.xlsb",
                    f"Precio_estabilizado_{periodo}.xlsb",
                ],
                "destination": "precio_estabilizado",
                "required": True,
            },
            "cmg_diario": {
                "patterns": ["CMg_Real_*.csv"],
                "destination": "cmg/diario",
                "required": True,
            },
            "programa_operacion": {
                "patterns": ["Programa_Operacion_*.xlsx"],
                "destination": "operacion/diario",
                "required": True,
            },
            "contratos_medidas": {
                "patterns": [
                    f"Contratos_Generadores_{self.periodo_corto}_Fisicos_Medidas.xlsx",
                    f"Contratos_Generadores_{periodo}_Fisicos_Medidas.xlsx",
                ],
                "destination": "contratos/medidas",
                "required": True,
            },
            "contratos_fisicos": {
                "patterns": [
                    f"Contratos_Generadores_{self.periodo_corto}_Fisicos_Resultados.xlsx",
                    f"Contratos_Generadores_{periodo}_Fisicos_Resultados.xlsx",
                ],
                "destination": "contratos/fisicos",
                "required": True,
            },
            "contratos_financieros": {
                "patterns": [
                    f"Contratos_Financieros_{self.periodo_corto}_Resultados.xlsx",
                    f"Contratos_Generadores_{self.periodo_corto}_Financieros.xlsb",
                ],
                "destination": "contratos/financieros",
                "required": True,
            },
        }

    def _find_file(self, pattern: str, search_path: Path) -> List[Path]:
        """
        Busca archivos que coincidan con el patrón dado.

        Args:
            pattern: Patrón de búsqueda (admite comodines *)
            search_path: Ruta donde buscar

        Returns:
            List[Path]: Lista de archivos encontrados
        """
        # Convertimos el patrón a expresión regular
        pattern = pattern.replace("*", ".*")
        regex = re.compile(pattern, re.IGNORECASE)

        found_files = []
        # Buscamos en todos los subdirectorios
        for root, _, files in os.walk(search_path):
            for filename in files:
                if regex.match(filename):
                    found_files.append(Path(root) / filename)

        return found_files

    def _copy_file(self, source: Path, destination: Path) -> bool:
        """
        Copia un archivo asegurando que el directorio destino exista.

        Args:
            source: Ruta del archivo origen
            destination: Ruta del archivo destino

        Returns:
            bool: True si la copia fue exitosa
        """
        try:
            # Crear directorio destino si no existe
            destination.parent.mkdir(parents=True, exist_ok=True)

            # Copiar archivo manteniendo metadata
            shutil.copy2(source, destination)
            logger.info(f"Archivo copiado: {source.name} -> {destination}")
            return True
        except Exception as e:
            logger.error(f"Error copiando {source.name}: {str(e)}")
            return False

    def organize(self) -> bool:
        """
        Ejecuta el proceso de organización de archivos.

        Returns:
            bool: True si el proceso fue exitoso
        """
        logger.info(f"Iniciando organización de archivos para periodo {self.periodo}")
        success = True

        # Procesar cada tipo de archivo
        for file_type, config in self.file_patterns.items():
            found_any = False

            # Buscar archivos que coincidan con cualquiera de los patrones
            for pattern in config["patterns"]:
                found_files = self._find_file(pattern, self.unzipped_path)

                for source_file in found_files:
                    dest_path = (
                        self.processed_path / config["destination"] / source_file.name
                    )
                    if self._copy_file(source_file, dest_path):
                        found_any = True
                    else:
                        success = False

            # Registrar error si no se encontró un archivo requerido
            if not found_any and config["required"]:
                error_msg = f"No se encontró archivo requerido: {file_type}"
                self.errors[file_type] = error_msg
                logger.error(error_msg)
                success = False

        return success

    def get_errors(self) -> Dict[str, str]:
        """
        Retorna los errores encontrados durante el proceso.

        Returns:
            Dict[str, str]: Diccionario de errores
        """
        return self.errors
