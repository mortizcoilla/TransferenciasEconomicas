"""
Módulo para la extracción y validación inicial de datos.
"""

import logging
from typing import Dict, Optional
import pandas as pd
import openpyxl

from src.utils.config_loader import ConfigLoader
from .file_registry import FileRegistry, FileRegistryError, FileNotFoundError

logger = logging.getLogger(__name__)


class DataExtractor:

    def __init__(self, periodo: str):
        """
        Inicializa el extractor de datos.

        Args:
            periodo (str): Periodo a procesar (formato: YYYYMM)
        """
        self.periodo = periodo
        self.config = ConfigLoader()
        self.file_registry = FileRegistry(self.config.get_config())
        self.extracted_data: Dict[str, pd.DataFrame] = {}

    def validate_required_files(self) -> bool:
        """
        Valida que existan todos los archivos requeridos usando FileRegistry.

        Returns:
            bool: True si todos los archivos requeridos están presentes
        """
        try:
            locations = self.file_registry.locate_all_files(self.periodo)
            all_valid = True

            for file_type, location in locations.items():
                if location is None:
                    logger.error(f"Archivo requerido no encontrado: {file_type}")
                    all_valid = False
                else:
                    logger.info(f"Archivo encontrado: {file_type} - {location}")

            return all_valid

        except FileRegistryError as e:
            logger.error(f"Error validando archivos: {str(e)}")
            return False

    def extract_data(self, file_type: str) -> Optional[pd.DataFrame]:
        try:
            location = self.file_registry.locate_file(file_type, self.periodo)
            if not location:
                logger.error(f"No se encontró la ubicación para {file_type}")
                return None

            definition = self.file_registry.definitions[file_type]
            logger.info(f"Leyendo archivo: {location}")

            if definition.format in ["xlsx", "xlsb", "xls", "xlsm"]:
                # Lectura simple de Excel sin validación de hojas
                return pd.read_excel(location)
            else:
                # Lectura simple de archivos CSV/TSV
                return pd.read_csv(location, encoding=definition.encoding)

        except Exception as e:
            logger.error(f"Error extrayendo {file_type}: {str(e)}")
            return None

    def extract_all(self) -> bool:
        """
        Extrae todos los archivos configurados.

        Returns:
            bool: True si todos los archivos fueron extraídos exitosamente
        """
        if not self.validate_required_files():
            return False

        success = True
        for file_type in self.file_registry.definitions:
            logger.info(f"Extrayendo {file_type}...")
            df = self.extract_data(file_type)

            if df is not None:
                self.extracted_data[file_type] = df
                logger.info(f"Extracción exitosa de {file_type}")
            else:
                success = False
                logger.error(f"Error extrayendo {file_type}")

        return success

    def get_extracted_data(self) -> Dict[str, pd.DataFrame]:
        """
        Obtiene los datos extraídos.

        Returns:
            Dict[str, pd.DataFrame]: Diccionario con los datos extraídos
        """
        return self.extracted_data
