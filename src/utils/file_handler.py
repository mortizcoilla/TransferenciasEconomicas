"""
Módulo para el manejo básico de archivos.
"""

import os
import glob
import logging
from typing import List, Optional
import pandas as pd

logger = logging.getLogger(__name__)

class FileHandler:
    """Clase base para el manejo de archivos."""

    @staticmethod
    def find_file(directory: str, pattern: str) -> Optional[str]:
        """
        Busca un archivo que coincida con el patrón en el directorio.

        Args:
            directory (str): Directorio donde buscar
            pattern (str): Patrón de búsqueda

        Returns:
            Optional[str]: Ruta del archivo encontrado o None si no se encuentra
        """
        try:
            matches = glob.glob(os.path.join(directory, pattern))
            if matches:
                # Retornar el archivo más reciente si hay varios
                return max(matches, key=os.path.getctime)
            return None
        except Exception as e:
            logger.error(f"Error buscando archivo {pattern}: {str(e)}")
            return None

    @staticmethod
    def read_csv(file_path: str, encoding: str = 'utf-8', **kwargs) -> Optional[pd.DataFrame]:
        """
        Lee un archivo CSV.

        Args:
            file_path (str): Ruta del archivo
            encoding (str): Codificación del archivo
            **kwargs: Argumentos adicionales para pd.read_csv

        Returns:
            Optional[pd.DataFrame]: DataFrame con los datos o None si hay error
        """
        try:
            return pd.read_csv(file_path, encoding=encoding, **kwargs)
        except Exception as e:
            logger.error(f"Error leyendo CSV {file_path}: {str(e)}")
            return None

    @staticmethod
    def read_excel(file_path: str, sheet_name: Optional[str] = None, **kwargs) -> Optional[pd.DataFrame]:
        """
        Lee un archivo Excel.

        Args:
            file_path (str): Ruta del archivo
            sheet_name (str, optional): Nombre de la hoja a leer
            **kwargs: Argumentos adicionales para pd.read_excel

        Returns:
            Optional[pd.DataFrame]: DataFrame con los datos o None si hay error
        """
        try:
            return pd.read_excel(file_path, sheet_name=sheet_name, **kwargs)
        except Exception as e:
            logger.error(f"Error leyendo Excel {file_path}: {str(e)}")
            return None

    @staticmethod
    def validate_columns(df: pd.DataFrame, expected_columns: List[str]) -> bool:
        """
        Valida que el DataFrame contenga las columnas esperadas.

        Args:
            df (pd.DataFrame): DataFrame a validar
            expected_columns (List[str]): Lista de columnas esperadas

        Returns:
            bool: True si todas las columnas están presentes
        """
        missing_columns = set(expected_columns) - set(df.columns)
        if missing_columns:
            logger.error(f"Columnas faltantes: {missing_columns}")
            return False
        return True

    @staticmethod
    def validate_file_exists(file_path: str) -> bool:
        """
        Valida que el archivo exista.

        Args:
            file_path (str): Ruta del archivo

        Returns:
            bool: True si el archivo existe
        """
        exists = os.path.isfile(file_path)
        if not exists:
            logger.error(f"Archivo no encontrado: {file_path}")
        return exists