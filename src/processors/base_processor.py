"""
Módulo que define la clase base para todos los procesadores.
"""

from abc import ABC, abstractmethod
import logging
from typing import Any, Dict, Optional
import pandas as pd

logger = logging.getLogger(__name__)

class BaseProcessor(ABC):
    """Clase base abstracta para todos los procesadores de datos."""

    def __init__(self, periodo: str, data: Dict[str, pd.DataFrame]):
        """
        Inicializa el procesador.

        Args:
            periodo (str): Periodo a procesar (YYYYMM)
            data (Dict[str, pd.DataFrame]): Diccionario con los datos extraídos
        """
        self.periodo = periodo
        self.data = data
        self.processed_data: Dict[str, Any] = {}
        self.errors: Dict[str, str] = {}

    @abstractmethod
    def validate_data(self) -> bool:
        """
        Valida los datos necesarios para el procesamiento.

        Returns:
            bool: True si los datos son válidos
        """
        pass

    @abstractmethod
    def process(self) -> bool:
        """
        Ejecuta el procesamiento de los datos.

        Returns:
            bool: True si el procesamiento fue exitoso
        """
        pass

    def get_processed_data(self) -> Dict[str, Any]:
        """
        Retorna los datos procesados.

        Returns:
            Dict[str, Any]: Diccionario con los datos procesados
        """
        return self.processed_data

    def get_errors(self) -> Dict[str, str]:
        """
        Retorna los errores encontrados durante el procesamiento.

        Returns:
            Dict[str, str]: Diccionario con los errores
        """
        return self.errors

    def log_error(self, error_type: str, message: str) -> None:
        """
        Registra un error en el log y en el diccionario de errores.

        Args:
            error_type (str): Tipo de error
            message (str): Mensaje de error
        """
        self.errors[error_type] = message
        logger.error(f"{error_type}: {message}")

    @staticmethod
    def validate_dataframe(
        df: pd.DataFrame,
        required_columns: list,
        name: str
    ) -> bool:
        """
        Valida que un DataFrame tenga las columnas requeridas.

        Args:
            df (pd.DataFrame): DataFrame a validar
            required_columns (list): Lista de columnas requeridas
            name (str): Nombre del DataFrame para los mensajes de error

        Returns:
            bool: True si el DataFrame es válido
        """
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            logger.error(
                f"Columnas faltantes en {name}: {sorted(missing_columns)}"
            )
            return False
        return True