"""
Módulo para procesar datos de Costos Variables.
"""

import logging
from typing import Dict, Optional
import pandas as pd
from datetime import datetime
from .base_processor import BaseProcessor

logger = logging.getLogger(__name__)

class CostosVariablesProcessor(BaseProcessor):
    """Procesador para datos de Costos Variables."""

    REQUIRED_COLUMNS = [
        "Central",
        "Emax",
        "cv",
        "cvC",
        "cvNC",
        "CMedMT",
        "CombC",
        "combU",
        "periodo"
    ]

    def __init__(self, periodo: str, data: Dict[str, pd.DataFrame]):
        """
        Inicializa el procesador de Costos Variables.

        Args:
            periodo (str): Periodo a procesar (YYYYMM)
            data (Dict[str, pd.DataFrame]): Diccionario con los datos extraídos
        """
        super().__init__(periodo, data)
        self.cv_df = self.data.get('costos_variables')

    def validate_data(self) -> bool:
        """
        Valida los datos de Costos Variables.

        Returns:
            bool: True si los datos son válidos
        """
        if self.cv_df is None:
            self.log_error(
                "DATA_NOT_FOUND",
                "No se encontraron datos de Costos Variables"
            )
            return False

        if not self.validate_dataframe(
            self.cv_df,
            self.REQUIRED_COLUMNS,
            "Costos Variables"
        ):
            return False

        # Validar tipos de datos
        if not self._validate_data_types():
            return False

        # Validar valores
        if not self._validate_values():
            return False

        return True

    def _validate_data_types(self) -> bool:
        """
        Valida los tipos de datos de las columnas.

        Returns:
            bool: True si los tipos son correctos
        """
        try:
            # Convertir columnas numéricas
            numeric_columns = ['Emax', 'cv', 'cvC', 'cvNC', 'CMedMT', 'CombC']
            for col in numeric_columns:
                self.cv_df[col] = pd.to_numeric(self.cv_df[col], errors='coerce')
                
                if self.cv_df[col].isna().any():
                    self.log_error(
                        "INVALID_DATA_TYPE",
                        f"Valores no numéricos encontrados en columna {col}"
                    )
                    return False

            return True

        except Exception as e:
            self.log_error(
                "DATA_TYPE_CONVERSION",
                f"Error en conversión de tipos de datos: {str(e)}"
            )
            return False

    def _validate_values(self) -> bool:
        """
        Valida los valores de Costos Variables.

        Returns:
            bool: True si los valores son válidos
        """
        # Validar que no hay valores negativos en columnas relevantes
        for col in ['Emax', 'cv', 'cvC', 'cvNC', 'CMedMT', 'CombC']:
            if (self.cv_df[col] < 0).any():
                self.log_error(
                    "INVALID_VALUE",
                    f"Se encontraron valores negativos en {col}"
                )
                return False

# Validar que el periodo corresponde
        if not all(per == self.periodo for per in self.cv_df['periodo'].astype(str)):
            self.log_error(
                "INVALID_PERIOD",
                f"Se encontraron registros que no corresponden al periodo {self.periodo}"
            )
            return False

        return True

    def process(self) -> bool:
        """
        Procesa los datos de Costos Variables.

        Returns:
            bool: True si el procesamiento fue exitoso
        """
        try:
            if not self.validate_data():
                return False

            # Procesar datos por tipo de combustible
            self.processed_data['por_combustible'] = self._process_by_combustible()
            
            # Procesar datos por central
            self.processed_data['por_central'] = self._process_by_central()
            
            # Calcular resumen general
            self.processed_data['resumen'] = self._calculate_resumen()

            return True

        except Exception as e:
            self.log_error("PROCESSING_ERROR", f"Error en procesamiento: {str(e)}")
            return False

    def _process_by_combustible(self) -> pd.DataFrame:
        """
        Procesa los costos variables agrupados por tipo de combustible.

        Returns:
            pd.DataFrame: DataFrame con los costos procesados por combustible
        """
        return self.cv_df.groupby('combU').agg({
            'cv': ['mean', 'min', 'max', 'std'],
            'Emax': 'sum',
            'CombC': 'mean'
        }).round(2)

    def _process_by_central(self) -> pd.DataFrame:
        """
        Procesa los costos variables agrupados por central.

        Returns:
            pd.DataFrame: DataFrame con los costos procesados por central
        """
        return self.cv_df.groupby('Central').agg({
            'Emax': 'first',
            'cv': 'first',
            'cvC': 'first',
            'cvNC': 'first',
            'CMedMT': 'first',
            'CombC': 'first',
            'combU': 'first'
        }).round(2)

    def _calculate_resumen(self) -> Dict[str, float]:
        """
        Calcula el resumen de costos variables.

        Returns:
            Dict[str, float]: Diccionario con los valores resumen
        """
        return {
            'cv_promedio_sistema': float(
                (self.cv_df['cv'] * self.cv_df['Emax']).sum() / 
                self.cv_df['Emax'].sum()
            ),
            'capacidad_total': float(self.cv_df['Emax'].sum()),
            'cantidad_centrales': int(self.cv_df['Central'].nunique()),
            'tipos_combustible': int(self.cv_df['combU'].nunique()),
            'cv_min': float(self.cv_df['cv'].min()),
            'cv_max': float(self.cv_df['cv'].max())
        }