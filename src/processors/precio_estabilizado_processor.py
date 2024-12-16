"""
Módulo para procesar datos de Precio Estabilizado.
"""

import logging
from typing import Dict, List, Optional
import pandas as pd
from .base_processor import BaseProcessor

logger = logging.getLogger(__name__)

class PrecioEstabilizadoProcessor(BaseProcessor):
    """Procesador para datos de Precio Estabilizado."""

    REQUIRED_COLUMNS_SUR = [
        "Energía_Sobre_9MWh",
        "Energía_Bajo_9MWh",
        "Costo_Marginal",
        "Precio_Nudo",
        "Precio_Estabilizado"
    ]

    REQUIRED_COLUMNS_NORTE = [
        "Energía_Sobre_9MWh",
        "Energía_Bajo_9MWh",
        "Costo_Marginal",
        "Precio_Nudo",
        "Precio_Estabilizado"
    ]

    def __init__(self, periodo: str, data: Dict[str, pd.DataFrame]):
        """
        Inicializa el procesador de Precio Estabilizado.

        Args:
            periodo (str): Periodo a procesar (YYYYMM)
            data (Dict[str, pd.DataFrame]): Diccionario con los datos extraídos
        """
        super().__init__(periodo, data)
        self.sur_df = self.data.get('precio_estabilizado_sur')
        self.norte_df = self.data.get('precio_estabilizado_norte')

    def validate_data(self) -> bool:
        """
        Valida los datos de Precio Estabilizado.

        Returns:
            bool: True si los datos son válidos
        """
        if self.sur_df is None or self.norte_df is None:
            self.log_error(
                "DATA_NOT_FOUND",
                "No se encontraron datos de Precio Estabilizado para alguna zona"
            )
            return False

        # Validar estructura de datos Sur
        if not self.validate_dataframe(
            self.sur_df,
            self.REQUIRED_COLUMNS_SUR,
            "Precio Estabilizado Sur"
        ):
            return False

        # Validar estructura de datos Norte
        if not self.validate_dataframe(
            self.norte_df,
            self.REQUIRED_COLUMNS_NORTE,
            "Precio Estabilizado Norte"
        ):
            return False

        # Validar tipos de datos
        if not self._validate_data_types():
            return False

        return True

    def _validate_data_types(self) -> bool:
        """
        Valida los tipos de datos de las columnas.

        Returns:
            bool: True si los tipos son correctos
        """
        numeric_columns = [
            "Energía_Sobre_9MWh",
            "Energía_Bajo_9MWh",
            "Costo_Marginal",
            "Precio_Nudo",
            "Precio_Estabilizado"
        ]

        try:
            # Validar datos Sur
            for col in numeric_columns:
                self.sur_df[col] = pd.to_numeric(self.sur_df[col], errors='coerce')
                if self.sur_df[col].isna().any():
                    self.log_error(
                        "INVALID_DATA_TYPE",
                        f"Valores no numéricos en columna {col} (Sur)"
                    )
                    return False

            # Validar datos Norte
            for col in numeric_columns:
                self.norte_df[col] = pd.to_numeric(self.norte_df[col], errors='coerce')
                if self.norte_df[col].isna().any():
                    self.log_error(
                        "INVALID_DATA_TYPE",
                        f"Valores no numéricos en columna {col} (Norte)"
                    )
                    return False

            return True

        except Exception as e:
            self.log_error(
                "DATA_TYPE_CONVERSION",
                f"Error en conversión de tipos de datos: {str(e)}"
            )
            return False

    def process(self) -> bool:
        """
        Procesa los datos de Precio Estabilizado.

        Returns:
            bool: True si el procesamiento fue exitoso
        """
        try:
            if not self.validate_data():
                return False

            # Procesar datos por zona
            self.processed_data['sur'] = self._process_zona(self.sur_df, 'Sur')
            self.processed_data['norte'] = self._process_zona(self.norte_df, 'Norte')
            self.processed_data['resumen'] = self._calculate_resumen()

            return True

        except Exception as e:
            self.log_error("PROCESSING_ERROR", f"Error en procesamiento: {str(e)}")
            return False

    def _process_zona(self, df: pd.DataFrame, zona: str) -> Dict[str, pd.DataFrame]:
        """
        Procesa los datos de una zona específica.

        Args:
            df (pd.DataFrame): DataFrame con los datos de la zona
            zona (str): Nombre de la zona ('Sur' o 'Norte')

        Returns:
            Dict[str, pd.DataFrame]: Diccionario con los datos procesados
        """
        return {
            'energia_total': float(
                df['Energía_Sobre_9MWh'].sum() + df['Energía_Bajo_9MWh'].sum()
            ),
            'precio_promedio': float(df['Precio_Estabilizado'].mean()),
            'diferencial_cmg': float(
                (df['Precio_Estabilizado'] - df['Costo_Marginal']).mean()
            ),
            'diferencial_pnudo': float(
                (df['Precio_Estabilizado'] - df['Precio_Nudo']).mean()
            ),
            'detalle_horario': df.copy()
        }

    def _calculate_resumen(self) -> Dict[str, float]:
        """
        Calcula el resumen general de precios estabilizados.

        Returns:
            Dict[str, float]: Diccionario con los valores resumen
        """
        return {
            'energia_total_sistema': float(
                self.processed_data['sur']['energia_total'] +
                self.processed_data['norte']['energia_total']
            ),
            'precio_promedio_sistema': float(
                (self.processed_data['sur']['precio_promedio'] +
                 self.processed_data['norte']['precio_promedio']) / 2
            ),
            'diferencial_promedio_cmg': float(
                (self.processed_data['sur']['diferencial_cmg'] +
                 self.processed_data['norte']['diferencial_cmg']) / 2
            ),
            'diferencial_promedio_pnudo': float(
                (self.processed_data['sur']['diferencial_pnudo'] +
                 self.processed_data['norte']['diferencial_pnudo']) / 2
            )
        }