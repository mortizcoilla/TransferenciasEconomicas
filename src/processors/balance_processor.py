"""
Módulo para procesar datos del Balance Valorizado.
"""

import logging
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from .base_processor import BaseProcessor

logger = logging.getLogger(__name__)

class BalanceProcessor(BaseProcessor):
    """Procesador para datos del Balance Valorizado."""

    REQUIRED_COLUMNS = [
        "nombre_barra",
        "propietario",
        "descripcion",
        "tipo1",
        "hora_mensual",
        "MedidaHoraria",
        "CMG_PESO_KWH",
        "VALORIZADO_PESOS"
    ]

    def __init__(self, periodo: str, data: Dict[str, pd.DataFrame]):
        """
        Inicializa el procesador de Balance Valorizado.

        Args:
            periodo (str): Periodo a procesar (YYYYMM)
            data (Dict[str, pd.DataFrame]): Diccionario con los datos extraídos
        """
        super().__init__(periodo, data)
        self.balance_df = self.data.get('balance_valorizado')

    def validate_data(self) -> bool:
        """
        Valida los datos del Balance Valorizado.

        Returns:
            bool: True si los datos son válidos
        """
        if self.balance_df is None:
            self.log_error(
                "DATA_NOT_FOUND",
                "No se encontraron datos de Balance Valorizado"
            )
            return False

        if not self.validate_dataframe(
            self.balance_df,
            self.REQUIRED_COLUMNS,
            "Balance Valorizado"
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
            self.balance_df['MedidaHoraria'] = pd.to_numeric(
                self.balance_df['MedidaHoraria'], errors='coerce'
            )
            self.balance_df['CMG_PESO_KWH'] = pd.to_numeric(
                self.balance_df['CMG_PESO_KWH'], errors='coerce'
            )
            self.balance_df['VALORIZADO_PESOS'] = pd.to_numeric(
                self.balance_df['VALORIZADO_PESOS'], errors='coerce'
            )

            # Verificar que no hay NaN después de la conversión
            numeric_columns = ['MedidaHoraria', 'CMG_PESO_KWH', 'VALORIZADO_PESOS']
            for col in numeric_columns:
                if self.balance_df[col].isna().any():
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
        Valida los valores del Balance Valorizado.

        Returns:
            bool: True si los valores son válidos
        """
        # Validar que no hay valores negativos en MedidaHoraria
        if (self.balance_df['MedidaHoraria'] < 0).any():
            self.log_error(
                "INVALID_VALUE",
                "Se encontraron valores negativos en MedidaHoraria"
            )
            return False

        # Validar tipos válidos
        valid_tipos = ['G', 'R', 'L', 'L_S', 'L_D', 'N']
        invalid_tipos = set(self.balance_df['tipo1']) - set(valid_tipos)
        if invalid_tipos:
            self.log_error(
                "INVALID_TYPE",
                f"Tipos inválidos encontrados: {invalid_tipos}"
            )
            return False

        return True

    def process(self) -> bool:
        """
        Procesa los datos del Balance Valorizado.

        Returns:
            bool: True si el procesamiento fue exitoso
        """
        try:
            if not self.validate_data():
                return False

            # Procesar por tipo
            self.processed_data['generacion'] = self._process_generacion()
            self.processed_data['retiros'] = self._process_retiros()
            self.processed_data['resumen'] = self._calculate_resumen()

            return True

        except Exception as e:
            self.log_error("PROCESSING_ERROR", f"Error en procesamiento: {str(e)}")
            return False

    def _process_generacion(self) -> pd.DataFrame:
        """
        Procesa los datos de generación.

        Returns:
            pd.DataFrame: DataFrame con los datos de generación procesados
        """
        generacion_df = self.balance_df[
            self.balance_df['tipo1'].isin(['G', 'N'])
        ].copy()

        # Agregar cálculos específicos para generación
        generacion_df['valor_energia'] = (
            generacion_df['MedidaHoraria'] * 
            generacion_df['CMG_PESO_KWH']
        )

        return generacion_df

    def _process_retiros(self) -> pd.DataFrame:
        """
        Procesa los datos de retiros.

        Returns:
            pd.DataFrame: DataFrame con los datos de retiros procesados
        """
        retiros_df = self.balance_df[
            self.balance_df['tipo1'].isin(['R', 'L', 'L_S', 'L_D'])
        ].copy()

        # Agregar cálculos específicos para retiros
        retiros_df['valor_energia'] = (
            retiros_df['MedidaHoraria'] * 
            retiros_df['CMG_PESO_KWH']
        )

        return retiros_df

    def _calculate_resumen(self) -> Dict[str, float]:
        """
        Calcula el resumen del balance.

        Returns:
            Dict[str, float]: Diccionario con los valores resumen
        """
        return {
            'total_generacion': float(self.processed_data['generacion']['MedidaHoraria'].sum()),
            'total_retiros': float(self.processed_data['retiros']['MedidaHoraria'].sum()),
            'balance_neto': float(
                self.processed_data['generacion']['MedidaHoraria'].sum() -
                self.processed_data['retiros']['MedidaHoraria'].sum()
            ),
            'valorizacion_total': float(self.balance_df['VALORIZADO_PESOS'].sum())
        }