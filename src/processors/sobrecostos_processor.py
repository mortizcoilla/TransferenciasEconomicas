"""
Módulo para procesar datos de Sobrecostos.
"""

import logging
from typing import Dict, Optional
import pandas as pd
from datetime import datetime
from .base_processor import BaseProcessor

logger = logging.getLogger(__name__)

class SobrecostosProcessor(BaseProcessor):
    """Procesador para datos de Sobrecostos."""

    REQUIRED_COLUMNS = [
        "fecha",
        "hora",
        "central",
        "tipo",
        "Sobrecosto_CLP"
    ]

    def __init__(self, periodo: str, data: Dict[str, pd.DataFrame]):
        """
        Inicializa el procesador de Sobrecostos.

        Args:
            periodo (str): Periodo a procesar (YYYYMM)
            data (Dict[str, pd.DataFrame]): Diccionario con los datos extraídos
        """
        super().__init__(periodo, data)
        self.sobrecostos_df = self.data.get('sobrecostos')

    def validate_data(self) -> bool:
        """
        Valida los datos de Sobrecostos.

        Returns:
            bool: True si los datos son válidos
        """
        if self.sobrecostos_df is None:
            self.log_error(
                "DATA_NOT_FOUND",
                "No se encontraron datos de Sobrecostos"
            )
            return False

        if not self.validate_dataframe(
            self.sobrecostos_df,
            self.REQUIRED_COLUMNS,
            "Sobrecostos"
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
            # Convertir y validar fechas
            self.sobrecostos_df['fecha'] = pd.to_datetime(
                self.sobrecostos_df['fecha']
            )

            # Convertir y validar valores numéricos
            self.sobrecostos_df['hora'] = pd.to_numeric(
                self.sobrecostos_df['hora'],
                errors='coerce'
            )
            self.sobrecostos_df['Sobrecosto_CLP'] = pd.to_numeric(
                self.sobrecostos_df['Sobrecosto_CLP'],
                errors='coerce'
            )

            # Verificar valores nulos
            if self.sobrecostos_df[['hora', 'Sobrecosto_CLP']].isna().any().any():
                self.log_error(
                    "INVALID_DATA_TYPE",
                    "Se encontraron valores no numéricos en hora o Sobrecosto_CLP"
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
        Valida los valores de Sobrecostos.

        Returns:
            bool: True si los valores son válidos
        """
        try:
            # Validar rango de horas
            if self.sobrecostos_df['hora'].between(0, 23).all() is False:
                self.log_error(
                    "INVALID_HOUR",
                    "Se encontraron horas fuera del rango válido (0-23)"
                )
                return False

            # Validar que las fechas correspondan al periodo
            year_month = datetime.strptime(self.periodo, "%Y%m").strftime("%Y-%m")
            if not self.sobrecostos_df['fecha'].dt.strftime("%Y-%m").eq(year_month).all():
                self.log_error(
                    "INVALID_DATE",
                    f"Se encontraron fechas que no corresponden al periodo {self.periodo}"
                )
                return False

            return True

        except Exception as e:
            self.log_error(
                "VALUE_VALIDATION",
                f"Error validando los valores: {str(e)}"
            )
            return False

    def process(self) -> bool:
        """
        Procesa los datos de Sobrecostos.

        Returns:
            bool: True si el procesamiento fue exitoso
        """
        try:
            if not self.validate_data():
                return False

            # Procesar sobrecostos por tipo y central
            self.processed_data['por_tipo'] = self._process_by_tipo()
            self.processed_data['por_central'] = self._process_by_central()
            self.processed_data['resumen'] = self._calculate_resumen()

            return True

        except Exception as e:
            self.log_error("PROCESSING_ERROR", f"Error en procesamiento: {str(e)}")
            return False

    def _process_by_tipo(self) -> pd.DataFrame:
        """
        Procesa los sobrecostos agrupados por tipo.

        Returns:
            pd.DataFrame: DataFrame con los sobrecostos procesados por tipo
        """
        return self.sobrecostos_df.groupby('tipo').agg({
            'Sobrecosto_CLP': ['sum', 'count', 'mean']
        }).round(2)

    def _process_by_central(self) -> pd.DataFrame:
        """
        Procesa los sobrecostos agrupados por central.

        Returns:
            pd.DataFrame: DataFrame con los sobrecostos procesados por central
        """
        return self.sobrecostos_df.groupby(['central', 'tipo']).agg({
            'Sobrecosto_CLP': ['sum', 'count', 'mean']
        }).round(2)

    def _calculate_resumen(self) -> Dict[str, float]:
        """
        Calcula el resumen de sobrecostos.

        Returns:
            Dict[str, float]: Diccionario con los valores resumen
        """
        return {
            'total_sobrecostos': float(self.sobrecostos_df['Sobrecosto_CLP'].sum()),
            'promedio_por_hora': float(
                self.sobrecostos_df.groupby(['fecha', 'hora'])['Sobrecosto_CLP']
                .sum().mean()
            ),
            'max_sobrecosto_horario': float(
                self.sobrecostos_df.groupby(['fecha', 'hora'])['Sobrecosto_CLP']
                .sum().max()
            ),
            'cantidad_centrales_afectadas': int(
                self.sobrecostos_df['central'].nunique()
            )
        }
