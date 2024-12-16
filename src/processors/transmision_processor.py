"""
Módulo para procesar datos de Transmisión.
"""

import logging
from typing import Dict, Optional
import pandas as pd
from .base_processor import BaseProcessor

logger = logging.getLogger(__name__)

class TransmisionProcessor(BaseProcessor):
    """Procesador para datos de Transmisión."""

    REQUIRED_COLUMNS = [
        "Barra_Origen",
        "Barra_Destino",
        "Tipo_Linea",
        "Flujo_MW",
        "Perdidas_MW",
        "Factor_Perdida",
        "IT_Propietario",
        "Peaje_CLP"
    ]

    def __init__(self, periodo: str, data: Dict[str, pd.DataFrame]):
        """
        Inicializa el procesador de Transmisión.

        Args:
            periodo (str): Periodo a procesar (YYYYMM)
            data (Dict[str, pd.DataFrame]): Diccionario con los datos extraídos
        """
        super().__init__(periodo, data)
        self.tx_df = self.data.get('transmision')

    def validate_data(self) -> bool:
        """
        Valida los datos de Transmisión.

        Returns:
            bool: True si los datos son válidos
        """
        if self.tx_df is None:
            self.log_error(
                "DATA_NOT_FOUND",
                "No se encontraron datos de Transmisión"
            )
            return False

        if not self.validate_dataframe(
            self.tx_df,
            self.REQUIRED_COLUMNS,
            "Transmisión"
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
            numeric_columns = [
                'Flujo_MW', 
                'Perdidas_MW', 
                'Factor_Perdida', 
                'Peaje_CLP'
            ]
            
            for col in numeric_columns:
                self.tx_df[col] = pd.to_numeric(self.tx_df[col], errors='coerce')
                if self.tx_df[col].isna().any():
                    self.log_error(
                        "INVALID_DATA_TYPE",
                        f"Valores no numéricos en columna {col}"
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
        Valida los valores de Transmisión.

        Returns:
            bool: True si los valores son válidos
        """
        # Validar que las pérdidas no sean mayores que el flujo
        if (self.tx_df['Perdidas_MW'].abs() > self.tx_df['Flujo_MW'].abs()).any():
            self.log_error(
                "INVALID_VALUE",
                "Se encontraron pérdidas mayores que el flujo"
            )
            return False

        # Validar que el factor de pérdida esté en rango razonable
        if not ((0 <= self.tx_df['Factor_Perdida']) & 
                (self.tx_df['Factor_Perdida'] <= 1)).all():
            self.log_error(
                "INVALID_VALUE",
                "Factor de pérdida fuera de rango [0,1]"
            )
            return False

        return True

    def process(self) -> bool:
        """
        Procesa los datos de Transmisión.

        Returns:
            bool: True si el procesamiento fue exitoso
        """
        try:
            if not self.validate_data():
                return False

            # Procesar datos por tipo de línea
            self.processed_data['por_tipo'] = self._process_by_tipo()
            
            # Procesar datos por propietario
            self.processed_data['por_propietario'] = self._process_by_propietario()
            
            # Procesar pérdidas
            self.processed_data['perdidas'] = self._process_perdidas()
            
            # Calcular resumen
            self.processed_data['resumen'] = self._calculate_resumen()

            return True

        except Exception as e:
            self.log_error("PROCESSING_ERROR", f"Error en procesamiento: {str(e)}")
            return False

    def _process_by_tipo(self) -> pd.DataFrame:
        """
        Procesa los datos agrupados por tipo de línea.

        Returns:
            pd.DataFrame: DataFrame con los datos procesados por tipo
        """
        return self.tx_df.groupby('Tipo_Linea').agg({
            'Flujo_MW': ['mean', 'max', 'min'],
            'Perdidas_MW': 'sum',
            'Factor_Perdida': 'mean',
            'Peaje_CLP': 'sum'
        }).round(2)

    def _process_by_propietario(self) -> pd.DataFrame:
        """
        Procesa los datos agrupados por propietario.

        Returns:
            pd.DataFrame: DataFrame con los datos procesados por propietario
        """
        return self.tx_df.groupby('IT_Propietario').agg({
            'Peaje_CLP': 'sum',
            'Flujo_MW': 'mean',
            'Perdidas_MW': 'sum'
        }).round(2)

    def _process_perdidas(self) -> pd.DataFrame:
        """
        Procesa los datos de pérdidas.

        Returns:
            pd.DataFrame: DataFrame con el análisis de pérdidas
        """
        return self.tx_df.groupby(['Barra_Origen', 'Barra_Destino']).agg({
            'Flujo_MW': 'mean',
            'Perdidas_MW': 'sum',
            'Factor_Perdida': 'mean'
        }).sort_values('Perdidas_MW', ascending=False).round(4)

    def _calculate_resumen(self) -> Dict[str, float]:
        """
        Calcula el resumen de transmisión.

        Returns:
            Dict[str, float]: Diccionario con los valores resumen
        """
        return {
            'total_peajes': float(self.tx_df['Peaje_CLP'].sum()),
            'perdidas_totales': float(self.tx_df['Perdidas_MW'].sum()),
            'factor_perdida_promedio': float(self.tx_df['Factor_Perdida'].mean()),
            'flujo_maximo': float(self.tx_df['Flujo_MW'].abs().max()),
            'cantidad_lineas': int(
                self.tx_df.groupby(['Barra_Origen', 'Barra_Destino']).ngroups
            ),
            'cantidad_propietarios': int(self.tx_df['IT_Propietario'].nunique())
        }