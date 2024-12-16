"""
Módulo para procesar datos de Servicios Complementarios (SSCC).
"""

import logging
from typing import Dict, Optional
import pandas as pd
from .base_processor import BaseProcessor

logger = logging.getLogger(__name__)

class SSCCProcessor(BaseProcessor):
    """Procesador para datos de Servicios Complementarios."""

    REQUIRED_COLUMNS = [
        "concepto",
        "empresa",
        "recibe",
        "paga",
        "sen"
    ]

    def __init__(self, periodo: str, data: Dict[str, pd.DataFrame]):
        """
        Inicializa el procesador de SSCC.

        Args:
            periodo (str): Periodo a procesar (YYYYMM)
            data (Dict[str, pd.DataFrame]): Diccionario con los datos extraídos
        """
        super().__init__(periodo, data)
        self.sscc_df = self.data.get('servicios_complementarios')

    def validate_data(self) -> bool:
        """
        Valida los datos de SSCC.

        Returns:
            bool: True si los datos son válidos
        """
        if self.sscc_df is None:
            self.log_error(
                "DATA_NOT_FOUND",
                "No se encontraron datos de Servicios Complementarios"
            )
            return False

        if not self.validate_dataframe(
            self.sscc_df,
            self.REQUIRED_COLUMNS,
            "Servicios Complementarios"
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
        try:
            # Convertir columnas numéricas
            numeric_columns = ['recibe', 'paga', 'sen']
            for col in numeric_columns:
                self.sscc_df[col] = pd.to_numeric(self.sscc_df[col], errors='coerce')
                if self.sscc_df[col].isna().any():
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

    def process(self) -> bool:
        """
        Procesa los datos de SSCC.

        Returns:
            bool: True si el procesamiento fue exitoso
        """
        try:
            if not self.validate_data():
                return False

            # Procesar datos por concepto
            self.processed_data['por_concepto'] = self._process_by_concepto()
            
            # Procesar datos por empresa
            self.processed_data['por_empresa'] = self._process_by_empresa()
            
            # Calcular balance por empresa
            self.processed_data['balance_empresa'] = self._calculate_balance_empresa()
            
            # Calcular resumen
            self.processed_data['resumen'] = self._calculate_resumen()

            return True

        except Exception as e:
            self.log_error("PROCESSING_ERROR", f"Error en procesamiento: {str(e)}")
            return False

    def _process_by_concepto(self) -> pd.DataFrame:
        """
        Procesa los datos agrupados por concepto de SSCC.

        Returns:
            pd.DataFrame: DataFrame con los datos procesados por concepto
        """
        return self.sscc_df.groupby('concepto').agg({
            'recibe': 'sum',
            'paga': 'sum',
            'sen': 'sum'
        }).round(2)

    def _process_by_empresa(self) -> pd.DataFrame:
        """
        Procesa los datos agrupados por empresa.

        Returns:
            pd.DataFrame: DataFrame con los datos procesados por empresa
        """
        return self.sscc_df.groupby('empresa').agg({
            'recibe': 'sum',
            'paga': 'sum',
            'sen': 'sum'
        }).round(2)

    def _calculate_balance_empresa(self) -> pd.DataFrame:
        """
        Calcula el balance neto por empresa.

        Returns:
            pd.DataFrame: DataFrame con el balance por empresa
        """
        balance_df = self.sscc_df.groupby('empresa').agg({
            'recibe': 'sum',
            'paga': 'sum',
            'sen': 'sum'
        }).round(2)

        # Calcular balance neto
        balance_df['balance_neto'] = (
            balance_df['recibe'] - 
            balance_df['paga'] + 
            balance_df['sen']
        ).round(2)

        # Ordenar por balance neto
        return balance_df.sort_values('balance_neto', ascending=False)

    def _calculate_resumen(self) -> Dict[str, float]:
        """
        Calcula el resumen de SSCC.

        Returns:
            Dict[str, float]: Diccionario con los valores resumen
        """
        return {
            'total_recibe': float(self.sscc_df['recibe'].sum()),
            'total_paga': float(self.sscc_df['paga'].sum()),
            'total_sen': float(self.sscc_df['sen'].sum()),
            'balance_sistema': float(
                self.sscc_df['recibe'].sum() - 
                self.sscc_df['paga'].sum() + 
                self.sscc_df['sen'].sum()
            ),
            'cantidad_empresas': int(self.sscc_df['empresa'].nunique()),
            'cantidad_conceptos': int(self.sscc_df['concepto'].nunique()),
            'max_pago': float(self.sscc_df['paga'].max()),
            'max_cobro': float(self.sscc_df['recibe'].max())
        }

    def get_balance_report(self) -> str:
        """
        Genera un reporte de balance del sistema SSCC.

        Returns:
            str: Reporte formateado del balance del sistema
        """
        try:
            if not self.processed_data:
                return "No hay datos procesados disponibles"

            resumen = self.processed_data['resumen']
            
            report = [
                "Reporte de Balance SSCC",
                "=" * 30,
                f"Período: {self.periodo}",
                f"Total Empresas: {resumen['cantidad_empresas']}",
                f"Total Conceptos: {resumen['cantidad_conceptos']}",
                "",
                "Balance del Sistema",
                "-" * 20,
                f"Total Recibe: {resumen['total_recibe']:,.2f}",
                f"Total Paga: {resumen['total_paga']:,.2f}",
                f"Total SEN: {resumen['total_sen']:,.2f}",
                f"Balance Neto: {resumen['balance_sistema']:,.2f}",
                "",
                "Valores Máximos",
                "-" * 15,
                f"Mayor Pago: {resumen['max_pago']:,.2f}",
                f"Mayor Cobro: {resumen['max_cobro']:,.2f}"
            ]
            
            return "\n".join(report)

        except Exception as e:
            logger.error(f"Error generando reporte: {str(e)}")
            return "Error generando reporte"