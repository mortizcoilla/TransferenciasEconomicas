"""
Módulo para procesar y validar datos del Balance Valorizado.
Este módulo se centra en asegurar la consistencia de los datos con la base de datos existente
y realizar los cálculos necesarios sin modificar la base de datos.
"""

import logging
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
import mysql.connector
from yaml import safe_load
from .base_processor import BaseProcessor

logger = logging.getLogger(__name__)


class BalanceProcessor(BaseProcessor):
    """
    Procesador para datos del Balance Valorizado.

    Esta clase realiza dos funciones principales:
    1. Valida que los datos sean consistentes con la información en la base de datos
    2. Procesa los datos para generar los resultados requeridos
    """

    # Columnas requeridas en el archivo de Balance Valorizado
    REQUIRED_COLUMNS = [
        "nombre_barra",
        "propietario",
        "descripcion",
        "tipo1",
        "hora_mensual",
        "MedidaHoraria",
        "CMG_PESO_KWH",
        "VALORIZADO_PESOS",
        "pnudo",
        "clave",
        "nro_lt",
        "clave_lt",
        "MedidaHoraria2",
    ]

    def __init__(self, periodo: str, data: Dict[str, pd.DataFrame]):
        """
        Inicializa el procesador con los datos del periodo a analizar.

        Args:
            periodo: Periodo a procesar (YYYYMM)
            data: Diccionario que contiene el DataFrame del Balance Valorizado
        """
        super().__init__(periodo, data)
        self.balance_df = self.data.get("balance_valorizado")
        self.conn = self._get_db_connection()
        self.validation_errors = []

    def _get_db_connection(self):
        """Establece conexión de solo lectura con la base de datos."""
        try:
            with open("config.yml", "r") as f:
                config = safe_load(f)["database"]

            # Creamos una conexión de solo lectura
            conn = mysql.connector.connect(**config)
            cursor = conn.cursor()
            cursor.execute("SET SESSION TRANSACTION READ ONLY")
            return conn
        except Exception as e:
            self.log_error(
                "DB_CONNECTION_ERROR", f"Error conectando a base de datos: {str(e)}"
            )
            return None

    def _validate_barras(self) -> bool:
        """
        Verifica que todas las barras existan en la base de datos.
        Las barras son puntos de medición del sistema eléctrico.
        """
        query = """
        SELECT DISTINCT t1.nombre_barra
        FROM (SELECT DISTINCT nombre_barra FROM balance_valorizado) t1
        LEFT JOIN barra2 b2 ON b2.col_1=t1.nombre_barra
        LEFT JOIN barra t2 ON b2.nombrebarra=t2.nombre
        WHERE t2.Id IS NULL
        """
        try:
            invalid_barras = pd.read_sql(query, self.conn)
            if not invalid_barras.empty:
                error_msg = (
                    f"Barras no encontradas: {invalid_barras['nombre_barra'].tolist()}"
                )
                self.validation_errors.append(("BARRAS", error_msg))
                return False
            return True
        except Exception as e:
            self.validation_errors.append(("BARRAS", f"Error en validación: {str(e)}"))
            return False

    def _validate_empresas(self) -> bool:
        """
        Verifica que todas las empresas existan en la base de datos.
        Las empresas son los participantes del mercado eléctrico.
        """
        query = """
        SELECT t1.propietario, tipo1
        FROM (SELECT DISTINCT propietario, tipo1 FROM balance_valorizado) t1
        LEFT JOIN empresa2 E2 on E2.col_7=t1.propietario
        LEFT JOIN empresa t2 ON E2.NombreEmpresa=t2.nombre
        WHERE t2.Id IS NULL
        """
        try:
            invalid_empresas = pd.read_sql(query, self.conn)
            if not invalid_empresas.empty:
                error_msg = "Empresas no encontradas:\n" + invalid_empresas.to_string(
                    index=False
                )
                self.validation_errors.append(("EMPRESAS", error_msg))
                return False
            return True
        except Exception as e:
            self.validation_errors.append(
                ("EMPRESAS", f"Error en validación: {str(e)}")
            )
            return False

    def _validate_descripcion(self) -> bool:
        """
        Verifica que todas las descripciones sean válidas.
        Las descripciones identifican el tipo de transacción energética.
        """
        query = """
        SELECT DISTINCT t1.descripcion, t1.tipo1 as TipoRegistro
        FROM (SELECT DISTINCT descripcion, tipo1 FROM balance_valorizado) t1
        LEFT JOIN val_descripcion2 VD2 ON VD2.col_8=t1.descripcion
        LEFT JOIN val_descripcion t2 ON VD2.descripcion=t2.nombre 
        WHERE t2.Id IS NULL
        """
        try:
            invalid_desc = pd.read_sql(query, self.conn)
            if not invalid_desc.empty:
                error_msg = "Descripciones no encontradas:\n" + invalid_desc.to_string(
                    index=False
                )
                self.validation_errors.append(("DESCRIPCIONES", error_msg))
                return False
            return True
        except Exception as e:
            self.validation_errors.append(
                ("DESCRIPCIONES", f"Error en validación: {str(e)}")
            )
            return False

    def validate_data(self) -> bool:
        """
        Ejecuta todas las validaciones necesarias sobre los datos.
        """
        if self.balance_df is None:
            self.validation_errors.append(
                ("DATA", "No se encontraron datos de Balance Valorizado")
            )
            return False

        # Validación de columnas requeridas
        if not self.validate_dataframe(
            self.balance_df, self.REQUIRED_COLUMNS, "Balance Valorizado"
        ):
            return False

        # Validación de tipos de datos
        if not self._validate_data_types():
            return False

        # Validaciones contra la base de datos
        validations_ok = all(
            [
                self._validate_barras(),
                self._validate_empresas(),
                self._validate_descripcion(),
            ]
        )

        return validations_ok

    def get_validation_errors(self) -> List[tuple]:
        """
        Retorna la lista de errores de validación encontrados.
        """
        return self.validation_errors

    def process(self) -> bool:
        """
        Procesa los datos si pasan todas las validaciones.
        """
        try:
            if not self.validate_data():
                logger.error("Errores de validación encontrados:")
                for error_type, message in self.validation_errors:
                    logger.error(f"{error_type}: {message}")
                return False

            # Procesar datos por tipo
            self.processed_data["generacion"] = self._process_generacion()
            self.processed_data["retiros"] = self._process_retiros()
            self.processed_data["resumen"] = self._calculate_resumen()

            return True

        except Exception as e:
            self.validation_errors.append(
                ("PROCESSING", f"Error en procesamiento: {str(e)}")
            )
            return False

    def __del__(self):
        """Cierra la conexión a la base de datos al finalizar."""
        if hasattr(self, "conn") and self.conn:
            self.conn.close()
