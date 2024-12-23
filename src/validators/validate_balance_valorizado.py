import os
import logging
import pandas as pd
import mysql.connector
from pathlib import Path
from src.utils.config_loader import ConfigLoader
from src.utils.homologation_dictionaries import (
    barras_dict,
    empresas_dict,
    descripciones_dict,
)

# Configurar el logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ValidateBalanceValorizado:
    """
    Clase para realizar validaciones y correcciones sobre el archivo Balance_Valorizado y conexión a SQL.
    """

    def __init__(self, config: dict):
        """
        Inicializa la clase con la configuración necesaria.

        Args:
            config (dict): Configuración cargada desde config.yml.
        """
        self.processed_path = Path(config["paths"]["processed"])
        self.db_config = config["database"]

        try:
            logger.info("Estableciendo conexión con la base de datos principal...")
            self.connection_main = mysql.connector.connect(
                host=self.db_config["host"],
                port=self.db_config["port"],
                user=self.db_config["user"],
                password=self.db_config["password"],
                database="prueba_transferencias",  # Base de datos principal
                allow_local_infile=True,
            )

            logger.info("Estableciendo conexión con la base de datos auxiliar...")
            self.connection_aux = mysql.connector.connect(
                host=self.db_config["host"],
                port=self.db_config["port"],
                user=self.db_config["user"],
                password=self.db_config["password"],
                database="prueba_importar",  # Base de datos auxiliar
            )
            logger.info("Conexiones a las bases de datos de prueba exitosas.")
        except mysql.connector.Error as e:
            logger.error(f"Error al conectar con las bases de datos: {e}")
            raise

    def load_to_temp(self, period: str):
        """
        Carga el archivo a una tabla temporal para validaciones.

        Args:
            period (str): Periodo en formato YYYYMM.
        """
        logger.info("Iniciando carga de datos en tabla temporal...")
        file_path = (
            self.processed_path
            / period
            / "valorizado"
            / f"Balance_Valorizado_{period[-4:]}_Data_VALORIZADO_15min.csv"
        )

        if not file_path.exists():
            logger.error(f"El archivo {file_path} no existe.")
            return

        mysql_file_path = str(file_path).replace("\\", "/")

        try:
            cursor_main = self.connection_main.cursor()

            # Crear tabla temporal si no existe
            cursor_main.execute(
                """
            CREATE TEMPORARY TABLE IF NOT EXISTS balance_valorizado_temp LIKE balance_valorizado;
            """
            )

            # Eliminar datos existentes en la tabla temporal
            cursor_main.execute("DELETE FROM balance_valorizado_temp;")

            # Cargar datos desde CSV a la tabla temporal
            load_query = f"""
            LOAD DATA LOCAL INFILE '{mysql_file_path}'
            INTO TABLE balance_valorizado_temp
            CHARACTER SET UTF8
            FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"'
            LINES TERMINATED BY '\r\n'
            IGNORE 1 LINES;
            """
            cursor_main.execute(load_query)
            self.connection_main.commit()

            logger.info(
                "Datos cargados exitosamente en tabla temporal balance_valorizado_temp."
            )

        except mysql.connector.Error as e:
            logger.error(f"Error durante la carga a la tabla temporal: {str(e)}")

    def validate_and_correct(self, period: str):
        """
        Realiza validaciones de barras, empresas y descripciones en la tabla temporal y corrige automáticamente.

        Args:
            period (str): Periodo en formato YYYYMM.
        """
        logger.info("Iniciando proceso de validación y corrección...")
        cursor_main = self.connection_main.cursor()

        # Validación y corrección de barras
        self.correct_barras(cursor_main)

        # Validación y corrección de empresas
        self.correct_empresas(cursor_main)

        # Validación y corrección de descripciones
        self.correct_descripciones(cursor_main)

        # Si no hay errores, mover datos a la tabla principal
        self.move_to_main_table()

        # Exportar resultados a CSV
        self.export_to_csv(period)

    def correct_barras(self, cursor):
        """Valida y corrige las barras utilizando los diccionarios de equivalencias."""
        logger.info("Corrigiendo barras...")
        for original, corrected in barras_dict.items():
            try:
                update_query = f"""
                UPDATE balance_valorizado_temp
                SET nombre_barra = '{corrected}'
                WHERE nombre_barra = '{original}';
                """
                cursor.execute(update_query)
            except mysql.connector.Error as e:
                logger.error(f"Error al corregir barra '{original}': {e}")
        self.connection_main.commit()
        logger.info("Barras corregidas utilizando los diccionarios de equivalencias.")
        logger.info("Barras corregidas utilizando los diccionarios de equivalencias.")

    def correct_empresas(self, cursor):
        """Valida y corrige las empresas utilizando los diccionarios de equivalencias."""
        logger.info("Corrigiendo empresas...")
        for original, corrected in empresas_dict.items():
            try:
                update_query = f"""
                UPDATE balance_valorizado_temp
                SET propietario = '{corrected}'
                WHERE propietario = '{original}';
                """
                cursor.execute(update_query)
            except mysql.connector.Error as e:
                logger.error(f"Error al corregir empresa '{original}': {e}")
        self.connection_main.commit()
        logger.info("Empresas corregidas utilizando los diccionarios de equivalencias.")
        logger.info("Empresas corregidas utilizando los diccionarios de equivalencias.")

    def correct_descripciones(self, cursor):
        """Valida y corrige las descripciones utilizando los diccionarios de equivalencias."""
        logger.info("Corrigiendo descripciones...")
        for original, corrected in descripciones_dict.items():
            try:
                update_query = """
                UPDATE balance_valorizado_temp
                SET descripcion = %s
                WHERE descripcion = %s;
                """
                cursor.execute(update_query, (corrected, original))
            except mysql.connector.Error as e:
                logger.error(f"Error al corregir descripcion '{original}': {e}")
        self.connection_main.commit()
        logger.info(
            "Descripciones corregidas utilizando los diccionarios de equivalencias."
        )
        logger.info(
            "Descripciones corregidas utilizando los diccionarios de equivalencias."
        )

    def move_to_main_table(self):
        """
        Mueve los datos validados de la tabla temporal a la tabla principal.
        """
        logger.info("Moviendo datos validados a la tabla principal...")
        try:
            cursor_main = self.connection_main.cursor()

            # Eliminar datos existentes en la tabla principal
            cursor_main.execute("DELETE FROM balance_valorizado;")

            # Mover datos desde la tabla temporal a la principal
            cursor_main.execute(
                """
            INSERT INTO balance_valorizado SELECT * FROM balance_valorizado_temp;
            """
            )
            self.connection_main.commit()

            logger.info(
                "Datos validados movidos a la tabla principal balance_valorizado."
            )

        except mysql.connector.Error as e:
            logger.error(f"Error al mover datos a la tabla principal: {str(e)}")

    def export_to_csv(self, period: str):
        """
        Exporta los resultados de la tabla principal a un archivo CSV en bloques.

        Args:
            period (str): Periodo en formato YYYYMM.
        """
        output_path = Path(
            f"C:/Workspace/TransferenciasEconomicas/data/outputs/balance_valorizado_{period}.csv"
        )
        logger.info(f"Exportando resultados a {output_path}...")

        try:
            cursor_main = self.connection_main.cursor(dictionary=True)
            query = "SELECT * FROM balance_valorizado;"
            cursor_main.execute(query)

            with open(output_path, mode="w", encoding="utf-8", newline="") as file:
                fieldnames = [i[0] for i in cursor_main.description]
                writer = pd.io.common.csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()

                while True:
                    rows = cursor_main.fetchmany(1000)
                    if not rows:
                        break
                    writer.writerows(rows)

            logger.info(f"Resultados exportados exitosamente a {output_path}")
        except Exception as e:
            logger.error(f"Error al exportar los resultados a CSV: {e}")

    def __del__(self):
        """
        Cierra las conexiones a las bases de datos.
        """
        try:
            if hasattr(self, "connection_main") and self.connection_main:
                self.connection_main.close()
            if hasattr(self, "connection_aux") and self.connection_aux:
                self.connection_aux.close()
            logger.info("Conexiones a las bases de datos cerradas.")
        except Exception as e:
            logger.error(f"Error al cerrar las conexiones: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Cargar configuración
    config_loader = ConfigLoader()
    config = config_loader.get_config()

    # Periodo de prueba
    period = "202407"

    # Inicializar el validador
    logger.info("Inicializando validador...")
    validator = ValidateBalanceValorizado(config)

    # Ejecutar carga y validaciones
    try:
        logger.info("Iniciando el flujo de validación...")
        validator.load_to_temp(period)
        validator.validate_and_correct(period)
    except Exception as e:
        logger.error(f"Error durante el proceso: {e}")
    finally:
        del validator
