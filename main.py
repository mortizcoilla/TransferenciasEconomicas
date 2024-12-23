"""
Punto de entrada principal para la organización de archivos de Transferencias Económicas.
"""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict

from src.utils.config_loader import ConfigLoader
from src.utils.zip_handler import ZipHandler
from src.utils.period_handler import PeriodHandler
from src.etl.file_organizer import FileOrganizer
from src.validators.validate_balance_valorizado import ValidateBalanceValorizado


def setup_logging(config: Dict) -> None:
    """
    Configura el sistema de logging para seguimiento y depuración.

    Args:
        config: Configuración con parámetros de logging
    """
    log_config = config.get("logging", {})
    log_path = os.path.dirname(log_config.get("file", "logs/process.log"))
    os.makedirs(log_path, exist_ok=True)

    logging.basicConfig(
        level=log_config.get("level", "INFO"),
        format=log_config.get(
            "format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        ),
        handlers=[
            logging.FileHandler(log_config.get("file", "logs/process.log")),
            logging.StreamHandler(),
        ],
    )


def validate_directories(config: Dict) -> bool:
    """
    Valida y crea los directorios necesarios para el procesamiento.

    Args:
        config: Configuración con rutas de directorios

    Returns:
        bool: True si la validación es exitosa
    """
    logger = logging.getLogger(__name__)
    paths = config.get("paths", {})

    try:
        for path_name, path in paths.items():
            os.makedirs(path, exist_ok=True)
            logger.debug(f"Directorio {path_name} validado: {path}")
        return True
    except Exception as e:
        logger.error(f"Error validando directorios: {str(e)}")
        return False


def process_zip_files(zip_handler: ZipHandler, periodo: str) -> bool:
    """
    Procesa los archivos ZIP del periodo especificado.

    Args:
        zip_handler: Instancia de ZipHandler para procesar archivos
        periodo: Periodo a procesar

    Returns:
        bool: True si el procesamiento fue exitoso
    """
    logger = logging.getLogger(__name__)

    try:
        # Procesar archivos ZIP principales
        if not zip_handler.process_period_zips(periodo):
            logger.error("Error procesando archivos ZIP principales")
            return False

        logger.info("Archivos ZIP procesados correctamente")

        # Extraer archivos ZIP anidados
        unzipped_path = os.path.join(zip_handler.unzipped_path, periodo)
        if not zip_handler.extract_all_nested_zips(unzipped_path):
            logger.error("Error extrayendo archivos ZIP anidados")
            return False

        logger.info("Archivos ZIP anidados extraídos correctamente")
        return True

    except Exception as e:
        logger.error(f"Error en procesamiento de ZIP: {str(e)}")
        return False


def organize_files(config: Dict, periodo: str) -> bool:
    """
    Organiza los archivos extraídos en la estructura final.

    Args:
        config: Configuración del sistema
        periodo: Periodo a procesar

    Returns:
        bool: True si la organización fue exitosa
    """
    logger = logging.getLogger(__name__)

    try:
        file_organizer = FileOrganizer(config, periodo)
        success = file_organizer.organize()

        if not success:
            errors = file_organizer.get_errors()
            for error_type, message in errors.items():
                logger.error(f"{error_type}: {message}")

        return success

    except Exception as e:
        logger.error(f"Error en organización de archivos: {str(e)}")
        return False


def main() -> None:
    """
    Función principal que coordina todo el proceso de organización de archivos.

    Esta función implementa el flujo de trabajo completo:
    1. Configuración inicial y logging
    2. Validación de directorios
    3. Procesamiento de archivos ZIP
    4. Organización final de archivos
    """
    start_time = datetime.now()

    try:
        # Configuración inicial
        config = ConfigLoader()
        config_data = config.get_config()
        setup_logging(config_data)
        logger = logging.getLogger(__name__)

        logger.info("Iniciando proceso de organización de archivos")
        logger.info(f"Hora de inicio: {start_time}")

        # Validar directorios base
        if not validate_directories(config_data):
            logger.error("Error en la validación de directorios")
            sys.exit(1)

        # Obtener periodo a procesar
        periodo = PeriodHandler.get_period_input(config_data["paths"]["raw"])
        if not periodo:
            logger.error("No se pudo obtener un periodo válido para procesar")
            sys.exit(1)
        logger.info(f"Procesando periodo: {periodo}")

        # Procesar archivos ZIP
        zip_handler = ZipHandler(
            raw_path=config_data["paths"]["raw"],
            unzipped_path=config_data["paths"]["unzipped"],
            archive_path=config_data["paths"]["archive"],
        )

        if not process_zip_files(zip_handler, periodo):
            sys.exit(1)

        # Organizar archivos
        if not organize_files(config_data, periodo):
            sys.exit(1)

        # Registrar tiempo total de procesamiento
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info("Proceso completado exitosamente")
        logger.info(f"Tiempo total de procesamiento: {duration}")

        # Validación de datos Balance Valorizado
        validator = ValidateBalanceValorizado(config_data)
        if not validator.load_and_validate(periodo):
            logger.error("Error en la validación del archivo Balance Valorizado.")
            sys.exit(1)
        else:
            logger.info("Validación de Balance Valorizado completada exitosamente.")

    except Exception as e:
        logger.error(f"Error crítico en el proceso principal: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
