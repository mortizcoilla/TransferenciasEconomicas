"""
Punto de entrada principal para el procesamiento de Transferencias Económicas.
"""

import logging
import os
import sys
from typing import Dict, Optional

import pandas as pd

from src.utils.config_loader import ConfigLoader
from src.utils.zip_handler import ZipHandler
from src.utils.period_handler import PeriodHandler
from src.etl.data_extractor import DataExtractor
from src.processors.balance_processor import BalanceProcessor
from src.processors.sobrecostos_processor import SobrecostosProcessor
from src.processors.precio_estabilizado_processor import PrecioEstabilizadoProcessor
from src.processors.costos_variables_processor import CostosVariablesProcessor
from src.processors.transmision_processor import TransmisionProcessor
from src.processors.sscc_processor import SSCCProcessor
from src.etl.file_registry import FileRegistryError, FileNotFoundError

def setup_logging(config: Dict) -> None:
    """
    Configura el sistema de logging.
    
    Args:
        config: Configuración del sistema
    """
    log_config = config.get('logging', {})
    log_path = os.path.dirname(log_config.get('file', 'logs/process.log'))
    os.makedirs(log_path, exist_ok=True)
    
    logging.basicConfig(
        level=log_config.get('level', 'INFO'),
        format=log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
        handlers=[
            logging.FileHandler(log_config.get('file', 'logs/process.log')),
            logging.StreamHandler()
        ]
    )

def process_data(periodo: str, data: Dict[str, pd.DataFrame]) -> Dict[str, Dict]:
    """
    Ejecuta los procesadores específicos para cada tipo de dato.
    
    Args:
        periodo: Periodo a procesar
        data: Diccionario con los DataFrames extraídos
        
    Returns:
        Dict: Resultados del procesamiento por tipo
    """
    logger = logging.getLogger(__name__)
    results = {}

    processors = [
        ('balance', BalanceProcessor(periodo, data)),
        ('sobrecostos', SobrecostosProcessor(periodo, data)),
        ('precio_estabilizado', PrecioEstabilizadoProcessor(periodo, data)),
        ('costos_variables', CostosVariablesProcessor(periodo, data)),
        ('transmision', TransmisionProcessor(periodo, data)),
        ('sscc', SSCCProcessor(periodo, data))
    ]

    for name, processor in processors:
        logger.info(f"Iniciando procesamiento de {name}...")
        
        try:
            if processor.process():
                results[name] = processor.get_processed_data()
                logger.info(f"Procesamiento de {name} completado exitosamente")
            else:
                logger.error(f"Error en el procesamiento de {name}")
                errors = processor.get_errors()
                for error_type, message in errors.items():
                    logger.error(f"{error_type}: {message}")
                results[name] = None
        
        except Exception as e:
            logger.error(f"Error inesperado procesando {name}: {str(e)}")
            results[name] = None

    return results

def validate_directories(config: Dict) -> bool:
    """
    Valida que existan los directorios necesarios.
    
    Args:
        config: Configuración del sistema
        
    Returns:
        bool: True si todos los directorios existen o fueron creados
    """
    logger = logging.getLogger(__name__)
    paths = config.get('paths', {})
    
    try:
        for path_name, path in paths.items():
            os.makedirs(path, exist_ok=True)
            logger.info(f"Directorio {path_name} validado: {path}")
        return True
    
    except Exception as e:
        logger.error(f"Error validando directorios: {str(e)}")
        return False

def export_results(results: Dict, periodo: str, config: Dict) -> None:
    """
    Exporta los resultados del procesamiento.
    
    Args:
        results: Resultados del procesamiento
        periodo: Periodo procesado
        config: Configuración del sistema
    """
    logger = logging.getLogger(__name__)
    export_path = os.path.join(config['paths']['processed'], periodo, 'results')
    os.makedirs(export_path, exist_ok=True)

    try:
        for process_type, data in results.items():
            if data is not None:
                # Guardar resultados según el tipo
                if isinstance(data, dict):
                    for key, value in data.items():
                        if isinstance(value, pd.DataFrame):
                            filename = f"{process_type}_{key}_{periodo}.csv"
                            value.to_csv(os.path.join(export_path, filename), index=False)
                            logger.info(f"Resultados guardados: {filename}")
                elif isinstance(data, pd.DataFrame):
                    filename = f"{process_type}_{periodo}.csv"
                    data.to_csv(os.path.join(export_path, filename), index=False)
                    logger.info(f"Resultados guardados: {filename}")
    
    except Exception as e:
        logger.error(f"Error exportando resultados: {str(e)}")

def main() -> None:
    """Función principal del proceso."""
    try:
        # Cargar configuración
        config = ConfigLoader()
        config_data = config.get_config()
        
        # Configurar logging
        setup_logging(config_data)
        logger = logging.getLogger(__name__)
        
        # Validar directorios
        if not validate_directories(config_data):
            logger.error("Error en la validación de directorios")
            sys.exit(1)
        
        # Obtener periodo a procesar
        periodo = PeriodHandler.get_period_input(config_data['paths']['raw'])
        if not periodo:
            logger.error("No se pudo obtener un periodo válido para procesar")
            sys.exit(1)
        
        logger.info(f"Iniciando procesamiento para periodo {periodo}")
        
        # Procesar archivos ZIP
        zip_handler = ZipHandler(
            raw_path=config_data['paths']['raw'],
            processed_path=config_data['paths']['processed'],
            archive_path=config_data['paths']['archive']
        )
        
        if not zip_handler.process_period_zips(periodo):
            logger.error("Error procesando archivos ZIP")
            sys.exit(1)
        
        logger.info("Archivos ZIP procesados correctamente")
        
        # Extraer datos
        logger.info("Iniciando extracción de datos...")
        data_extractor = DataExtractor(periodo)
        
        try:
            if not data_extractor.validate_required_files():
                logger.error("No se encontraron todos los archivos requeridos")
                sys.exit(1)
            
            if not data_extractor.extract_all():
                logger.error("Error en la extracción de datos")
                sys.exit(1)
            
            extracted_data = data_extractor.get_extracted_data()
            logger.info(f"Datos extraídos exitosamente: {list(extracted_data.keys())}")
            
            # Procesar datos
            logger.info("Iniciando procesamiento de datos...")
            results = process_data(periodo, extracted_data)
            
            # Exportar resultados
            logger.info("Exportando resultados...")
            export_results(results, periodo, config_data)
            
            logger.info("Procesamiento completado exitosamente")
        
        except (FileRegistryError, FileNotFoundError) as e:
            logger.error(f"Error en el registro de archivos: {str(e)}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error inesperado: {str(e)}")
            sys.exit(1)
    
    except Exception as e:
        logging.error(f"Error crítico en el proceso principal: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()