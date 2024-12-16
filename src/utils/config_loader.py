"""
Módulo para cargar y gestionar la configuración del proyecto.
"""

import os
import logging
from typing import Any, Dict, Optional
import yaml

logger = logging.getLogger(__name__)

class ConfigLoader:
    """
    Clase para cargar y gestionar la configuración del proyecto.
    Implementa el patrón Singleton para asegurar una única instancia de configuración.
    """
    _instance = None
    _config = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigLoader, cls).__new__(cls)
            cls._instance._load_config()
        return cls._instance

    def _load_config(self) -> None:
        """
        Carga la configuración desde el archivo config.yml.
        
        Raises:
            FileNotFoundError: Si no encuentra el archivo de configuración
            yaml.YAMLError: Si hay un error en el formato del archivo
        """
        try:
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                'config',
                'config.yml'
            )
            
            with open(config_path, 'r', encoding='utf-8') as file:
                self._config = yaml.safe_load(file)
                logger.info("Configuración cargada exitosamente")
        except FileNotFoundError as e:
            logger.error(f"Archivo de configuración no encontrado: {config_path}")
            raise FileNotFoundError(f"No se encontró el archivo de configuración: {e}")
        except yaml.YAMLError as e:
            logger.error(f"Error en el formato del archivo de configuración: {str(e)}")
            raise yaml.YAMLError(f"Error en el formato del archivo YAML: {e}")
        except Exception as e:
            logger.error(f"Error inesperado cargando configuración: {str(e)}")
            raise

    def get_config(self, section: Optional[str] = None) -> Dict[str, Any]:
        """
        Obtiene la configuración completa o una sección específica.

        Args:
            section (str, optional): Nombre de la sección de configuración.

        Returns:
            Dict[str, Any]: Configuración solicitada.

        Raises:
            KeyError: Si la sección solicitada no existe.
        """
        if section is None:
            return self._config

        try:
            return self._config[section]
        except KeyError as e:
            logger.error(f"Sección de configuración no encontrada: {section}")
            raise KeyError(f"Sección de configuración no encontrada: {section}")

    def get_path(self, path_type: str) -> str:
        """
        Obtiene una ruta específica de la configuración.

        Args:
            path_type (str): Tipo de ruta (ej: 'raw', 'processed', 'archive')

        Returns:
            str: Ruta solicitada

        Raises:
            KeyError: Si el tipo de ruta no existe en la configuración
        """
        try:
            return self._config['paths'][path_type]
        except KeyError as e:
            logger.error(f"Tipo de ruta no encontrado: {path_type}")
            raise KeyError(f"Tipo de ruta no encontrado: {path_type}")

    def get_file_pattern(self, file_type: str) -> str:
        """
        Obtiene el patrón de nombre de archivo para un tipo específico.

        Args:
            file_type (str): Tipo de archivo (ej: 'balance_valorizado', 'contratos_gx')

        Returns:
            str: Patrón del nombre de archivo

        Raises:
            KeyError: Si el tipo de archivo no existe en la configuración
        """
        try:
            return self._config['expected_files'][file_type]
        except KeyError as e:
            logger.error(f"Tipo de archivo no encontrado: {file_type}")
            raise KeyError(f"Tipo de archivo no encontrado: {file_type}")

    def get_periodo(self) -> str:
        """
        Obtiene el periodo inicial configurado.

        Returns:
            str: Periodo inicial (ej: '202409')

        Raises:
            KeyError: Si no se encuentra la configuración del periodo
        """
        try:
            return self._config['process']['periodo_inicial']
        except KeyError as e:
            logger.error("Periodo inicial no encontrado en la configuración")
            raise KeyError("Periodo inicial no encontrado en la configuración")