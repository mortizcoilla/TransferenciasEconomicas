"""
Módulo principal del registro de archivos.
"""

import logging
from pathlib import Path
from typing import Dict, Optional, List, Set
from .exceptions import (
    FileRegistryError,
    FileNotFoundError,
    ConfigurationError
)
from .file_definition import FileDefinition
from .file_location import FileLocation
from .file_index import FileIndex

logger = logging.getLogger(__name__)

class FileRegistry:
    """Clase principal para el registro y gestión de archivos."""

    def __init__(self, config: Dict):
        """
        Inicializa el registro de archivos.

        Args:
            config: Configuración del registro
        
        Raises:
            ConfigurationError: Si hay errores en la configuración
            FileRegistryError: Si hay errores en la inicialización
        """
        self.config = config
        self.base_path = Path(config['paths']['processed'])
        self.definitions: Dict[str, FileDefinition] = {}
        self.index = FileIndex()
        
        # Obtener variables de ruta
        registry_config = self.config.get('file_registry', {})
        self.path_variables = registry_config.get('path_variables', {})
        
        self._load_definitions()
        self._validate_dependencies()

    def _load_definitions(self) -> None:
        """
        Carga las definiciones de archivos desde la configuración.
        
        Raises:
            ConfigurationError: Si hay errores en la configuración
            FileRegistryError: Si hay errores cargando definiciones
        """
        try:
            registry_config = self.config.get('file_registry')
            if not registry_config:
                raise ConfigurationError("No se encontró la configuración 'file_registry'")

            files_config = registry_config.get('files')
            if not files_config:
                raise ConfigurationError("No se encontró la configuración de archivos")

            for file_key, config in files_config.items():
                if not isinstance(config, dict):
                    raise ConfigurationError(
                        f"Configuración inválida para {file_key}"
                    )

                try:
                    # Crear definición del archivo
                    definition = FileDefinition(**config)
                    
                    # Validar la definición
                    if definition.validate():
                        self.definitions[file_key] = definition
                        logger.debug(f"Definición cargada: {file_key}")
                    
                except Exception as e:
                    raise ConfigurationError(
                        f"Error en definición de {file_key}: {str(e)}"
                    )

        except Exception as e:
            raise FileRegistryError(f"Error cargando definiciones: {str(e)}")

    def _validate_dependencies(self) -> None:
        """
        Valida que todas las dependencias declaradas existan y no haya ciclos.
        
        Raises:
            ConfigurationError: Si hay dependencias no válidas o ciclos
        """
        for file_key, definition in self.definitions.items():
            visited: Set[str] = set()
            self._check_dependency_chain(file_key, definition, visited)

    def _check_dependency_chain(self, file_key: str, definition: FileDefinition, visited: Set[str]) -> None:
        """
        Verifica recursivamente la cadena de dependencias para detectar ciclos.
        
        Args:
            file_key: Clave del archivo actual
            definition: Definición del archivo
            visited: Conjunto de archivos ya visitados
            
        Raises:
            ConfigurationError: Si se detecta un ciclo o una dependencia no válida
        """
        if file_key in visited:
            raise ConfigurationError(f"Ciclo de dependencias detectado en '{file_key}'")
        
        visited.add(file_key)
        
        for dependency in definition.validation.dependencies:
            if dependency not in self.definitions:
                raise ConfigurationError(
                    f"Dependencia '{dependency}' no encontrada para archivo '{file_key}'"
                )
            
            # Verificar recursivamente las dependencias
            self._check_dependency_chain(
                dependency,
                self.definitions[dependency],
                visited.copy()
            )

    def locate_file(self, file_key: str, periodo: str) -> Optional[Path]:
        """
        Localiza un archivo específico.

        Args:
            file_key: Clave del archivo a localizar
            periodo: Periodo a procesar

        Returns:
            Optional[Path]: Ruta al archivo o None si no se encuentra y no es requerido

        Raises:
            FileRegistryError: Si el tipo de archivo no está definido
            FileNotFoundError: Si el archivo es requerido y no se encuentra
        """
        # Verificar cache primero
        cached_location = self.index.get_file_location(file_key)
        if cached_location:
            return cached_location

        if file_key not in self.definitions:
            raise FileRegistryError(
                f"Tipo de archivo no definido: {file_key}"
            )

        definition = self.definitions[file_key]
        
        for location_config in definition.locations:
            location = FileLocation(self.base_path, location_config)
            try:
                found_file = location.find_latest_file(
                    periodo=periodo,
                    path_variables=self.path_variables
                )
                
                if found_file:
                    self.index.register_file(file_key, found_file)
                    return found_file
                    
            except Exception as e:
                logger.warning(
                    f"Error buscando archivo {file_key} en {location_config['path']}: {str(e)}"
                )

        if definition.required:
            raise FileNotFoundError(
                f"Archivo requerido no encontrado: {file_key}"
            )
        
        return None

    def locate_all_files(self, periodo: str) -> Dict[str, Optional[Path]]:
        """
        Localiza todos los archivos configurados.

        Args:
            periodo: Periodo a procesar

        Returns:
            Dict[str, Optional[Path]]: Diccionario de ubicaciones

        Raises:
            FileRegistryError: Si hay errores en la localización
        """
        locations: Dict[str, Optional[Path]] = {}
        errors = []

        for file_key in self.definitions:
            try:
                locations[file_key] = self.locate_file(file_key, periodo)
            except FileNotFoundError as e:
                errors.append(str(e))
                locations[file_key] = None
            except Exception as e:
                logger.error(f"Error inesperado localizando {file_key}: {str(e)}")
                locations[file_key] = None

        if errors:
            raise FileRegistryError(
                "Errores localizando archivos:\n" + "\n".join(errors)
            )

        return locations

    def validate_file(self, file_key: str, file_path: Path) -> bool:
        """
        Valida un archivo según su definición.

        Args:
            file_key: Clave del archivo
            file_path: Ruta al archivo

        Returns:
            bool: True si el archivo es válido
        """
        if not file_path.exists():
            return False
        
        try:
            definition = self.definitions[file_key]
            
            # Aquí se podrían agregar más validaciones específicas
            # como verificar columnas, tipos de datos, etc.
            
            self.index.register_file(
                file_key,
                file_path,
                validation_status=True
            )
            return True
            
        except Exception as e:
            logger.error(f"Error validando archivo {file_key}: {str(e)}")
            return False

    def get_file_info(self, file_key: str) -> Optional[Dict]:
        """
        Obtiene información sobre un archivo registrado.

        Args:
            file_key: Clave del archivo

        Returns:
            Optional[Dict]: Información del archivo o None si no está registrado
        """
        return self.index.get_file_info(file_key)

    def clear_cache(self) -> None:
        """
        Limpia el caché de archivos.
        """
        self.index.clear()