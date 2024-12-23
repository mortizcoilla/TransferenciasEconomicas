"""
Módulo principal del registro de archivos.
Este módulo coordina la gestión y acceso a las definiciones de archivos del sistema.
"""

import logging
from pathlib import Path
from typing import Dict, Optional
from .exceptions import FileRegistryError
from .file_definition import FileDefinition, FileValidation
from .file_location import FileLocation
from .file_index import FileIndex

logger = logging.getLogger(__name__)


class FileRegistry:
    """
    Clase principal para el registro y gestión de archivos.
    Solo registra y proporciona acceso a las definiciones de archivos.
    """

    def __init__(self, config: Dict):
        """
        Inicializa el registro de archivos.
        """
        self.config = config
        self.base_path = Path(config["paths"]["processed"])
        self.definitions: Dict[str, FileDefinition] = {}
        self.index = FileIndex()

        registry_config = self.config.get("file_registry", {})
        self.path_variables = registry_config.get("path_variables", {})

        self._load_definitions()

    def _load_definitions(self) -> None:
        """
        Carga las definiciones de archivos desde la configuración.
        """
        try:
            registry_config = self.config.get("file_registry", {})
            files_config = registry_config.get("files", {})

            for file_key, config in files_config.items():
                validation_config = config.get("validation", {})
                validation = FileValidation(**validation_config)
                file_config = {**config, "validation": validation}
                self.definitions[file_key] = FileDefinition(**file_config)

        except Exception as e:
            logger.error(f"Error cargando definiciones: {str(e)}")
            raise FileRegistryError(f"Error cargando definiciones: {str(e)}")

    def locate_file(self, file_key: str, periodo: str) -> Optional[Path]:
        """
        Localiza un archivo específico.
        """
        cached_location = self.index.get_file_location(file_key)
        if cached_location:
            return cached_location

        definition = self.definitions.get(file_key)
        if not definition:
            return None

        for location_config in definition.locations:
            location = FileLocation(self.base_path, location_config)
            try:
                found_file = location.find_latest_file(
                    periodo=periodo, path_variables=self.path_variables
                )
                if found_file:
                    self.index.register_file(file_key, found_file)
                    return found_file
            except Exception:
                continue

        return None

    def locate_all_files(self, periodo: str) -> Dict[str, Optional[Path]]:
        """
        Localiza todos los archivos configurados.
        """
        locations: Dict[str, Optional[Path]] = {}

        for file_key in self.definitions:
            locations[file_key] = self.locate_file(file_key, periodo)

        return locations

    def clear_cache(self) -> None:
        """
        Limpia el caché de archivos encontrados.
        """
        self.index.clear()
