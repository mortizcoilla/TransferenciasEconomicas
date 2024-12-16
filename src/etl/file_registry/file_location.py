"""
Módulo para el manejo de ubicaciones de archivos.
"""

import os
from pathlib import Path
from typing import List, Dict, Any
from .exceptions import FileLocationError

class FileLocation:
    """Clase para manejar las ubicaciones de archivos."""

    def __init__(self, base_path: str, location_config: dict):
        """
        Inicializa una ubicación de archivo.

        Args:
            base_path: Ruta base donde buscar
            location_config: Configuración de la ubicación
        """
        self.base_path = Path(base_path)
        self.relative_path = location_config.get('path', '')
        self.patterns = location_config.get('patterns', [])

    def get_absolute_path(self, **kwargs) -> Path:
        """
        Obtiene la ruta absoluta con variables reemplazadas.

        Args:
            **kwargs: Variables para reemplazar en la ruta
                path_variables: Diccionario de variables de ruta
                periodo: Periodo a procesar
                otros parámetros según necesidad

        Returns:
            Path: Ruta absoluta

        Raises:
            FileLocationError: Si faltan variables necesarias
        """
        try:
            # Obtener variables de ruta
            path_vars = kwargs.get('path_variables', {})
            path_with_dirs = self.relative_path

            # Reemplazar variables de ruta
            for var_name, var_value in path_vars.items():
                if isinstance(var_value, str):
                    # Reemplazar las variables de período en el valor
                    formatted_value = var_value.format(**kwargs)
                    path_with_dirs = path_with_dirs.replace(
                        f"{{{var_name}}}", 
                        formatted_value
                    )

            # Aplicar el resto de variables
            formatted_path = path_with_dirs.format(**kwargs)
            return self.base_path / formatted_path

        except KeyError as e:
            raise FileLocationError(f"Falta variable para la ruta: {str(e)}")
        except Exception as e:
            raise FileLocationError(f"Error formateando ruta: {str(e)}")

    def format_pattern(self, pattern: str, **kwargs) -> str:
        """
        Formatea un patrón con las variables proporcionadas.

        Args:
            pattern: Patrón a formatear
            **kwargs: Variables para reemplazar

        Returns:
            str: Patrón formateado
        """
        try:
            return pattern.format(**kwargs)
        except KeyError as e:
            raise FileLocationError(f"Falta variable para el patrón: {str(e)}")
        except Exception as e:
            raise FileLocationError(f"Error formateando patrón: {str(e)}")

    def find_matching_files(self, **kwargs) -> List[Path]:
        """
        Encuentra archivos que coincidan con los patrones.

        Args:
            **kwargs: Variables para reemplazar en los patrones

        Returns:
            List[Path]: Lista de archivos encontrados

        Raises:
            FileLocationError: Si hay error en la búsqueda
        """
        try:
            base_dir = self.get_absolute_path(**kwargs)
            if not base_dir.exists():
                return []

            matching_files = []
            for pattern in self.patterns:
                formatted_pattern = self.format_pattern(pattern, **kwargs)
                matching_files.extend(
                    list(base_dir.glob(formatted_pattern))
                )
            return matching_files

        except Exception as e:
            raise FileLocationError(f"Error buscando archivos: {str(e)}")

    def find_latest_file(self, **kwargs) -> Path | None:
        """
        Encuentra el archivo más reciente que coincida.

        Args:
            **kwargs: Variables para reemplazar en los patrones

        Returns:
            Optional[Path]: Archivo más reciente o None

        Raises:
            FileLocationError: Si hay error en la búsqueda
        """
        try:
            matching_files = self.find_matching_files(**kwargs)
            if not matching_files:
                return None

            return max(matching_files, key=os.path.getmtime)

        except Exception as e:
            raise FileLocationError(f"Error buscando último archivo: {str(e)}")

    def __str__(self) -> str:
        """Representación en string de la ubicación."""
        return f"FileLocation(base={self.base_path}, path={self.relative_path})"

    def __repr__(self) -> str:
        """Representación para debugging."""
        return (f"FileLocation(base_path='{self.base_path}', "
                f"relative_path='{self.relative_path}', "
                f"patterns={self.patterns})")