"""
Módulo para el índice de archivos encontrados.
"""

from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
from .exceptions import FileRegistryError

class FileIndex:
    """Clase para mantener un índice de archivos encontrados."""

    def __init__(self):
        """Inicializa el índice de archivos."""
        self.index: Dict[str, Dict] = {}

    def register_file(
        self,
        file_type: str,
        file_path: Path,
        validation_status: bool = False
    ) -> None:
        """
        Registra un archivo en el índice.

        Args:
            file_type: Tipo de archivo
            file_path: Ruta al archivo
            validation_status: Estado de validación
        """
        self.index[file_type] = {
            'path': file_path,
            'last_modified': datetime.fromtimestamp(
                file_path.stat().st_mtime
            ),
            'validated': validation_status,
            'registered_at': datetime.now()
        }

    def get_file_location(self, file_type: str) -> Optional[Path]:
        """
        Obtiene la ubicación de un archivo registrado.

        Args:
            file_type: Tipo de archivo

        Returns:
            Optional[Path]: Ruta al archivo o None
        """
        if file_type in self.index:
            path = self.index[file_type]['path']
            if path.exists():
                return path
            del self.index[file_type]
        return None

    def is_file_valid(self, file_type: str) -> bool:
        """
        Verifica si un archivo está validado.

        Args:
            file_type: Tipo de archivo

        Returns:
            bool: True si el archivo está validado
        """
        if file_type in self.index:
            return self.index[file_type]['validated']
        return False

    def clear(self) -> None:
        """Limpia el índice."""
        self.index.clear()

    def get_stats(self) -> Dict:
        """
        Obtiene estadísticas del índice.

        Returns:
            Dict: Estadísticas del índice
        """
        return {
            'total_files': len(self.index),
            'validated_files': sum(
                1 for info in self.index.values() 
                if info['validated']
            ),
            'last_update': max(
                (info['registered_at'] for info in self.index.values()),
                default=None
            )
        }