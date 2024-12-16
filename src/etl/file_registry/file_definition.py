"""
Módulo para la definición de archivos y sus características.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional
from .exceptions import FileDefinitionError

@dataclass
class FileValidation:
    """Clase para las reglas de validación de archivos."""
    required_columns: List[str]
    data_types: Dict[str, str]
    dependencies: List[str]

@dataclass
class SheetDefinition:
    """Definición de una hoja de cálculo para archivos Excel."""
    name: str
    required: bool = True

class FileDefinition:
    """Clase que define las características y reglas de un archivo."""

    def __init__(
        self,
        file_type: str,
        required: bool,
        format: str,
        locations: List[Dict],
        validation: Dict,
        requires_sheets: bool = False,
        encoding: str = 'utf-8',
        sheets: Optional[List[Dict]] = None
    ):
        """
        Inicializa la definición de un archivo.

        Args:
            file_type: Tipo de archivo
            required: Si es requerido
            format: Formato del archivo
            locations: Lista de ubicaciones posibles
            validation: Reglas de validación
            requires_sheets: Si requiere hojas específicas (para Excel)
            encoding: Codificación del archivo
            sheets: Definición de hojas para archivos Excel
        """
        self.file_type = file_type
        self.required = required
        self.format = format.lower()
        self.encoding = encoding
        self.requires_sheets = requires_sheets
        self.validation = FileValidation(
            required_columns=validation.get('required_columns', []),
            data_types=validation.get('data_types', {}),
            dependencies=validation.get('dependencies', [])
        )
        self.locations = locations
        self.sheets = [
            SheetDefinition(**sheet) for sheet in (sheets or [])
        ] if sheets else []

    def validate(self) -> bool:
        """
        Valida que la definición sea correcta y completa.

        Returns:
            bool: True si la definición es válida
        """
        try:
            if not self.file_type:
                raise FileDefinitionError("Tipo de archivo no especificado")

            if not self.locations:
                raise FileDefinitionError(
                    f"No se especificaron ubicaciones para {self.file_type}"
                )

            if self.format not in ['csv', 'xlsx', 'xlsb', 'xls']:
                raise FileDefinitionError(
                    f"Formato no soportado: {self.format}"
                )

            # Validación de hojas de Excel
            if self.requires_sheets:
                if self.format not in ['xlsx', 'xlsb', 'xls']:
                    raise FileDefinitionError(
                        f"Se requieren hojas pero el formato {self.format} no es Excel"
                    )
                if not self.sheets:
                    raise FileDefinitionError(
                        f"No se especificaron hojas requeridas para {self.file_type}"
                    )

            # Validación de codificación para archivos CSV
            if self.format == 'csv' and not self.encoding:
                raise FileDefinitionError(
                    f"No se especificó codificación para archivo CSV: {self.file_type}"
                )

            return True

        except FileDefinitionError as e:
            raise FileDefinitionError(
                f"Error en definición de {self.file_type}: {str(e)}"
            )

    def __str__(self) -> str:
        """Representación en string de la definición."""
        return (
            f"FileDefinition(type={self.file_type}, "
            f"format={self.format}, required={self.required}, "
            f"requires_sheets={self.requires_sheets})"
        )